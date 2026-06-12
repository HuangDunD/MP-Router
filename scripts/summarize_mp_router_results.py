#!/usr/bin/env python3
import csv
import json
import re
import sys
from html.parser import HTMLParser
from pathlib import Path


SUMMARY_COLUMNS = [
    "case_id",
    "run_mode",
    "workload",
    "access_pattern",
    "zipfian_theta",
    "zipfian_generator",
    "hotspot_fraction",
    "hotspot_prob",
    "account_count",
    "warehouse_count",
    "worker_threads",
    "affinity_txn_ratio",
    "batch_size",
    "num_bucket",
    "key_page_ratio",
    "mlp_enabled",
    "scan_axis",
    "use_data_cache",
    "data_cache_path",
    "throughput_after_warmup_tps",
    "overall_throughput_tps",
    "committed_transactions",
    "committed_transactions_after_warmup",
    "batch_local_conflicting_ratio",
    "cache_fusion_receive_wait_count",
    "cache_fusion_receive_total_time_s",
    "cache_fusion_receive_waits_per_txn",
    "cache_fusion_receive_avg_ms",
    "cache_fusion_receive_waits_per_app_txn",
    "cache_fusion_receive_time_ms_per_app_txn",
    "result_file",
    "kwr_file",
]


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self.current_table = None
        self.current_row = None
        self.current_cell = None

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "table":
            self.current_table = []
        elif tag == "tr" and self.current_table is not None:
            self.current_row = []
        elif tag in ("td", "th") and self.current_row is not None:
            self.current_cell = []

    def handle_data(self, data):
        if self.current_cell is not None:
            self.current_cell.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in ("td", "th") and self.current_cell is not None:
            text = normalize_text("".join(self.current_cell))
            self.current_row.append(text)
            self.current_cell = None
        elif tag == "tr" and self.current_row is not None:
            if any(self.current_row):
                self.current_table.append(self.current_row)
            self.current_row = None
        elif tag == "table" and self.current_table is not None:
            if self.current_table:
                self.tables.append(self.current_table)
            self.current_table = None


def normalize_text(value):
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def read_text(path):
    return path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""


def regex_value(text, pattern):
    match = re.search(pattern, text)
    return match.group(1) if match else ""


def clean_number(value):
    return value.replace(",", "").replace("%", "").strip()


def to_float(value):
    try:
        return float(clean_number(str(value)))
    except (TypeError, ValueError):
        return None


def parse_result(path):
    text = read_text(path)
    return {
        "throughput_after_warmup_tps": regex_value(
            text, r"Throughput \(after warmup\):\s*([0-9.,]+)"
        ),
        "overall_throughput_tps": regex_value(text, r"(?m)^Throughput:\s*([0-9.,]+)"),
        "committed_transactions": regex_value(text, r"Committed transactions:\s*([0-9,]+)"),
        "committed_transactions_after_warmup": regex_value(
            text, r"Committed transactions \(after warmup\):\s*([0-9,]+)"
        ),
        "batch_local_conflicting_ratio": regex_value(
            text, r"Batch-Local Conflicting Ratio:\s*([0-9.]+)%"
        ),
    }


def header_index(header, candidates):
    for index, cell in enumerate(header):
        compact = cell.lower().replace(" ", "")
        if any(candidate in compact for candidate in candidates):
            return index
    return None


def parse_cache_fusion_row(row, header):
    wait_count_index = header_index(header, ["等待次数", "waits", "waitcount"])
    total_time_index = header_index(header, ["总时间(s)", "总时间", "totaltime(s)", "totaltime"])
    per_txn_index = header_index(header, ["每事务等待数", "waitspertxn", "waitpertxn"])
    avg_ms_index = header_index(
        header, ["平均时间(ms)", "平均时间", "avgwait(ms)", "avgwait", "averagetime(ms)"]
    )

    if wait_count_index is None and len(row) >= 2:
        wait_count_index = 1
    if total_time_index is None and len(row) >= 3:
        total_time_index = 2
    if avg_ms_index is None and len(row) >= 4:
        avg_ms_index = 3

    return {
        "cache_fusion_receive_wait_count": clean_number(row[wait_count_index])
        if wait_count_index is not None and wait_count_index < len(row)
        else "",
        "cache_fusion_receive_total_time_s": clean_number(row[total_time_index])
        if total_time_index is not None and total_time_index < len(row)
        else "",
        "cache_fusion_receive_waits_per_txn": clean_number(row[per_txn_index])
        if per_txn_index is not None and per_txn_index < len(row)
        else "",
        "cache_fusion_receive_avg_ms": clean_number(row[avg_ms_index])
        if avg_ms_index is not None and avg_ms_index < len(row)
        else "",
    }


def parse_kwr(path):
    if not path.exists():
        return {}
    parser = TableParser()
    parser.feed(read_text(path))
    fallback = {}
    for table in parser.tables:
        for row_index, row in enumerate(table):
            if not any(cell.lower() == "cache fusion receive" for cell in row):
                continue
            header = []
            for candidate in reversed(table[:row_index]):
                joined = "".join(candidate).lower().replace(" ", "")
                if ("每事务等待数" in joined or "waitspertxn" in joined) and (
                    "平均时间" in joined or "avgwait" in joined or "averagetime" in joined
                ):
                    header = candidate
                    break
            parsed = parse_cache_fusion_row(row, header)
            if parsed.get("cache_fusion_receive_waits_per_txn"):
                return parsed
            if not fallback:
                fallback = parsed
    return fallback


def add_app_txn_cache_fusion_metrics(row):
    app_txn_count = to_float(row.get("committed_transactions_after_warmup"))
    if app_txn_count is None or app_txn_count <= 0:
        app_txn_count = to_float(row.get("committed_transactions"))
    if app_txn_count is None or app_txn_count <= 0:
        return

    wait_count = to_float(row.get("cache_fusion_receive_wait_count"))
    total_time_s = to_float(row.get("cache_fusion_receive_total_time_s"))
    if wait_count is not None:
        row["cache_fusion_receive_waits_per_app_txn"] = f"{wait_count / app_txn_count:.6f}"
    if total_time_s is not None:
        row["cache_fusion_receive_time_ms_per_app_txn"] = f"{total_time_s * 1000.0 / app_txn_count:.6f}"


def load_metadata(case_dir):
    path = case_dir / "metadata.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def collect_rows(result_dir):
    rows = []
    for result_path in sorted(result_dir.rglob("result.txt")):
        case_dir = result_path.parent
        kwr_files = sorted(case_dir.glob("*_end.html"))
        kwr_path = kwr_files[-1] if kwr_files else Path()
        row = {column: "" for column in SUMMARY_COLUMNS}
        row.update(load_metadata(case_dir))
        row.update(parse_result(result_path))
        if kwr_files:
            row.update(parse_kwr(kwr_path))
        add_app_txn_cache_fusion_metrics(row)
        row["result_file"] = str(result_path)
        row["kwr_file"] = str(kwr_path) if kwr_files else ""
        rows.append(row)
    return rows


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in SUMMARY_COLUMNS})


def write_markdown(path, rows):
    columns = [
        "case_id",
        "run_mode",
        "throughput_after_warmup_tps",
        "cache_fusion_receive_waits_per_txn",
        "cache_fusion_receive_waits_per_app_txn",
        "cache_fusion_receive_avg_ms",
        "cache_fusion_receive_time_ms_per_app_txn",
    ]
    with path.open("w", encoding="utf-8") as f:
        f.write(f"# MP-Router Result Summary\n\nCompleted cases: {len(rows)}\n\n")
        f.write("| " + " | ".join(columns) + " |\n")
        f.write("| " + " | ".join(["---"] * len(columns)) + " |\n")
        for row in rows:
            f.write("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |\n")


def main():
    if len(sys.argv) != 2:
        print(f"usage: {Path(sys.argv[0]).name} RESULT_DIR", file=sys.stderr)
        return 2
    result_dir = Path(sys.argv[1]).resolve()
    if not result_dir.is_dir():
        print(f"result directory does not exist: {result_dir}", file=sys.stderr)
        return 2
    rows = collect_rows(result_dir)
    write_csv(result_dir / "summary.csv", rows)
    write_markdown(result_dir / "summary.md", rows)
    print(f"Wrote {len(rows)} rows")
    print(f"CSV: {result_dir / 'summary.csv'}")
    print(f"Markdown: {result_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
