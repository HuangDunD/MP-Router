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
    "compute_node_count",
    "affinity_txn_ratio",
    "batch_size",
    "num_bucket",
    "key_page_ratio",
    "mlp_enabled",
    "scan_axis",
    "use_data_cache",
    "data_cache_path",
    "throughput_after_warmup_tps",
    "exec_latency_p50_ms",
    "exec_latency_p95_ms",
    "exec_latency_p99_ms",
    "e2e_latency_p50_ms",
    "e2e_latency_p95_ms",
    "e2e_latency_p99_ms",
    "overall_throughput_tps",
    "committed_transactions",
    "committed_transactions_after_warmup",
    "batch_local_conflicting_ratio",
    "cf_wait_count",
    "cf_total_time_s",
    "cf_waits_per_txn",
    "cf_avg_ms",
    "cf_waits_per_app_txn",
    "cf_time_ms_per_app_txn",
    "kwr_business_sql_exec_count",
    "cf_waits_per_kwr_business_txn",
    "cf_time_ms_per_kwr_business_txn",
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


def parse_latency_section(text, title_pattern, prefix):
    match = re.search(title_pattern, text)
    if not match:
        return {}
    section = text[match.end():]
    next_section = re.search(r"\n\S.*Statistics \(After Warmup\):", section)
    if next_section:
        section = section[:next_section.start()]
    return {
        f"{prefix}_p50_ms": regex_value(section, r"(?m)^\s*P50:\s*([0-9.,]+)"),
        f"{prefix}_p95_ms": regex_value(section, r"(?m)^\s*P95:\s*([0-9.,]+)"),
        f"{prefix}_p99_ms": regex_value(section, r"(?m)^\s*P99:\s*([0-9.,]+)"),
    }


def parse_result(path):
    text = read_text(path)
    result = {
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
    result.update(parse_latency_section(
        text,
        r"(?m)^Latency Statistics \(After Warmup\):",
        "exec_latency",
    ))
    result.update(parse_latency_section(
        text,
        r"(?m)^(?:Batch-Start-to-Complete|Preprocess-and-Route-to-Complete|Route-to-Complete|Fetch-to-Complete) "
        r"Latency Statistics \(After Warmup\):",
        "e2e_latency",
    ))
    return result


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
        "cf_wait_count": clean_number(row[wait_count_index])
        if wait_count_index is not None and wait_count_index < len(row)
        else "",
        "cf_total_time_s": clean_number(row[total_time_index])
        if total_time_index is not None and total_time_index < len(row)
        else "",
        "cf_waits_per_txn": clean_number(row[per_txn_index])
        if per_txn_index is not None and per_txn_index < len(row)
        else "",
        "cf_avg_ms": clean_number(row[avg_ms_index])
        if avg_ms_index is not None and avg_ms_index < len(row)
        else "",
    }


def is_measured_workload_sql(sql):
    sql = normalize_text(sql).lower()
    if not sql:
        return False
    utility_prefixes = (
        "begin",
        "commit",
        "rollback",
        "start transaction",
        "set ",
        "show ",
        "explain ",
        "do $$",
    )
    utility_fragments = (
        "perf.create_snapshot",
        "perf.kwr_",
        "pg_stat_reset",
        "kwr_report",
        "create_snapshot",
    )
    if any(sql.startswith(prefix) for prefix in utility_prefixes):
        return False
    if any(fragment in sql for fragment in utility_fragments):
        return False
    return True


def parse_business_sql_counts(tables):
    exec_counts = {}
    for table in tables:
        if not table:
            continue
        header = table[0]
        sql_index = header_index(header, ["sql语句", "sqlstatement"])
        exec_index = header_index(header, ["执行次数", "executions", "executes", "calls"])
        query_id_index = header_index(header, ["queryid", "queryid"])
        if sql_index is None or exec_index is None:
            continue

        for row in table[1:]:
            if sql_index >= len(row) or not is_measured_workload_sql(row[sql_index]):
                continue
            key = row[query_id_index] if query_id_index is not None and query_id_index < len(row) else row[sql_index]
            if exec_index is not None and exec_index < len(row):
                value = to_float(row[exec_index])
                if value is not None:
                    exec_counts[key] = max(exec_counts.get(key, 0), int(value))

    return {
        "kwr_business_sql_exec_count": str(sum(exec_counts.values())) if exec_counts else "",
    }


def parse_kwr(path):
    if not path.exists():
        return {}
    parser = TableParser()
    parser.feed(read_text(path))
    result = parse_business_sql_counts(parser.tables)
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
            if parsed.get("cf_waits_per_txn"):
                result.update(parsed)
                return result
            if not fallback:
                fallback = parsed
    result.update(fallback)
    return result


def add_app_txn_cache_fusion_metrics(row):
    app_txn_count = to_float(row.get("committed_transactions_after_warmup"))
    if app_txn_count is None or app_txn_count <= 0:
        app_txn_count = to_float(row.get("committed_transactions"))
    if app_txn_count is None or app_txn_count <= 0:
        return

    wait_count = to_float(row.get("cf_wait_count"))
    total_time_s = to_float(row.get("cf_total_time_s"))
    if wait_count is not None:
        row["cf_waits_per_app_txn"] = f"{wait_count / app_txn_count:.6f}"
    if total_time_s is not None:
        row["cf_time_ms_per_app_txn"] = f"{total_time_s * 1000.0 / app_txn_count:.6f}"


def add_kwr_business_cache_fusion_metrics(row):
    business_txn_count = to_float(row.get("kwr_business_sql_exec_count"))
    if business_txn_count is None or business_txn_count <= 0:
        return

    wait_count = to_float(row.get("cf_wait_count"))
    total_time_s = to_float(row.get("cf_total_time_s"))
    if wait_count is not None:
        row["cf_waits_per_kwr_business_txn"] = (
            f"{wait_count / business_txn_count:.6f}"
        )
    if total_time_s is not None:
        row["cf_time_ms_per_kwr_business_txn"] = (
            f"{total_time_s * 1000.0 / business_txn_count:.6f}"
        )


def load_metadata(case_dir):
    path = case_dir / "metadata.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def case_id_sort_value(row):
    try:
        return int(row.get("case_id", ""))
    except (TypeError, ValueError):
        return 10**12


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            case_id_sort_value(row),
            str(row.get("result_file", "")),
        ),
    )


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
        add_kwr_business_cache_fusion_metrics(row)
        row["result_file"] = str(result_path)
        row["kwr_file"] = str(kwr_path) if kwr_files else ""
        rows.append(row)
    return sort_rows(rows)


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
        "exec_latency_p50_ms",
        "exec_latency_p95_ms",
        "exec_latency_p99_ms",
        "e2e_latency_p50_ms",
        "e2e_latency_p95_ms",
        "e2e_latency_p99_ms",
        "cf_waits_per_txn",
        "cf_waits_per_app_txn",
        "cf_waits_per_kwr_business_txn",
        "cf_avg_ms",
        "cf_time_ms_per_app_txn",
        "cf_time_ms_per_kwr_business_txn",
        "kwr_business_sql_exec_count",
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
