#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract plot-ready data from MP-Router summary.csv files.

This script intentionally does not change any plotting style. It only converts
summary rows into the same Python literal structures currently hard-coded in the
revision plotting scripts.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from pprint import pformat


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SMALLBANK_SUMMARY = ROOT / "实验结果备份/20260616040130/summary.csv"
DEFAULT_TPCC_SUMMARY = ROOT / "实验结果备份/20260615094601/summary.csv"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "revision_extracted_data.py"


MAIN_SYSTEMS = [
    ("RR", "0"),
    ("MWR", "25"),
    ("PHR", "2"),
    ("PAR", "23"),
    ("CPR", "28"),
    ("MP-Router", "11"),
]

KP_SYSTEMS = [
    ("PHR", "2"),
    ("PAR", "23"),
    ("CPR", "28"),
    ("MP-Router", "11"),
]

MLP_SYSTEMS = [
    ("MP-Router with MLP", "11", "1"),
    ("MP-Router", "11", "0"),
]

ABLATION_SYSTEMS = [
    ("MP-Router", "11"),
    ("w/o barrier", "26"),
    ("w/o critical-path", "30"),
    ("w/o scheduling", "13"),
]

TPCC_SYSTEMS = MAIN_SYSTEMS + [("Warehouse-aware", "31")]


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def norm_float(value: str) -> str:
    if value == "":
        return ""
    try:
        return f"{float(value):g}"
    except ValueError:
        return value


def throughput(row: dict[str, str]) -> float:
    return round(float(row["throughput_after_warmup_tps"]), 2)


class Extractor:
    def __init__(self, rows: list[dict[str, str]]):
        self.rows = rows
        self.missing: list[str] = []

    def find_one(self, desc: str, **conds: str) -> dict[str, str] | None:
        matches = []
        for row in self.rows:
            ok = True
            for key, expected in conds.items():
                actual = norm_float(row.get(key, ""))
                if actual != str(expected):
                    ok = False
                    break
            if ok:
                matches.append(row)
        if not matches:
            self.missing.append(f"{desc}: {conds}")
            return None
        if len(matches) > 1:
            self.missing.append(f"{desc}: multiple matches for {conds}; using first")
        return matches[0]

    def series(
        self,
        desc: str,
        labels: list[tuple[str, dict[str, str]]],
        systems: list[tuple[str, str]],
        base_conds: dict[str, str] | None = None,
    ) -> list[tuple[str, list[float | None]]]:
        out = []
        for label, label_conds in labels:
            values = []
            for _, mode in systems:
                conds = dict(base_conds or {})
                conds.update(label_conds)
                conds["run_mode"] = mode
                row = self.find_one(f"{desc}/{label}/mode{mode}", **conds)
                values.append(throughput(row) if row else None)
            out.append((label, values))
        return out


def smallbank_figures(rows: list[dict[str, str]]) -> tuple[dict[str, object], list[str]]:
    e = Extractor(rows)

    zipf_labels = [
        (theta, {"scan_axis": "access", "access_pattern": "1", "zipfian_theta": theta})
        for theta in ["0.1", "0.6"]
    ]
    zipf_labels.append(("0.8", {"scan_axis": "base", "access_pattern": "1", "zipfian_theta": "0.8"}))
    zipf_labels.extend(
        [
            (theta, {"scan_axis": "access", "access_pattern": "1", "zipfian_theta": theta})
            for theta in ["0.9", "1.1", "1.3"]
        ]
    )
    hotspot_labels = [
        (label, {"scan_axis": "access", "access_pattern": "2", "hotspot_fraction": frac})
        for label, frac in [("100%", "1"), ("10%", "0.1"), ("1%", "0.01"), ("0.1%", "0.001")]
    ]

    affinity_labels = [
        (f"{int(float(ratio) * 100)}%", {"scan_axis": "affinity_txn_ratio", "affinity_txn_ratio": ratio})
        for ratio in ["0", "0.2", "0.4", "0.6", "1"]
    ]
    affinity_labels.insert(
        4,
        ("80%", {"scan_axis": "base", "affinity_txn_ratio": "0.8"}),
    )

    thread_labels = [
        (threads, {"scan_axis": "worker_threads", "worker_threads": threads})
        for threads in ["2", "4", "8", "32", "64", "128"]
    ]
    thread_labels.insert(3, ("16", {"scan_axis": "base", "worker_threads": "16"}))

    kp_labels = [
        (f"{int(float(ratio) * 100)}%", {"scan_axis": "key_page_capacity", "key_page_ratio": ratio})
        for ratio in ["0.2", "0.4", "0.6", "0.8", "1"]
    ]

    batch_labels = [
        (size, {"scan_axis": "batch_size", "batch_size": size})
        for size in ["10", "100", "500", "1000", "5000", "50000", "100000"]
    ]
    batch_labels.insert(5, ("10000", {"scan_axis": "base", "batch_size": "10000"}))

    ablation_labels = [
        ("0.8", {"scan_axis": "base", "access_pattern": "1", "zipfian_theta": "0.8"}),
        ("0.9", {"scan_axis": "access", "access_pattern": "1", "zipfian_theta": "0.9"}),
        ("1.1", {"scan_axis": "access", "access_pattern": "1", "zipfian_theta": "1.1"}),
    ]

    data: dict[str, object] = {
        "MAIN_SYSTEMS": [name for name, _ in MAIN_SYSTEMS],
        "KP_SYSTEMS": [name for name, _ in KP_SYSTEMS],
        "MLP_SYSTEMS": [name for name, _, _ in MLP_SYSTEMS],
        "ABLATION_SYSTEMS": [name for name, _ in ABLATION_SYSTEMS],
        "throughput_zipfian_data": e.series("throughput_zipfian", zipf_labels, MAIN_SYSTEMS),
        "throughput_hotspot_data": e.series("throughput_hotspot", hotspot_labels, MAIN_SYSTEMS),
        "affinity_data": e.series("affinity", affinity_labels, MAIN_SYSTEMS),
        "thread_num_data": e.series("thread_num", thread_labels, MAIN_SYSTEMS),
        "kp_data": e.series("kp", kp_labels, KP_SYSTEMS),
        "batch_size_data": e.series("batch_size", batch_labels, [("MP-Router", "11")]),
        "ablation_data": e.series("ablation", ablation_labels, ABLATION_SYSTEMS),
    }

    # MLP has one series with mlp_enabled=1 from scan_axis=mlp_zipfian and one
    # baseline series from the normal access sweep.
    mlp_data = []
    for theta in ["0.1", "0.6", "0.8", "0.9", "1.1", "1.3"]:
        values = []
        row = e.find_one(
            f"mlp/{theta}/mlp",
            scan_axis="mlp_zipfian",
            access_pattern="1",
            zipfian_theta=theta,
            run_mode="11",
            mlp_enabled="1",
        )
        values.append(throughput(row) if row else None)
        baseline_scan_axis = "base" if theta == "0.8" else "access"
        row = e.find_one(
            f"mlp/{theta}/baseline",
            scan_axis=baseline_scan_axis,
            access_pattern="1",
            zipfian_theta=theta,
            run_mode="11",
            mlp_enabled="0",
        )
        values.append(throughput(row) if row else None)
        mlp_data.append((theta, values))
    data["mlp_data"] = mlp_data

    return data, e.missing


def tpcc_figures(rows: list[dict[str, str]]) -> tuple[dict[str, object], list[str]]:
    e = Extractor(rows)
    labels = [
        ("Unpartitioned", {"scan_axis": "base"}),
        ("Warehouse-partitioned", {"scan_axis": "tpcc_partition_warehouses"}),
    ]
    data = {
        "TPCC_SYSTEMS": [name for name, _ in TPCC_SYSTEMS],
        "tpcc_data": e.series("tpcc", labels, TPCC_SYSTEMS),
    }
    return data, e.missing


def write_module(output: Path, payload: dict[str, object], missing: list[str]) -> None:
    lines = [
        "# Auto-generated by extract_revision_data.py.",
        "# Do not edit by hand; adjust CSV paths or extraction config instead.",
        "",
    ]
    for key, value in payload.items():
        lines.append(f"{key} = {pformat(value, width=120)}")
        lines.append("")
    lines.append(f"MISSING = {pformat(missing, width=120)}")
    lines.append("")
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smallbank-summary", type=Path, default=DEFAULT_SMALLBANK_SUMMARY)
    parser.add_argument("--tpcc-summary", type=Path, default=DEFAULT_TPCC_SUMMARY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    smallbank_data, smallbank_missing = smallbank_figures(read_rows(args.smallbank_summary))
    tpcc_data, tpcc_missing = tpcc_figures(read_rows(args.tpcc_summary))
    payload = {**smallbank_data, **tpcc_data}
    missing = smallbank_missing + tpcc_missing
    write_module(args.output, payload, missing)

    print(f"Wrote {args.output}")
    print(f"Missing entries: {len(missing)}")
    for item in missing:
        print("  -", item)


if __name__ == "__main__":
    main()
