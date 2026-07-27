#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extract Fig. 14 ablation and time-breakdown data from result.txt files."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from pprint import pformat


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MAIN_DIR = PROJECT_ROOT / "vldb_res" / "20260709000616(main)"
DEFAULT_ABLATION_DIR = PROJECT_ROOT / "vldb_res" / "20260718190512(abaltion-new)"
DEFAULT_BREAKDOWN_DIR = PROJECT_ROOT / "vldb_res" / "20260718190512(abaltion-new)"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "fig14_ablation_breakdown.py"

THETAS = ["0.1", "0.6", "0.8", "0.9", "1.1"]
BREAKDOWN_THETAS = ["0.6", "0.9"]
ABLATION_SYSTEMS = [
    ("RR", "main", "0"),
    ("w/o scheduling", "ablation", "13"),
    ("w/o critical-path", "ablation", "27"),
    ("w/o barrier", "ablation", "26"),
    ("MP-Router", "ablation", "11"),
]

SMALLBANK_MAIN_TEMPLATE = (
    "smallbank_p1_ZipfTheta{theta}_ZipfGenfinite_acc10000000"
    "_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0"
)

SMALLBANK_ABLATION_TEMPLATES = (
    SMALLBANK_MAIN_TEMPLATE,
    "smallbank_p1_ZipfTheta{theta}_ZipfGenfinite_acc10000000"
    "_t16_r0.8_b10000_n4_nb1_whpart0_kp1.1_mlp0",
)

SMALLBANK_BREAKDOWN_TEMPLATE = (
    "smallbank_p1_ZipfTheta{theta}_ZipfGenfinite_acc10000000"
    "_t16_r0.8_b10000_n4_nb1_whpart0_kp1.1_mlp0"
)


def read_text(path: Path) -> str:
    return path.read_text(errors="ignore")


def read_after_warmup_tps(result_file: Path) -> float:
    text = read_text(result_file)
    match = re.search(r"Throughput \(after warmup\):\s*([0-9.]+)", text)
    if not match:
        raise RuntimeError(f"Cannot find after-warmup throughput in {result_file}")
    return round(float(match.group(1)), 2)


def ablation_result_path(main_dir: Path, ablation_dir: Path, theta: str, source: str, mode: str) -> Path:
    if source == "main":
        path = main_dir / SMALLBANK_MAIN_TEMPLATE.format(theta=theta) / f"m{mode}" / "result.txt"
        if not path.exists():
            raise FileNotFoundError(path)
        return path
    else:
        matches = []
        for template in SMALLBANK_ABLATION_TEMPLATES:
            path = ablation_dir / template.format(theta=theta) / f"m{mode}" / "result.txt"
            if path.exists():
                matches.append(path)
        if len(matches) != 1:
            raise RuntimeError(
                f"theta={theta}, mode={mode}: expected exactly one ablation result, "
                f"found {len(matches)} under {ablation_dir}. matches={matches}"
            )
        return matches[0]


def extract_ablation(main_dir: Path, ablation_dir: Path) -> list[tuple[str, list[float]]]:
    data = []
    for theta in THETAS:
        values = []
        for _, source, mode in ABLATION_SYSTEMS:
            path = ablation_result_path(main_dir, ablation_dir, theta, source, mode)
            values.append(read_after_warmup_tps(path))
        data.append((theta, values))
    return data


def last_block(text: str, start_marker: str, end_marker: str | None = None) -> str:
    starts = [m.start() for m in re.finditer(re.escape(start_marker), text)]
    if not starts:
        raise RuntimeError(f"Cannot find block marker {start_marker!r}")
    start = starts[-1]
    if end_marker is None:
        return text[start:]
    end = text.find(end_marker, start + len(start_marker))
    return text[start:] if end < 0 else text[start:end]


def number(block: str, label: str) -> float:
    match = re.search(re.escape(label) + r":\s*([0-9.]+)\s*ms", block)
    if not match:
        raise RuntimeError(f"Cannot find {label!r}")
    return float(match.group(1))


def last_number(block: str, label: str) -> float:
    matches = re.findall(re.escape(label) + r":\s*([0-9.]+)\s*ms", block)
    if not matches:
        raise RuntimeError(f"Cannot find {label!r}")
    return float(matches[-1])


def optional_number(block: str, label: str) -> float:
    match = re.search(re.escape(label) + r":\s*([0-9.]+)\s*ms", block)
    return float(match.group(1)) if match else 0.0


def percent_parts(parts: list[tuple[str, float]]) -> list[tuple[str, float]]:
    total = sum(value for _, value in parts)
    if total <= 0:
        raise RuntimeError("Non-positive total in time breakdown")
    return [(label, round(value / total * 100.0, 2)) for label, value in parts]


def router_breakdown(text: str) -> list[tuple[str, float]]:
    block = last_block(text, "[Router Thread Time Breakdown]", "[Worker Thread Time Breakdown]")
    wait_db = number(block, "Wait Last Batch Finish Time")
    wait_preprocess = last_number(text, "Batch Boundary Gap Time")
    conflict_free = number(block, "Ownership Retrieval And Devide Unconflicted Txn Time")
    backpressure_sleep = optional_number(block, "Queue Backpressure Sleep Time")
    conflicting = max(0.0, number(block, "Process Conflicted Txn Time") - backpressure_sleep)
    return percent_parts(
        [
            ("Wait DB", wait_db + backpressure_sleep),
            ("Wait Preprocess", wait_preprocess),
            ("Conflict-Free", conflict_free),
            ("Conflicting", conflicting),
        ]
    )


def worker_breakdown(text: str) -> list[tuple[str, float]]:
    block = last_block(text, "[Worker Thread Time Breakdown]", "------------------------------------------")
    node_blocks = re.split(r"\nNode\s+\d+:\n", block)[1:]
    if not node_blocks:
        raise RuntimeError("Cannot find worker node blocks")

    wait = execute = 0.0
    for node in node_blocks:
        pop = number(node, "Average Pop Txn From Queue Time")
        wait_next = number(node, "Average Wait Next Batch Time")
        exec_time = number(node, "Average Worker Thread Exec Time")
        mark = number(node, "Average Mark Done Time")
        log = number(node, "Average Log Debug Info Time")
        wait += pop + wait_next + mark + log
        execute += exec_time

    return percent_parts(
        [
            ("Wait Barrier", wait),
            ("Execute", execute),
        ]
    )


def breakdown_result_path(root: Path, theta: str) -> Path:
    path = root / SMALLBANK_BREAKDOWN_TEMPLATE.format(theta=theta) / "m11" / "result.txt"
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def extract_time_breakdown(root: Path) -> list[tuple[str, str, list[tuple[str, float]]]]:
    data = []
    for theta in BREAKDOWN_THETAS:
        text = read_text(breakdown_result_path(root, theta))
        data.append((theta, "Router Thread", router_breakdown(text)))
        data.append((theta, "DB Connector", worker_breakdown(text)))
    return data


def write_module(
    output: Path,
    main_dir: Path,
    ablation_dir: Path,
    breakdown_dir: Path,
    ablation_data: list[tuple[str, list[float]]],
    breakdown_data: list[tuple[str, str, list[tuple[str, float]]]],
) -> None:
    lines = [
        "# Auto-generated by extract_fig14_ablation_breakdown.py.",
        "# Do not edit by hand; rerun the extractor with the intended data directory.",
        "#",
        f"# Main source: {main_dir}",
        f"# Ablation source: {ablation_dir}",
        f"# Time-breakdown source: {breakdown_dir}",
        "",
        f"ABLATION_SYSTEMS = {pformat([name for name, _, _ in ABLATION_SYSTEMS], width=120)}",
        "",
        f"ablation_data = {pformat(ablation_data, width=120)}",
        "",
        f"time_breakdown_data = {pformat(breakdown_data, width=120)}",
        "",
    ]
    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-dir", type=Path, default=DEFAULT_MAIN_DIR)
    parser.add_argument("--ablation-dir", type=Path, default=DEFAULT_ABLATION_DIR)
    parser.add_argument("--breakdown-dir", type=Path, default=DEFAULT_BREAKDOWN_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    ablation_data = extract_ablation(args.main_dir, args.ablation_dir)
    breakdown_data = extract_time_breakdown(args.breakdown_dir)
    write_module(args.output, args.main_dir, args.ablation_dir, args.breakdown_dir, ablation_data, breakdown_data)

    print(f"Wrote {args.output}")
    print(f"Main source: {args.main_dir}")
    print(f"Ablation source: {args.ablation_dir}")
    print(f"Time-breakdown source: {args.breakdown_dir}")


if __name__ == "__main__":
    main()
