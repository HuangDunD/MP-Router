#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract SmallBank latency percentiles and generate a paper-style LaTeX table.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SUMMARY = ROOT / "实验结果备份" / "20260616040130(fianl)" / "summary.csv"
DEFAULT_OUTDIR = Path("figs")

SYSTEMS = [
    ("RR", "0"),
    ("MWR", "25"),
    ("PHR", "2"),
    ("PAR", "23"),
    ("CPR", "28"),
    (r"\Hname", "11"),
]
THETAS = ["0.6", "0.9"]


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


def float_eq(value: str, expected: float) -> bool:
    try:
        return abs(float(value) - expected) < 1e-9
    except ValueError:
        return False


def is_baseline_row(row: dict[str, str]) -> bool:
    if norm_float(row.get("access_pattern", "")) != "1":
        return False
    if not float_eq(row.get("account_count", ""), 10000000):
        return False
    if norm_float(row.get("mlp_enabled", "0")) == "1":
        return False
    return True


def find_row(rows: list[dict[str, str]], theta: str, mode: str) -> dict[str, str] | None:
    matches = [
        row
        for row in rows
        if is_baseline_row(row)
        and norm_float(row.get("zipfian_theta", "")) == theta
        and norm_float(row.get("run_mode", "")) == mode
    ]
    if not matches:
        return None
    # Prefer base/access sweeps and then the largest case_id. This avoids MLP
    # or auxiliary runs while tolerating merged CSVs with repeated case IDs.
    preferred = [
        row
        for row in matches
        if row.get("scan_axis", "") in {"base", "access"}
    ]
    candidates = preferred or matches
    return max(candidates, key=lambda row: int(row.get("case_id") or 0))


def fmt(value: float | None, bold: bool = False) -> str:
    if value is None:
        return r"\needfill{}"
    text = f"{value:.2f}"
    return rf"\textbf{{{text}}}" if bold else text


def collect_latency(rows: list[dict[str, str]]):
    data = {}
    missing = []
    for system, mode in SYSTEMS:
        data[system] = {}
        for theta in THETAS:
            row = find_row(rows, theta, mode)
            if row is None:
                missing.append(f"{system}/theta={theta}/mode={mode}")
                data[system][theta] = None
                continue
            data[system][theta] = {
                "db_p50": float(row["exec_latency_p50_ms"]),
                "db_p99": float(row["exec_latency_p99_ms"]),
                "e2e_p50": float(row["e2e_latency_p50_ms"]) / 100.0,
                "e2e_p99": float(row["e2e_latency_p99_ms"]) / 100.0,
                "case_id": row.get("case_id", ""),
                "scan_axis": row.get("scan_axis", ""),
            }
    return data, missing


def make_latex(data) -> str:
    lines = [
        r"\begin{table}[t]",
        r"  \centering",
        r"  \caption{Latency percentiles on the larger SmallBank dataset (DB-side \& end-to-end).}",
        r"  \vspace{-5pt}",
        r"  \label{tab:txn-latency-theta-revision}",
        r"  \small",
        r"  \scalebox{0.90}{",
        r"  \begin{tabular}{ccccc cccc}",
        r"    \toprule",
        r"    \multirow{3}{*}{\textbf{Method}} &",
        r"    \multicolumn{4}{c}{\textbf{DB-side latency} $(ms)$} &",
        r"    \multicolumn{4}{c}{\textbf{End-to-end latency} $(\times 10^{2} ms)$} \\",
        r"    \cmidrule(lr){2-5} \cmidrule(lr){6-9}",
        r"    & \multicolumn{2}{c}{$\boldsymbol{\theta=0.6}$} &",
        r"      \multicolumn{2}{c}{$\boldsymbol{\theta=0.9}$} &",
        r"      \multicolumn{2}{c}{$\boldsymbol{\theta=0.6}$} &",
        r"      \multicolumn{2}{c}{$\boldsymbol{\theta=0.9}$} \\",
        r"    \cmidrule(lr){2-3} \cmidrule(lr){4-5}",
        r"    \cmidrule(lr){6-7} \cmidrule(lr){8-9}",
        r"    & \textbf{P50} & \textbf{P99} & \textbf{P50} & \textbf{P99}",
        r"    & \textbf{P50} & \textbf{P99} & \textbf{P50} & \textbf{P99} \\",
        r"    \midrule",
    ]

    for system, _ in SYSTEMS:
        bold = system == r"\Hname"
        row06 = data[system]["0.6"]
        row09 = data[system]["0.9"]
        values = []
        for row, key in [
            (row06, "db_p50"),
            (row06, "db_p99"),
            (row09, "db_p50"),
            (row09, "db_p99"),
            (row06, "e2e_p50"),
            (row06, "e2e_p99"),
            (row09, "e2e_p50"),
            (row09, "e2e_p99"),
        ]:
            values.append(fmt(None if row is None else row[key], bold))
        method = rf"\textbf{{{system}}}" if bold else rf"\textbf{{{system}}}"
        lines.append(f"    {method} & " + " & ".join(values) + r" \\")

    lines.extend(
        [
            r"    \bottomrule",
            r"  \end{tabular}",
            r"  }",
            r"  \vspace{-4mm}",
            r"\end{table}",
            "",
        ]
    )
    return "\n".join(lines)


def make_markdown(data) -> str:
    headers = [
        "Method",
        "DB P50 θ=0.6",
        "DB P99 θ=0.6",
        "DB P50 θ=0.9",
        "DB P99 θ=0.9",
        "E2E P50 θ=0.6 (×10^2ms)",
        "E2E P99 θ=0.6 (×10^2ms)",
        "E2E P50 θ=0.9 (×10^2ms)",
        "E2E P99 θ=0.9 (×10^2ms)",
    ]
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for system, _ in SYSTEMS:
        row06 = data[system]["0.6"]
        row09 = data[system]["0.9"]
        values = []
        for row, key in [
            (row06, "db_p50"),
            (row06, "db_p99"),
            (row09, "db_p50"),
            (row09, "db_p99"),
            (row06, "e2e_p50"),
            (row06, "e2e_p99"),
            (row09, "e2e_p50"),
            (row09, "e2e_p99"),
        ]:
            values.append("MISSING" if row is None else f"{row[key]:.2f}")
        out.append("| " + " | ".join([system.replace(r"\Hname", "MP-Router"), *values]) + " |")
    return "\n".join(out) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--outdir", type=Path, default=DEFAULT_OUTDIR)
    args = parser.parse_args()

    data, missing = collect_latency(read_rows(args.summary))
    args.outdir.mkdir(parents=True, exist_ok=True)
    tex_path = args.outdir / "smallbank_latency_table.tex"
    md_path = args.outdir / "smallbank_latency_table.md"
    tex_path.write_text(make_latex(data), encoding="utf-8")
    md_path.write_text(make_markdown(data), encoding="utf-8")

    print(f"Wrote {tex_path}")
    print(f"Wrote {md_path}")
    if missing:
        print("Missing rows:")
        for item in missing:
            print(f"  {item}")
    print()
    print(make_markdown(data))


if __name__ == "__main__":
    main()
