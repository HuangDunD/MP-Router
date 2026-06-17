#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot TPC-C throughput for unpartitioned data only.

The script reads result.txt files directly, so duplicate/merged case IDs in
summary files do not affect the plotted values.
"""

import argparse
import math
import os
import re
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams


DEFAULT_RESULT_DIR = (
    Path(__file__).resolve().parents[3]
    / "实验结果备份"
    / "20260617142027(tpcc)"
)

SYSTEMS = [
    ("RR", "0"),
    ("MWR", "25"),
    ("PHR", "2"),
    ("PAR", "23"),
    ("CPR", "28"),
    ("MP-Router", "11"),
]

GROUPS = [
    ("Normal", "tpcc_p0_wh200_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0"),
    ("50\\%", "tpcc_p2_HsFrac0.25_HsProb0.5_wh200_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0"),
    ("80\\%", "tpcc_p2_HsFrac0.25_HsProb0.8_wh200_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0"),
    ("90\\%", "tpcc_p2_HsFrac0.25_HsProb0.9_wh200_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0"),
]


if shutil.which("latex"):
    rcParams.update({
        "text.usetex": True,
        "font.family": "serif",
        "font.serif": ["Times", "Computer Modern Roman"],
        "axes.unicode_minus": False,
        "text.latex.preamble": r"\usepackage{amsmath}",
    })
else:
    rcParams["text.usetex"] = False

rcParams["hatch.linewidth"] = 0.3
rcParams["font.family"] = "serif"
rcParams["font.serif"] = ["Arial"]
rcParams["font.size"] = 12
rcParams["axes.labelsize"] = 14
rcParams["xtick.labelsize"] = 12
rcParams["ytick.labelsize"] = 12
rcParams["legend.fontsize"] = 12


def read_after_warmup_tps(result_file: Path):
    if not result_file.exists():
        return math.nan
    text = result_file.read_text(errors="ignore")
    match = re.search(r"Throughput \(after warmup\):\s*([0-9.]+)", text)
    if not match:
        return math.nan
    return float(match.group(1))


def load_data(result_dir: Path):
    data = []
    missing = []
    for label, folder in GROUPS:
        values = []
        for system, mode in SYSTEMS:
            result_file = result_dir / folder / f"m{mode}" / "result.txt"
            tps = read_after_warmup_tps(result_file)
            if math.isnan(tps):
                missing.append(f"{label}/{system}: {result_file}")
            values.append(tps)
        data.append((label, values))
    return data, missing


def plot(data, outfile: Path):
    outfile.parent.mkdir(parents=True, exist_ok=True)

    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
    hatches = ["////", "\\\\\\\\", "xxxx", "oo", "OOOO", "...."]

    labels = [label for label, _ in data]
    system_values = []
    for i in range(len(SYSTEMS)):
        vals = []
        for _, group_vals in data:
            tps = group_vals[i]
            vals.append(tps * 60.0 / 1000.0 if not math.isnan(tps) else math.nan)
        system_values.append(vals)

    fig, ax = plt.subplots(1, 1, figsize=(6.5, 5.5))

    total_width = 0.8
    n_sys = len(SYSTEMS)
    bar_width = total_width / n_sys
    x_base = np.arange(len(data))

    for i, (system, _) in enumerate(SYSTEMS):
        offset = (i - (n_sys - 1) / 2) * bar_width
        ax.bar(
            x_base + offset,
            system_values[i],
            width=bar_width,
            label=system,
            color=colors[i],
            hatch=hatches[i],
            edgecolor="#404040AD",
            linewidth=0.8,
            zorder=3,
        )

    finite_values = [v for values in system_values for v in values if not math.isnan(v)]
    ymax = max(finite_values) * 1.18 if finite_values else 1.0

    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=14)
    ax.set_xlabel("Hot-Spot Concentration", fontsize=14)
    ax.set_xlim(-0.55, len(data) - 1 + 0.55)
    ax.set_ylim(0, ymax)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)
    ax.set_ylabel(r"Throughput ($\times 10^3$ TPM)", fontsize=16)

    handles, legend_labels = ax.get_legend_handles_labels()
    ax.legend(
        handles,
        legend_labels,
        loc="upper center",
        bbox_to_anchor=(0.45, 1.15),
        prop={"weight": "bold", "size": 12},
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=6,
        columnspacing=1.0,
    )

    plt.tight_layout(rect=[0, 0, 1, 0.80])
    plt.savefig(outfile, dpi=600, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", type=Path, default=DEFAULT_RESULT_DIR)
    parser.add_argument(
        "--outfile",
        type=Path,
        default=Path("figs") / "tpcc_unpartitioned_throughput_bar.pdf",
    )
    args = parser.parse_args()

    data, missing = load_data(args.result_dir)
    print("Extracted TPC-C unpartitioned throughput (TPS):")
    for label, values in data:
        formatted = ", ".join(
            f"{system}={value:.2f}" if not math.isnan(value) else f"{system}=MISSING"
            for (system, _), value in zip(SYSTEMS, values)
        )
        print(f"  {label}: {formatted}")
    if missing:
        print("Missing values:")
        for item in missing:
            print(f"  {item}")

    plot(data, args.outfile)
    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
