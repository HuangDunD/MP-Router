#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot SmallBank and TPC-C throughput side by side.
"""

import argparse
import math
import os
import re
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import MultipleLocator

try:
    import revision_extracted_data as revision_data
except ImportError:
    revision_data = None


DEFAULT_TPCC_RESULT_DIR = (
    Path(__file__).resolve().parents[3]
    / "实验结果备份"
    / "20260617142027(tpcc)"
)

SYSTEMS = ["RR", "MWR", "PHR", "PAR", "CPR", "MP-Router"]
TPCC_SYSTEM_MODES = [
    ("RR", "0"),
    ("MWR", "25"),
    ("PHR", "2"),
    ("PAR", "23"),
    ("CPR", "28"),
    ("MP-Router", "11"),
]

TPCC_GROUPS = [
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


def load_smallbank_data():
    data = [
        ("0.1", [83629.17, 84793.7, 106955.9, 96134.37, 97803.73, 126348.13]),
        ("0.6", [82187.26, 82718.88, 100301.26, 92793.34, 93070.26, 122045.9]),
        ("0.8", [72046.07, 72224.56, 84830.97, 68772.67, 78025.03, 107074.93]),
        ("0.9", [14892.43, 12971.53, 15253.43, 14371.59, 17182.89, 77165.69]),
        ("1.1", [2611.65, 2475.33, 2642.91, 2682.67, 2654.77, 34410.69]),
        ("1.3", [1194.93, 1149.9, 1210.47, 1120.47, 1248.2, 19189.73]),
    ]
    if revision_data:
        return revision_data.MAIN_SYSTEMS, revision_data.throughput_zipfian_data
    return SYSTEMS, data


def load_tpcc_data(result_dir: Path):
    data = []
    missing = []
    for label, folder in TPCC_GROUPS:
        values = []
        for system, mode in TPCC_SYSTEM_MODES:
            result_file = result_dir / folder / f"m{mode}" / "result.txt"
            tps = read_after_warmup_tps(result_file)
            if math.isnan(tps):
                missing.append(f"{label}/{system}: {result_file}")
            values.append(tps)
        data.append((label, values))
    return data, missing


def plot_grouped_bars(ax, data, systems, colors, hatches, *, scale, xlabel, ylabel, ylabel_fontsize=14):
    labels = [label for label, _ in data]
    values_by_system = []
    for i in range(len(systems)):
        values = []
        for _, group_values in data:
            raw = group_values[i]
            values.append(raw * scale if not math.isnan(raw) else math.nan)
        values_by_system.append(values)

    total_width = 0.8
    bar_width = total_width / len(systems)
    x_base = list(range(len(labels)))

    for i, values in enumerate(values_by_system):
        offset = (i - (len(systems) - 1) / 2) * bar_width
        ax.bar(
            [x + offset for x in x_base],
            values,
            width=bar_width,
            label=systems[i],
            color=colors[i],
            hatch=hatches[i],
            edgecolor="#404040AD",
            linewidth=0.8,
            zorder=3,
        )

    finite_values = [value for values in values_by_system for value in values if not math.isnan(value)]
    upper_val = max(finite_values) * 1.12 if finite_values else 1
    if upper_val > 1000:
        upper_val = math.ceil(upper_val / 500) * 500
        ax.yaxis.set_major_locator(MultipleLocator(1000))
    elif upper_val > 300:
        upper_val = math.ceil(upper_val / 100) * 100
        ax.yaxis.set_major_locator(MultipleLocator(100))
    else:
        upper_val = math.ceil(upper_val / 10) * 10
        ax.yaxis.set_major_locator(MultipleLocator(20 if upper_val > 80 else 10))

    ax.set_ylim(0, upper_val)
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=12)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel(xlabel, fontsize=14)
    ax.set_ylabel(ylabel, fontsize=ylabel_fontsize)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tpcc-result-dir", type=Path, default=DEFAULT_TPCC_RESULT_DIR)
    parser.add_argument(
        "--outfile",
        type=Path,
        default=Path("figs") / "workload_throughput_combined.pdf",
    )
    args = parser.parse_args()

    smallbank_systems, smallbank_data = load_smallbank_data()
    tpcc_data, missing = load_tpcc_data(args.tpcc_result_dir)
    if smallbank_systems != SYSTEMS:
        raise ValueError(f"Unexpected SmallBank systems: {smallbank_systems}")

    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
    hatches = ["////", "\\\\\\\\", "xxxx", "oo", "OOOO", "...."]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, 2.5), sharey=False)

    plot_grouped_bars(
        ax1,
        smallbank_data,
        SYSTEMS,
        colors,
        hatches,
        scale=1.0 / 1000.0,
        xlabel=r"Skewness ($\theta$)",
        ylabel="Throughput (KTPS)",
    )
    ax1.text(
        0.5,
        -0.35,
        "(a) SmallBank",
        transform=ax1.transAxes,
        ha="center",
        va="top",
        fontsize=15,
        weight="bold",
    )

    plot_grouped_bars(
        ax2,
        tpcc_data,
        SYSTEMS,
        colors,
        hatches,
        scale=60.0 / 10000.0,
        xlabel="Hot-Spot Concentration",
        ylabel=r"Throughput ($\times 10^4$ TPM)",
        ylabel_fontsize=12,
    )
    ax2.text(
        0.5,
        -0.35,
        "(b) TPC-C",
        transform=ax2.transAxes,
        ha="center",
        va="top",
        fontsize=15,
        weight="bold",
    )

    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.86),
        prop={"weight": "bold", "size": 12},
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=6,
        columnspacing=1.5,
    )

    plt.subplots_adjust(bottom=0.15, top=0.85, wspace=0.25)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)

    if missing:
        print("Missing TPC-C values:")
        for item in missing:
            print(f"  {item}")
    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
