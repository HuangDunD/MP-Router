#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot SmallBank Zipfian throughput as a standalone rebuttal figure.
"""

import math
import os
import shutil

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import MultipleLocator

try:
    import revision_extracted_data as revision_data
except ImportError:
    revision_data = None


if shutil.which("latex"):
    try:
        rcParams.update({
            "text.usetex": True,
            "font.family": "serif",
            "font.serif": ["Times", "Computer Modern Roman"],
            "axes.unicode_minus": False,
            "text.latex.preamble": r"\usepackage{amsmath}",
        })
    except Exception:
        pass
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


def main():
    outdir = "figs"
    outfile = "smallbank_zipfian_throughput.pdf"
    os.makedirs(outdir, exist_ok=True)

    systems = ["RR", "MWR", "PHR", "PAR", "CPR", "MP-Router"]
    data = [
        ("0.1", [83629.17, 84793.7, 106955.9, 96134.37, 97803.73, 126348.13]),
        ("0.6", [82187.26, 82718.88, 100301.26, 92793.34, 93070.26, 122045.9]),
        ("0.8", [72046.07, 72224.56, 84830.97, 68772.67, 78025.03, 107074.93]),
        ("0.9", [14892.43, 12971.53, 15253.43, 14371.59, 17182.89, 77165.69]),
        ("1.1", [2611.65, 2475.33, 2642.91, 2682.67, 2654.77, 34410.69]),
        ("1.3", [1194.93, 1149.9, 1210.47, 1120.47, 1248.2, 19189.73]),
    ]
    if revision_data:
        systems = revision_data.MAIN_SYSTEMS
        data = revision_data.throughput_zipfian_data

    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
    hatches = ["////", "\\\\\\\\", "xxxx", "oo", "OOOO", "...."]

    labels = [item[0] for item in data]
    values_by_system = []
    for i in range(len(systems)):
        values_by_system.append([item[1][i] / 1000.0 for item in data])

    fig, ax = plt.subplots(1, 1, figsize=(4.4, 2.8))
    total_width = 0.8
    bar_width = total_width / len(systems)
    x_base = range(len(data))

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

    all_values = [value for values in values_by_system for value in values]
    upper_val = math.ceil(max(all_values) * 1.12 / 10) * 10
    ax.set_ylim(0, upper_val)
    ax.yaxis.set_major_locator(MultipleLocator(20 if upper_val > 80 else 10))

    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=12)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel(r"Skewness ($\theta$)", fontsize=14)
    ax.set_ylabel("Throughput (KTPS)", fontsize=14)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    handles, legend_labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        legend_labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.02),
        prop={"weight": "bold", "size": 10},
        handlelength=1.4,
        handleheight=1.1,
        frameon=False,
        ncol=6,
        columnspacing=0.9,
    )

    plt.tight_layout(rect=[0, 0, 1, 0.90])
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.04)
    print(f"Saved figure: {out_path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
