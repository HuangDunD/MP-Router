#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 6 throughput under different affinity transaction ratios."""

from __future__ import annotations

import argparse
import importlib
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import MultipleLocator

SYSTEMS = ["RR", "MWR", "PHR", "PAR", "CPR", "MP-Router"]

rcParams["hatch.linewidth"] = 0.3
rcParams["text.usetex"] = False
rcParams["font.family"] = "sans-serif"
rcParams["font.sans-serif"] = ["Arial"]
rcParams["mathtext.fontset"] = "custom"
rcParams["mathtext.rm"] = "Arial"
rcParams["mathtext.it"] = "Arial:italic"
rcParams["mathtext.bf"] = "Arial:bold"
rcParams["axes.unicode_minus"] = False
rcParams["font.size"] = 12
rcParams["axes.labelsize"] = 12
rcParams["xtick.labelsize"] = 12
rcParams["ytick.labelsize"] = 12
rcParams["legend.fontsize"] = 12


def plot_grouped_bars(ax, data, systems, colors, hatches):
    labels = [label for label, _ in data]
    values_by_system = []
    for i in range(len(systems)):
        values_by_system.append([values[i] / 1000.0 for _, values in data])

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

    finite_values = [value for values in values_by_system for value in values]
    upper_val = math.ceil(max(finite_values) * 1.12 / 20) * 20
    ax.set_ylim(20, 130)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=12)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel("Affinity Transaction Ratio", fontsize=16)
    ax.set_ylabel("Throughput (KTPS)", fontsize=16)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-module", default="extracted_data.fig6_affinity")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig6_affinity_ratio_bar.pdf")
    args = parser.parse_args()

    revision_data = importlib.import_module(args.data_module)
    systems = revision_data.MAIN_SYSTEMS
    if systems != SYSTEMS:
        raise ValueError(f"Unexpected systems: {systems}")

    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
    hatches = ["////", "\\\\\\\\", "xxxx", "oo", "OOOO", "...."]

    fig, ax = plt.subplots(1, 1, figsize=(6.0, 4))
    plot_grouped_bars(ax, revision_data.affinity_data, SYSTEMS, colors, hatches)

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.99),
        prop={"weight": "bold", "size": 12},
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=6,
        columnspacing=1.0,
    )

    fig.subplots_adjust(left=0.12, right=0.98, bottom=0.16, top=0.84)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
