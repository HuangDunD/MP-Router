#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot throughput of MLP-based routing versus MP-Router."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import MultipleLocator

from extracted_data import fig13_mlp as revision_data


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


def plot_grouped_bars(ax, data, systems):
    labels = [label for label, _ in data]
    values_by_system = []
    for i in range(len(systems)):
        values_by_system.append([values[i] / 1000.0 for _, values in data])

    colors = ["#B157D790", "#e47474"]
    hatches = ["oo", "...."]
    total_width = 0.55
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
    ax.set_ylim(0, upper_val)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=12)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel(r"Skewness ($\theta$)", fontsize=14)
    ax.set_ylabel("Throughput (KTPS)", fontsize=14)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig13_mlp.pdf")
    args = parser.parse_args()

    systems = revision_data.MLP_SYSTEMS
    data = revision_data.mlp_data
    fig, ax = plt.subplots(1, 1, figsize=(4.0, 2.5))
    plot_grouped_bars(ax, data, systems)

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.55, 1.03),
        prop={"weight": "bold", "size": 12},
        handlelength=1.4,
        handleheight=1.2,
        frameon=False,
        ncol=2,
        columnspacing=1.1,
    )

    fig.subplots_adjust(left=0.17, right=0.98, bottom=0.22, top=0.85)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
