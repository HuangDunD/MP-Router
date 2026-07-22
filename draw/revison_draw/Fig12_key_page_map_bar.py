#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 12 throughput under different key-page-map capacities."""

from __future__ import annotations

import argparse
import importlib
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import MultipleLocator


SYSTEMS = ["PHR", "PAR", "CPR", "MP-Router"]

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
    values_by_system = [
        [values[i] / 1000.0 for _, values in data]
        for i in range(len(systems))
    ]

    total_width = 0.68
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

    max_y = max(value for values in values_by_system for value in values)
    min_y = min(value for values in values_by_system for value in values)
    lower = math.floor(min_y * 0.85 / 10) * 10
    upper = math.ceil(max_y * 1.10 / 10) * 10
    ax.set_ylim(60, 120)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=12)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel("Key-Page Map Capacity", fontsize=14)
    ax.set_ylabel("Throughput (KTPS)", fontsize=14)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-module", default="extracted_data.fig12_key_page_map")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig12_key_page_map_bar.pdf")
    args = parser.parse_args()

    revision_data = importlib.import_module(args.data_module)
    systems = revision_data.KP_SYSTEMS
    if systems != SYSTEMS:
        raise ValueError(f"Unexpected systems: {systems}")

    colors = ["#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
    hatches = ["xxxx", "oo", "OOOO", "...."]

    fig, ax = plt.subplots(1, 1, figsize=(4.0, 2.5))
    plot_grouped_bars(ax, revision_data.key_page_map_data, systems, colors, hatches)

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.52, 1.03),
        prop={"weight": "bold", "size": 12},
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=4,
        columnspacing=0.6,
    )

    fig.subplots_adjust(left=0.17, right=0.98, bottom=0.22, top=0.85)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
