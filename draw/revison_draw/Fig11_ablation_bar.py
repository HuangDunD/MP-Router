#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot standalone ablation study as a half-width figure."""

from __future__ import annotations

import argparse
import importlib
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import MultipleLocator


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
rcParams["axes.labelsize"] = 16
rcParams["xtick.labelsize"] = 12
rcParams["ytick.labelsize"] = 12
rcParams["legend.fontsize"] = 12


COLORS = ["#85c0e9", "#e4e474", "#74e4bfce", "#CE2D9890", "#e47474"]
HATCHES = ["////", "----", "xxxx", "oo", "...."]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-module", default="extracted_data.fig14_ablation_breakdown")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig11_ablation_bar.pdf")
    args = parser.parse_args()

    revision_data = importlib.import_module(args.data_module)
    systems = revision_data.ABLATION_SYSTEMS
    labels = [label for label, _ in revision_data.ablation_data]
    values_by_system = [
        [values[i] / 1000.0 for _, values in revision_data.ablation_data]
        for i in range(len(systems))
    ]

    fig, ax = plt.subplots(1, 1, figsize=(4.5, 3))
    total_width = 0.72
    bar_width = total_width / len(systems)
    x_base = list(range(len(labels)))

    for i, values in enumerate(values_by_system):
        offset = (i - (len(systems) - 1) / 2) * bar_width
        ax.bar(
            [x + offset for x in x_base],
            values,
            width=bar_width,
            label=systems[i],
            color=COLORS[i],
            hatch=HATCHES[i],
            edgecolor="#404040AD",
            linewidth=0.8,
            zorder=3,
        )

    upper = math.ceil(max(value for values in values_by_system for value in values) * 1.12 / 20) * 20
    ax.set_ylim(0, upper)
    ax.yaxis.set_major_locator(MultipleLocator(40))
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel(r"Skewness ($\theta$)", fontsize=16)
    ax.set_ylabel("Throughput (KTPS)", fontsize=16)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    handles, legend_labels = ax.get_legend_handles_labels()
    legend_order = [0, 3, 1, 4, 2]
    fig.legend(
        [handles[i] for i in legend_order],
        [legend_labels[i] for i in legend_order],
        loc="upper center",
        bbox_to_anchor=(0.52, 1.03),
        prop={"weight": "bold", "size": 11},
        handlelength=1.2,
        handleheight=1,
        frameon=False,
        ncol=3,
        columnspacing=0.45,
        labelspacing=0.25,
    )
    # top_order = [0, 1, 2]
    # bottom_order = [3, 4]
    # fig.legend(
    #     [handles[i] for i in top_order],
    #     [legend_labels[i] for i in top_order],
    #     loc="upper center",
    #     bbox_to_anchor=(0.5, 1.00),
    #     prop={"weight": "bold", "size": 11},
    #     handlelength=1.2,
    #     handleheight=0.85,
    #     frameon=False,
    #     ncol=3,
    #     columnspacing=0.45,
    #     labelspacing=0.25,
    # )
    # fig.legend(
    #     [handles[i] for i in bottom_order],
    #     [legend_labels[i] for i in bottom_order],
    #     loc="upper center",
    #     bbox_to_anchor=(0.5, 0.93),
    #     prop={"weight": "bold", "size": 11},
    #     handlelength=1.2,
    #     handleheight=0.85,
    #     frameon=False,
    #     ncol=2,
    #     columnspacing=0.9,
    #     labelspacing=0.25,
    # )

    fig.subplots_adjust(left=0.18, right=0.98, bottom=0.20, top=0.82)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
