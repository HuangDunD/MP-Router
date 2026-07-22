#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 14 ablation study and time breakdown."""

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
rcParams["font.size"] = 10
rcParams["axes.labelsize"] = 11
rcParams["xtick.labelsize"] = 10
rcParams["ytick.labelsize"] = 10
rcParams["legend.fontsize"] = 9


ABLATION_COLORS = ["#85c0e9", "#e4e474", "#74e4bfce", "#B157D790", "#e47474"]
ABLATION_HATCHES = ["////", "----", "xxxx", "oo", "...."]

BREAKDOWN_STYLE = {
    "Wait DB": ("#d3d3d3", ""),
    "Wait Preprocess": ("#b07cc6", "...."),
    "Wait Queue": ("#85c0e9", ""),
    "Conflict-Free": ("#ffb347", ""),
    "Conflicting": ("#e47474", "////"),
    "Execute": ("#77dd77", "xxxx"),
}


def plot_ablation(ax, data, systems):
    labels = [label for label, _ in data]
    values_by_system = [
        [values[i] / 1000.0 for _, values in data]
        for i in range(len(systems))
    ]
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
            color=ABLATION_COLORS[i],
            hatch=ABLATION_HATCHES[i],
            edgecolor="#404040AD",
            linewidth=0.8,
            zorder=3,
        )

    max_y = max(value for values in values_by_system for value in values)
    ax.set_ylim(0, math.ceil(max_y * 1.12 / 20) * 20)
    ax.yaxis.set_major_locator(MultipleLocator(40))
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel(r"Skewness ($\theta$)", fontsize=12)
    ax.set_ylabel("Throughput (KTPS)", fontsize=12)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)
    ax.text(0.45, -0.3, "(a) Ablation study", transform=ax.transAxes, ha="center", va="top", fontsize=16, fontweight="bold")


def plot_time_breakdown(ax, data):
    labels = [f"{kind}\n" + rf"($\theta={theta}$)" for theta, kind, _ in data]
    y_positions = [0, 1, 2.4, 3.4]
    seen = {}

    for y, (_, _, parts) in zip(y_positions, data):
        left = 0.0
        for name, value in parts:
            color, hatch = BREAKDOWN_STYLE[name]
            label = name if name not in seen else "_" + name
            bar = ax.barh(
                y,
                value,
                left=left,
                height=0.55,
                label=label,
                color=color,
                hatch=hatch,
                edgecolor="#404040AD",
                linewidth=0.8,
                zorder=3,
            )
            seen.setdefault(name, bar[0])
            left += value

    ax.set_yticks(y_positions)
    ax.set_yticklabels(labels, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.xaxis.set_major_locator(MultipleLocator(25))
    ax.set_xlabel("Time Breakdown (%)", fontsize=12)
    ax.grid(axis="x", linestyle="--", alpha=0.5, zorder=0)
    ax.text(0.47, -0.3, "(b) Time breakdown", transform=ax.transAxes, ha="center", va="top", fontsize=16, fontweight="bold")

    legend_order = ["Wait DB", "Conflict-Free", "Wait Preprocess", "Conflicting", "Wait Queue", "Execute"]
    handles = [seen[name] for name in legend_order if name in seen]
    legend_labels = [name for name in legend_order if name in seen]
    return handles, legend_labels


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-module", default="extracted_data.fig14_ablation_breakdown")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig14_ablation_time_breakdown.pdf")
    args = parser.parse_args()

    revision_data = importlib.import_module(args.data_module)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, 3.45), gridspec_kw={"width_ratios": [0.78, 1.72]})
    plot_ablation(ax1, revision_data.ablation_data, revision_data.ABLATION_SYSTEMS)
    bd_handles, bd_labels = plot_time_breakdown(ax2, revision_data.time_breakdown_data)

    ab_handles, ab_labels = ax1.get_legend_handles_labels()
    ax1.legend(
        ab_handles,
        ab_labels,
        loc="lower center",
        bbox_to_anchor=(0.43, 1.03),
        prop={"weight": "bold", "size": 11},
        handlelength=1.4,
        handleheight=1.0,
        frameon=False,
        ncol=2,
        columnspacing=0.8,
    )
    ax2.legend(
        bd_handles,
        bd_labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 1.03),
        prop={"weight": "bold", "size": 11},
        handlelength=1.4,
        handleheight=1.0,
        frameon=False,
        ncol=3,
        columnspacing=0.35,
    )

    fig.subplots_adjust(left=0.07, right=0.99, bottom=0.25, top=0.80, wspace=0.30)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
