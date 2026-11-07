#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 12 throughput and MP-Router map hit rate versus map capacity."""

from __future__ import annotations

import argparse
import importlib
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

    ax.set_ylim(60, 120)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.yaxis.set_minor_locator(MultipleLocator(10))
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=12)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel("Key-Page Map Capacity", fontsize=14)
    ax.set_ylabel("Throughput (KTPS)", fontsize=14)
    ax.grid(axis="y", which="both", linestyle="--", alpha=0.5, zorder=0)


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

    hit_rates = revision_data.KP_HIT_RATES["MP-Router"]
    if len(hit_rates) != len(revision_data.key_page_map_data):
        raise ValueError("Hit rates and capacity points must have the same length")
    hit_ax = ax.twinx()
    hit_line, = hit_ax.plot(
        range(len(hit_rates)), hit_rates, color="#414040", linestyle="--",
        marker="D", markersize=4.5, markerfacecolor="white",
        linewidth=1.4, label="MP-Router map hit rate", zorder=5,
    )
    hit_ax.set_ylim(0, 105)
    hit_ax.yaxis.set_major_locator(MultipleLocator(25))
    hit_ax.set_ylabel("Map hit rate (%)", fontsize=12, labelpad=3)
    hit_ax.tick_params(axis="y", labelsize=11, pad=2)
    hit_ax.grid(False)

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.50, 1.02),
        prop={"weight": "bold", "size": 10},
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=4,
        columnspacing=0.6,
    )
    fig.legend(
        [hit_line], [hit_line.get_label()], loc="upper center",
        bbox_to_anchor=(0.50, 0.925), fontsize=10, frameon=False,
        handlelength=2.4,
    )

    fig.subplots_adjust(left=0.17, right=0.83, bottom=0.22, top=0.79)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.savefig(args.outfile.with_suffix(".png"), dpi=240)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
