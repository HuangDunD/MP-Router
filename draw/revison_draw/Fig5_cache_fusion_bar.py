#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 5 ownership transfers for SmallBank and TPC-C."""

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


def plot_grouped_bars(
    ax,
    data,
    systems,
    colors,
    hatches,
    *,
    xlabel,
    ylabel,
    major_locator=None,
    ylim_top=None,
):
    labels = [label for label, _ in data]
    values_by_system = []
    for i in range(len(systems)):
        values = []
        for _, group_values in data:
            raw = group_values[i]
            values.append(raw if not math.isnan(raw) else math.nan)
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

    finite_values = [value for values in values_by_system for value in values if math.isfinite(value)]
    upper_val = max(finite_values) * 1.12 if finite_values else 1
    if major_locator is None:
        major_locator = max(1, math.ceil(upper_val / 5))
    upper_val = math.ceil(upper_val / major_locator) * major_locator
    if ylim_top is not None:
        upper_val = ylim_top

    ax.set_ylim(0, upper_val)
    ax.yaxis.set_major_locator(MultipleLocator(major_locator))
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=0, ha="center", fontsize=12)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-module",
        default="extracted_data.fig5_cache_fusion",
        help="Python module containing Fig. 5 data.",
    )
    parser.add_argument(
        "--outfile",
        type=Path,
        default=Path("figs") / "fig5_cache_fusion_bar.pdf",
    )
    args = parser.parse_args()
    revision_data = importlib.import_module(args.data_module)

    systems = revision_data.MAIN_SYSTEMS
    if systems != SYSTEMS:
        raise ValueError(f"Unexpected systems: {systems}")

    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
    hatches = ["////", "\\\\\\\\", "xxxx", "oo", "OOOO", "...."]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, 2.5), sharey=False)
    ylabel = "Ownership Transfers / Txn"

    plot_grouped_bars(
        ax1,
        revision_data.smallbank_cache_fusion_data,
        systems,
        colors,
        hatches,
        xlabel=r"Skewness ($\theta$)",
        ylabel=ylabel,
        major_locator=2,
    )
    ax1.text(
        0.46,
        -0.3,
        "(a) SmallBank",
        transform=ax1.transAxes,
        ha="center",
        va="top",
        fontsize=15,
        fontweight="bold",
    )

    plot_grouped_bars(
        ax2,
        revision_data.tpcc_cache_fusion_data,
        systems,
        colors,
        hatches,
        xlabel="Hot-Spot Concentration",
        ylabel=ylabel,
        major_locator=8,
        ylim_top=40,
    )
    ax2.text(
        0.5,
        -0.3,
        "(b) TPC-C",
        transform=ax2.transAxes,
        ha="center",
        va="top",
        fontsize=15,
        fontweight="bold",
    )
    ax2.set_ylim(0, 35)

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

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
