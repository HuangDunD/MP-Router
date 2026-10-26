#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Appendix Fig. C scalability experiments."""

from __future__ import annotations

import argparse
import importlib
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import MultipleLocator


SYSTEMS = ["RR", "MWR", "PHR", "PAR", "CPR", "MP-Router"]

COLORS = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
MARKERS = ["o", "s", "^", "p", "8", "D"]
LINESTYLES = ["--", "--", "--", "--", "--", "-"]
LINEWIDTHS = [2.6, 2.6, 2.6, 2.6, 2.6, 3.0]

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
rcParams["axes.labelsize"] = 14
rcParams["xtick.labelsize"] = 12
rcParams["ytick.labelsize"] = 12
rcParams["legend.fontsize"] = 12


def build_series(data: list[tuple[str, list[float]]], systems: list[str]) -> tuple[list[str], list[list[float]]]:
    labels = [label for label, _ in data]
    series = [
        [math.nan if values[i] is None else values[i] / 1000.0 for _, values in data]
        for i in range(len(systems))
    ]
    return labels, series


def max_value(series_groups: list[list[list[float]]]) -> float:
    values = [value for series in series_groups for line in series for value in line if not math.isnan(value)]
    return max(values) if values else 1.0


def plot_panel(ax, labels: list[str], series: list[list[float]], xlabel: str, caption: str) -> None:
    x = list(range(len(labels)))
    for i, values in enumerate(series):
        if len(values) != len(labels):
            continue
        ax.plot(
            x,
            values,
            label=SYSTEMS[i],
            color=COLORS[i],
            marker=MARKERS[i],
            markersize=6.5,
            linewidth=LINEWIDTHS[i],
            linestyle=LINESTYLES[i],
            markeredgecolor="white",
            markeredgewidth=0.5,
            zorder=3,
        )

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    if labels:
        ax.set_xlim(-0.25, len(labels) - 0.75)
    ax.set_xlabel(xlabel, fontsize=14)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)
    ax.grid(axis="x", linestyle="--", alpha=0.25, zorder=0)
    ax.text(0.48, -0.33, caption, transform=ax.transAxes, ha="center", va="top", fontsize=15, fontweight="bold")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--thread-data-module", default="extracted_data.fig8_thread_scalability")
    parser.add_argument("--node-data-module", default="extracted_data.fig9_node_scalability")
    parser.add_argument("--account-data-module", default="extracted_data.figC_account_scalability")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "figC_scalability_combined.pdf")
    args = parser.parse_args()

    thread_data = importlib.import_module(args.thread_data_module)
    node_data = importlib.import_module(args.node_data_module)
    account_data = importlib.import_module(args.account_data_module)
    if thread_data.MAIN_SYSTEMS != SYSTEMS:
        raise ValueError(f"Unexpected thread systems: {thread_data.MAIN_SYSTEMS}")
    if node_data.MAIN_SYSTEMS != SYSTEMS:
        raise ValueError(f"Unexpected node systems: {node_data.MAIN_SYSTEMS}")
    if account_data.MAIN_SYSTEMS != SYSTEMS:
        raise ValueError(f"Unexpected account systems: {account_data.MAIN_SYSTEMS}")

    thread_labels, thread_series = build_series(thread_data.thread_scalability_data, SYSTEMS)
    node_labels, node_series = build_series(node_data.node_scalability_data, SYSTEMS)
    account_labels, account_series = build_series(account_data.account_scalability_data, SYSTEMS)
    if not account_labels:
        account_labels = list(account_data.ACCOUNT_PLACEHOLDER_LABELS)
        account_series = [[] for _ in SYSTEMS]
    account_labels = [label[:-1] if label.endswith("M") else label for label in account_labels]

    upper = math.ceil(max_value([thread_series, node_series, account_series]) * 1.12 / 20) * 20

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(10.5, 3.1))
    plot_panel(ax1, thread_labels, thread_series, "Number of Connections", "(a) Connection")
    plot_panel(ax2, node_labels, node_series, "Number of Primaries", "(b) Primary")
    plot_panel(ax3, account_labels, account_series, "Number of Accounts (M)", "(c) Data size")

    for ax in (ax1, ax2, ax3):
        ax.set_ylim(0, upper)
        ax.yaxis.set_major_locator(MultipleLocator(20))
        ax.yaxis.tick_left()
        ax.yaxis.set_ticks_position("left")
        ax.yaxis.set_label_position("left")
        ax.tick_params(axis="y", which="both", left=True, labelleft=True, right=False, labelright=False)
        ax.spines["left"].set_visible(True)
        ax.spines["right"].set_visible(True)
    ax1.set_ylabel("Throughput (KTPS)", fontsize=14)

    handles, legend_labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles,
        legend_labels,
        loc="upper center",
        bbox_to_anchor=(0.52, 1.02),
        prop={"weight": "bold", "size": 16},
        handlelength=2.5,
        handleheight=1.1,
        frameon=False,
        ncol=6,
        columnspacing=1.8,
    )

    fig.subplots_adjust(left=0.08, right=0.99, bottom=0.27, top=0.84, wspace=0.18)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
