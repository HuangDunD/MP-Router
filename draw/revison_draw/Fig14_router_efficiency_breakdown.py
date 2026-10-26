#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 14 router-only throughput/mix and time breakdown."""

from __future__ import annotations

import argparse
import importlib
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
rcParams["axes.labelsize"] = 12
rcParams["xtick.labelsize"] = 10
rcParams["ytick.labelsize"] = 10
rcParams["legend.fontsize"] = 10


MIX_STYLE = {
    "Conflict-Free": ("#85c0e9", "////"),
    "Critical": ("#e47474", "...."),
    "Non-Critical": ("#ffb347", "xxxx"),
}

BREAKDOWN_STYLE = {
    "Wait DB": ("#d3d3d3", ""),
    "Wait Preprocess": ("#b07cc6", ""),
    "Wait Barrier": ("#a9c5d8", ""),
    "Conflict-Free": ("#ffb347", "...."),
    "Conflicting": ("#e47474", "////"),
    "Execute": ("#77dd77", "xxxx"),
}

BREAKDOWN_LABEL = {
    "Conflict-Free": "Conflict-Free Route",
    "Conflicting": "Conflicting Route",
}

ROUTER_THREAD_ORDER = ["Wait Preprocess", "Conflict-Free", "Conflicting", "Wait DB"]


def system_throughput_by_theta(system_data):
    return {theta: throughput / 1_000.0 for theta, throughput in system_data}


def plot_route_only(ax, route_data, system_data):
    labels = [theta for theta, *_ in route_data]
    x = list(range(len(labels)))
    router_throughput_ktps = [throughput / 1_000.0 for _, throughput, *_ in route_data]
    system_tps = system_throughput_by_theta(system_data)
    system_throughput_ktps = [system_tps[theta] for theta in labels]
    mix_by_name = {name: [] for name in MIX_STYLE}
    for _, _, mix, _, _ in route_data:
        values = dict(mix)
        for name in mix_by_name:
            mix_by_name[name].append(values[name])

    ax_mix = ax.twinx()
    ax_mix.set_zorder(1)
    ax.set_zorder(2)
    ax.patch.set_alpha(0)

    bottoms = [0.0] * len(labels)
    bar_handles = []
    for name in ["Conflict-Free", "Critical", "Non-Critical"]:
        color, hatch = MIX_STYLE[name]
        bars = ax_mix.bar(
            x,
            mix_by_name[name],
            width=0.62,
            bottom=bottoms,
            label=name,
            color=color,
            hatch=hatch,
            alpha=0.48,
            edgecolor="#404040AD",
            linewidth=0.8,
            zorder=1,
        )
        bar_handles.append(bars[0])
        bottoms = [base + value for base, value in zip(bottoms, mix_by_name[name])]

    router_line, = ax.plot(
        x,
        router_throughput_ktps,
        color="#215aecc1",
        marker="s",
        markersize=8,
        linewidth=2.8,
        markeredgecolor="white",
        markeredgewidth=0.5,
        zorder=4,
    )
    system_line, = ax.plot(
        x,
        system_throughput_ktps,
        color="#6f7378",
        marker="o",
        markersize=8,
        linewidth=2.8,
        markeredgecolor="white",
        markeredgewidth=0.5,
        zorder=5,
    )

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlim(-0.5, len(labels) - 0.5)
    ax.set_ylim(0, 1300)
    ax.yaxis.set_major_locator(MultipleLocator(250))
    ax_mix.set_ylim(0, 100)
    ax_mix.yaxis.set_major_locator(MultipleLocator(20))
    ax.set_xlabel(r"Skewness ($\theta$)", fontsize=12)
    ax.set_ylabel("Throughput (KTPS)", fontsize=12)
    ax_mix.set_ylabel("Txn Ratio (%)", fontsize=12)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)
    ax.text(
        0.5,
        -0.28,
        "(a) Throughput and txn mix",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=14,
        fontweight="bold",
    )
    line_handles = [router_line, system_line]
    line_labels = ["Router-only Thput.", "Database Thput."]
    bar_labels = ["Conflict-Free", "Critical", "Non-Critical"]
    return line_handles, line_labels, bar_handles, bar_labels


def plot_time_breakdown(ax, data):
    labels = [f"{kind}\n" + rf"($\theta={theta}$)" for theta, kind, _ in data]
    y_positions = [0, 1, 2.4, 3.4]
    seen = {}

    for y, (_, kind, parts) in zip(y_positions, data):
        left = 0.0
        if kind == "Router Thread":
            order = {name: index for index, name in enumerate(ROUTER_THREAD_ORDER)}
            parts = sorted(parts, key=lambda item: order.get(item[0], len(order)))
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
    ax.text(
        0.47,
        -0.28,
        "(b) Time breakdown",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=14,
        fontweight="bold",
    )

    legend_order = ["Wait DB", "Conflict-Free", "Wait Preprocess", "Conflicting", "Wait Barrier", "Execute"]
    handles = [seen[name] for name in legend_order if name in seen]
    legend_labels = [BREAKDOWN_LABEL.get(name, name) for name in legend_order if name in seen]
    return handles, legend_labels


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--route-data-module", default="extracted_data.fig11_route_only")
    parser.add_argument("--breakdown-data-module", default="extracted_data.fig14_ablation_breakdown")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig14_router_efficiency_breakdown.pdf")
    args = parser.parse_args()

    route_data = importlib.import_module(args.route_data_module)
    breakdown_data = importlib.import_module(args.breakdown_data_module)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, 3.2), gridspec_kw={"width_ratios": [1.0, 1.45]})
    line_handles, line_labels, mix_handles, mix_labels = plot_route_only(
        ax1,
        route_data.route_only_data,
        route_data.system_throughput_data,
    )
    bd_handles, bd_labels = plot_time_breakdown(ax2, breakdown_data.time_breakdown_data)

    line_legend = ax1.legend(
        line_handles,
        line_labels,
        loc="lower center",
        bbox_to_anchor=(0.49, 1.13),
        prop={"weight": "bold", "size": 11},
        handlelength=1.8,
        handleheight=1,
        frameon=False,
        ncol=2,
        columnspacing=0.6,
    )
    ax1.add_artist(line_legend)
    ax1.legend(
        mix_handles,
        mix_labels,
        loc="lower center",
        bbox_to_anchor=(0.49, 1),
        prop={"weight": "bold", "size": 11},
        handlelength=1.2,
        handleheight=1,
        frameon=False,
        ncol=3,
        columnspacing=0.45,
    )
    ax2.legend(
        bd_handles,
        bd_labels,
        loc="lower center",
        bbox_to_anchor=(0.38, 1),
        prop={"weight": "bold", "size": 11},
        handlelength=1.2,
        handleheight=1,
        frameon=False,
        ncol=3,
        columnspacing=0.35,
    )

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.25, top=0.82, wspace=0.57)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
