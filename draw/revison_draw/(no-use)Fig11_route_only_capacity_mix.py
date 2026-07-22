#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 11 router-only throughput and transaction mix."""

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
rcParams["font.size"] = 12
rcParams["axes.labelsize"] = 16
rcParams["xtick.labelsize"] = 12
rcParams["ytick.labelsize"] = 12
rcParams["legend.fontsize"] = 12


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-module", default="extracted_data.fig11_route_only")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig11_route_only_capacity_mix.pdf")
    args = parser.parse_args()

    revision_data = importlib.import_module(args.data_module)
    labels = [theta for theta, *_ in revision_data.route_only_data]
    x = list(range(len(labels)))
    throughput_ktps = [throughput / 1_000.0 for _, throughput, *_ in revision_data.route_only_data]
    mix_by_name = {
        "Conflict-Free": [],
        "Critical": [],
        "Non-Critical": [],
    }
    for _, _, mix, _, _ in revision_data.route_only_data:
        values = dict(mix)
        for name in mix_by_name:
            mix_by_name[name].append(values[name])

    fig, ax1 = plt.subplots(1, 1, figsize=(4.5, 3))
    ax2 = ax1.twinx()
    ax2.set_zorder(1)
    ax1.set_zorder(2)
    ax1.patch.set_alpha(0)

    bar_width = 0.62
    bottoms = [0.0] * len(labels)
    bar_styles = {
        "Conflict-Free": ("#85c0e9", "////"),
        "Critical": ("#e47474", "...."),
        "Non-Critical": ("#ffb347", "xxxx"),
    }
    bar_handles = []
    for name in ["Conflict-Free", "Critical", "Non-Critical"]:
        color, hatch = bar_styles[name]
        bars = ax2.bar(
            x,
            mix_by_name[name],
            width=bar_width,
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

    (line,) = ax1.plot(
        x,
        throughput_ktps,
        color="#215aecc1",
        marker="s",
        markersize=10,
        linewidth=3.5,
        markeredgecolor="white",
        markeredgewidth=0.5,
        label="Router throughput",
        zorder=4,
    )

    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.set_xlim(-0.5, len(labels) - 0.5)
    ax1.set_ylim(0, 1300)
    ax1.yaxis.set_major_locator(MultipleLocator(250))
    ax2.set_ylim(0, 100)
    ax2.yaxis.set_major_locator(MultipleLocator(20))

    ax1.set_xlabel(r"Skewness ($\theta$)", fontsize=16)
    ax1.set_ylabel("Throughput (KTPS)", fontsize=16)
    ax2.set_ylabel("Txn Ratio (%)", fontsize=16)
    ax1.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    handles = bar_handles
    legend_labels = ["Conflict-Free", "Critical", "Non-Critical"]
    fig.legend(
        handles,
        legend_labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.02),
        prop={"weight": "bold", "size": 13},
        handlelength=1.4,
        handleheight=1.0,
        frameon=False,
        ncol=3,
        columnspacing=0.8,
    )

    fig.subplots_adjust(left=0.18, right=0.84, bottom=0.20, top=0.85)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
