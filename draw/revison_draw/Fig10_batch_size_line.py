#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot Fig. 10 throughput while varying transaction window size."""

from __future__ import annotations

import argparse
import importlib
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.ticker import FixedLocator, FuncFormatter, MultipleLocator


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
    parser.add_argument("--data-module", default="extracted_data.fig10_batch_size")
    parser.add_argument("--outfile", type=Path, default=Path("figs") / "fig10_batch_size_line.pdf")
    args = parser.parse_args()

    revision_data = importlib.import_module(args.data_module)
    batch_sizes = [int(label) for label, _ in revision_data.batch_size_data]
    throughput = [value / 1000.0 for _, value in revision_data.batch_size_data]

    fig, ax = plt.subplots(1, 1, figsize=(4.5, 3))
    ax.plot(
        batch_sizes,
        throughput,
        color="#215aecc1",
        marker="s",
        markersize=10,
        linewidth=3.5,
        markeredgecolor="white",
        markeredgewidth=0.5,
        zorder=3,
    )

    upper = math.ceil(max(throughput) * 1.12 / 20) * 20
    ax.set_ylim(0, upper)
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.set_xscale("log")
    ax.set_xlim(min(batch_sizes) * 0.75, max(batch_sizes) * 1.25)
    major_ticks = [10, 100, 1000, 10000, 100000]
    ax.xaxis.set_major_locator(FixedLocator(major_ticks))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: rf"$10^{{{int(math.log10(value))}}}$"))
    ax.set_xlabel("Txn Window Size", fontsize=16)
    ax.set_ylabel("Throughput (KTPS)", fontsize=16)
    ax.grid(axis="both", linestyle="--", alpha=0.5, zorder=0)

    fig.subplots_adjust(left=0.18, right=0.98, bottom=0.20, top=0.82)
    # fig.subplots_adjust(left=0.18, right=0.98, bottom=0.22, top=0.94)
    args.outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outfile, dpi=600)
    plt.close(fig)

    print(f"Saved figure: {args.outfile}")


if __name__ == "__main__":
    main()
