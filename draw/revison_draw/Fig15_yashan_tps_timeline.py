#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot YashanDB dynamic-workload TPS timeline from result.txt files."""

import argparse
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import rcParams


TPS_PATTERN = re.compile(r"\[Exec TPS\]\s+([\d.]+)\s+txn/sec")


def parse_tps(path):
    values = []
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            match = TPS_PATTERN.search(line)
            if match:
                values.append(float(match.group(1)))
    return values


def resample(values, source_interval_s, target_interval_s):
    if target_interval_s <= source_interval_s:
        return values
    bucket = max(1, int(round(target_interval_s / source_interval_s)))
    out = []
    for i in range(0, len(values), bucket):
        chunk = values[i : i + bucket]
        if chunk:
            out.append(sum(chunk) / len(chunk))
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", help="result.txt files")
    parser.add_argument("--labels", nargs="+", required=True)
    parser.add_argument("--output", default="yashan_tps_timeline.pdf")
    parser.add_argument("--duration-minutes", type=float, default=25.0)
    parser.add_argument("--source-interval-seconds", type=float, default=2.0)
    parser.add_argument("--target-interval-seconds", type=float, default=5.0)
    args = parser.parse_args()

    if len(args.files) != len(args.labels):
        raise ValueError("The number of files must match the number of labels.")

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

    colors = {
        "RR": "#85c0e9",
        "MWR": "#ff7e0e",
        "PHR": "#2ca02c",
        "PAR": "#B157D7",
        "CPR": "#d6d027",
        "MP-Router": "#e47474",
    }
    linestyles = {
        "RR": ":",
        "MWR": "-.",
        "PHR": "--",
        "PAR": "--",
        "CPR": "-.",
        "MP-Router": "--",
    }

    fig, ax = plt.subplots(figsize=(8, 4))
    for label, file_path in zip(args.labels, args.files):
        values = parse_tps(file_path)
        values = resample(values, args.source_interval_seconds, args.target_interval_seconds)
        if not values:
            print(f"No TPS points found: {file_path}")
            continue
        x = [i * args.target_interval_seconds / 60.0 for i in range(len(values))]
        y = [v / 1000.0 for v in values]
        ax.plot(
            x,
            y,
            label=label,
            color=colors.get(label),
            linestyle=linestyles.get(label, "-"),
            linewidth=1.2,
            alpha=0.95,
        )
        print(f"{label}: {len(values)} sampled points")

    phase_lines = [5, 10, 15, 20]
    phase_labels = [
        ("Warmup", 2.5),
        ("Partitioned", 7.5),
        ("Low Affinity", 12.5),
        ("Change Affinity", 17.5),
        ("High Skew", 22.5),
    ]
    for x in phase_lines:
        ax.axvline(x, color="#555555", linestyle="--", linewidth=1.1, alpha=0.9)

    ax.set_xlim(0, args.duration_minutes)
    ax.set_ylim(0, 70)
    top = ax.get_ylim()[1]
    for text, x in phase_labels:
        if 0 <= x <= args.duration_minutes:
            ax.text(x, top * 0.95, text, ha="center", va="top", fontsize=12, fontweight="bold")

    ax.set_xlabel("Time (Minutes)", fontsize=14)
    ax.set_ylabel("Throughput (KTPS)", fontsize=14)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.47, 1.17),
        ncol=len(args.labels),
        frameon=False,
        prop={"weight": "bold", "size": 12},
    )

    plt.tight_layout()
    out_path = Path("figs") / args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.05)
    print(f"Saved figure: {out_path}")


if __name__ == "__main__":
    main()
