#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extract per-phase YashanDB throughput statistics for Fig. 15.

The timeline contains five consecutive five-minute phases. Statistics are
computed from the raw two-second ``[Exec TPS]`` samples rather than the
resampled points used only to smooth the plotted timeline.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from pprint import pformat


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_RESULT_DIR = (
    PROJECT_ROOT
    / "vldb_res"
    / "20260715020857(yashan)"
    / "smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0"
)
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "fig15_yashan_phases.py"

SYSTEMS = [
    ("RR", "0"),
    ("MWR", "25"),
    ("PHR", "2"),
    ("PAR", "23"),
    ("CPR", "28"),
    ("MP-Router", "11"),
]

PHASES = [
    ("Warmup", 0.0, 5.0),
    ("Partitioned", 5.0, 10.0),
    ("Low Affinity", 10.0, 15.0),
    ("Change Affinity", 15.0, 20.0),
    ("High Skew", 20.0, 25.0),
]

PHASE_DESCRIPTIONS = {
    "Warmup": "Initial warmup",
    "Partitioned": "theta=0.8 and affinity transaction ratio=80%",
    "Low Affinity": "affinity transaction ratio=20%",
    "Change Affinity": "restore affinity transaction ratio to 80% and replace about 50% of friend relationships",
    "High Skew": "theta=0.95",
}

PARTITIONED_PHASE = "Partitioned"
TARGET_SYSTEM = "MP-Router"
TPS_PATTERN = re.compile(r"\[Exec TPS\]\s+([\d.]+)\s+txn/sec")


def parse_tps(path: Path) -> list[float]:
    values = []
    with path.open(encoding="utf-8", errors="ignore") as result_file:
        for line in result_file:
            match = TPS_PATTERN.search(line)
            if match:
                values.append(float(match.group(1)))
    if not values:
        raise RuntimeError(f"No [Exec TPS] samples found in {path}")
    return values


def phase_slice(
    values: list[float], start_minutes: float, end_minutes: float, source_interval_seconds: float
) -> list[float]:
    start = round(start_minutes * 60.0 / source_interval_seconds)
    end = round(end_minutes * 60.0 / source_interval_seconds)
    return values[start:end]


def extract(
    result_dir: Path, source_interval_seconds: float
) -> tuple[list[tuple[str, list[float]]], list[tuple[str, list[int]]], list[str]]:
    raw_by_system = {}
    warnings = []
    required_samples = round(PHASES[-1][2] * 60.0 / source_interval_seconds)

    for system, mode in SYSTEMS:
        result_file = result_dir / f"m{mode}" / "result.txt"
        if not result_file.exists():
            raise FileNotFoundError(result_file)
        values = parse_tps(result_file)
        raw_by_system[system] = values
        if len(values) < required_samples - 1:
            warnings.append(
                f"{system}: expected about {required_samples} samples for 25 minutes, found {len(values)}"
            )

    phase_tps = []
    phase_counts = []
    for phase, start_minutes, end_minutes in PHASES:
        averages = []
        counts = []
        for system, _ in SYSTEMS:
            samples = phase_slice(
                raw_by_system[system], start_minutes, end_minutes, source_interval_seconds
            )
            if not samples:
                raise RuntimeError(f"{system}: no samples in phase {phase}")
            averages.append(round(sum(samples) / len(samples), 2))
            counts.append(len(samples))
        phase_tps.append((phase, averages))
        phase_counts.append((phase, counts))

    return phase_tps, phase_counts, warnings


def derive_metrics(
    phase_tps: list[tuple[str, list[float]]],
) -> tuple[list[tuple[str, list[float]]], list[tuple[str, list[float]]]]:
    systems = [name for name, _ in SYSTEMS]
    phase_lookup = dict(phase_tps)
    partitioned = phase_lookup[PARTITIONED_PHASE]

    retention = []
    for phase, values in phase_tps:
        retention.append(
            (
                phase,
                [round(value / reference * 100.0, 2) for value, reference in zip(values, partitioned)],
            )
        )

    target_index = systems.index(TARGET_SYSTEM)
    baseline_indices = [index for index, name in enumerate(systems) if name != TARGET_SYSTEM]
    target_over_baselines = []
    for phase, values in phase_tps:
        target_value = values[target_index]
        target_over_baselines.append(
            (phase, [round(target_value / values[index], 2) for index in baseline_indices])
        )

    return retention, target_over_baselines


def write_module(
    output: Path,
    result_dir: Path,
    source_interval_seconds: float,
    phase_tps: list[tuple[str, list[float]]],
    phase_counts: list[tuple[str, list[int]]],
    retention: list[tuple[str, list[float]]],
    target_over_baselines: list[tuple[str, list[float]]],
    warnings: list[str],
) -> None:
    systems = [name for name, _ in SYSTEMS]
    baseline_systems = [name for name in systems if name != TARGET_SYSTEM]
    phase_bounds = [(name, start, end) for name, start, end in PHASES]
    lines = [
        "# Auto-generated by extract_fig15_yashan_phases.py.",
        "# Do not edit by hand; rerun the extractor with the intended result directory.",
        "# Phase means use raw TPS samples, without plot-time resampling.",
        "#",
        f"# Source: {result_dir}",
        "",
        f"SYSTEMS = {pformat(systems, width=120)}",
        "",
        f"BASELINE_SYSTEMS = {pformat(baseline_systems, width=120)}",
        "",
        f"TARGET_SYSTEM = {TARGET_SYSTEM!r}",
        "",
        f"SOURCE_INTERVAL_SECONDS = {source_interval_seconds!r}",
        "",
        f"PHASE_BOUNDS_MINUTES = {pformat(phase_bounds, width=120)}",
        "",
        f"PHASE_DESCRIPTIONS = {pformat(PHASE_DESCRIPTIONS, width=120, sort_dicts=False)}",
        "",
        f"phase_throughput_tps = {pformat(phase_tps, width=120)}",
        "",
        f"phase_sample_counts = {pformat(phase_counts, width=120)}",
        "",
        "# Each phase's throughput as a percentage of the same system's Partitioned phase.",
        f"retention_vs_partitioned_percent = {pformat(retention, width=120)}",
        "",
        f"# {TARGET_SYSTEM} throughput divided by each baseline in BASELINE_SYSTEMS.",
        f"target_over_baselines_x = {pformat(target_over_baselines, width=120)}",
        "",
        "MISSING = []",
        "",
        f"WARNINGS = {pformat(warnings, width=120)}",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


def print_summary(
    phase_tps: list[tuple[str, list[float]]],
    retention: list[tuple[str, list[float]]],
    target_over_baselines: list[tuple[str, list[float]]],
) -> None:
    systems = [name for name, _ in SYSTEMS]
    print("\nAverage throughput (KTPS)")
    print("Phase".ljust(18) + " ".join(name.rjust(10) for name in systems))
    for phase, values in phase_tps:
        print(phase.ljust(18) + " ".join(f"{value / 1000.0:10.2f}" for value in values))

    target_index = systems.index(TARGET_SYSTEM)
    retention_lookup = dict(retention)
    ratio_lookup = dict(target_over_baselines)
    print(f"\n{TARGET_SYSTEM} retention vs. Partitioned")
    for phase in ("Low Affinity", "Change Affinity", "High Skew"):
        print(f"  {phase}: {retention_lookup[phase][target_index]:.2f}%")

    print(f"\n{TARGET_SYSTEM} over baselines")
    for phase in ("Partitioned", "Low Affinity", "Change Affinity", "High Skew"):
        ratios = ratio_lookup[phase]
        print(f"  {phase}: {min(ratios):.2f}--{max(ratios):.2f}x")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", type=Path, default=DEFAULT_RESULT_DIR)
    parser.add_argument("--source-interval-seconds", type=float, default=2.0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    if args.source_interval_seconds <= 0:
        raise ValueError("--source-interval-seconds must be positive")

    phase_tps, phase_counts, warnings = extract(args.result_dir, args.source_interval_seconds)
    retention, target_over_baselines = derive_metrics(phase_tps)
    write_module(
        args.output,
        args.result_dir,
        args.source_interval_seconds,
        phase_tps,
        phase_counts,
        retention,
        target_over_baselines,
        warnings,
    )

    print(f"Wrote {args.output}")
    print(f"Source: {args.result_dir}")
    print_summary(phase_tps, retention, target_over_baselines)
    for warning in warnings:
        print(f"Warning: {warning}")


if __name__ == "__main__":
    main()
