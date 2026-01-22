#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot System Average Execution Time barchart.
Combined Zipfian and Hotspot plots side-by-side.
"""

import matplotlib.pyplot as plt
import os
import shutil
from matplotlib import rcParams

# Configuration (same as Q1.py)
# Check for LaTeX availability before enabling
if shutil.which('latex'):
    try:
        rcParams.update({
            "text.usetex": True,
            "font.family": "serif",
            "font.serif": ["Times", "Computer Modern Roman"],
            "axes.unicode_minus": False,
            "text.latex.preamble": r"\usepackage{amsmath}",
        })
    except Exception:
        pass
else:
    rcParams["text.usetex"] = False

# Global setting for hatch (texture) line width
rcParams['hatch.linewidth'] = 0.3 

def plot_subgroup(ax, data, systems, colors, hatches, bar_width, xlabel=None):
    labels = [item[0] for item in data]
    # Transpose data to get a list of values for each system
    system_values = []
    for i in range(len(systems)):
        system_values.append([item[1][i] for item in data])
        
    n_vars = len(data)
    x_base = range(n_vars)
    n_sys = len(systems)

    # Plot bars
    for i in range(n_sys):
        offset = (i - (n_sys - 1) / 2) * bar_width
        ax.bar(
            [x + offset for x in x_base], 
            system_values[i], 
            width=bar_width, 
            label=systems[i],
            color=colors[i],
            hatch=hatches[i],
            edgecolor="#404040AD",
            linewidth=0.8,
            zorder=3
        )

    ax.set_xticks(x_base)
    # Rotation 0 is better for short labels like numbers or percentages
    ax.set_xticklabels(labels, rotation=0, ha='center', fontsize=12)
    
    # Reduce margins on left and right
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)

    ax.grid(axis='y', linestyle='--', alpha=0.5, zorder=0)

    # Calculate min value to set start of Y-axis
    all_values = [val for sublist in system_values for val in sublist]
    min_val = min(all_values)
    # Start y-axis at ~60% of min value to emphasize top differences, but keep bar visible
    bottom_val = int(0)
    ax.set_ylim(bottom=bottom_val)
    
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=14)

def main():
    # Output file
    outdir = "figs"
    outfile = "system_avg_execution_time_combined.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # Systems
    systems = [
        "Random", 
        "MinWaiting", 
        "Page Hash",
        "Page Affinity", 
        "MP-Router"
    ]
    
    # Colors
    # colors = ["#85bfe995", "#ff7e0e8f", "#2ca02c88", "#B157D77E", "#e47474"] 
    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#e47474"] 
    # Hatches
    hatches = ['////', '\\\\\\\\', 'xxxx', 'oo', '....']

    zipfian_data = [
        ("0.6",   [4.03,  4.06, 3.31,  3.4,  2.34]),
        ("0.7",   [4.37,  4.47, 3.53,  4,    2.58]),
        ("0.8",   [4.87,  5.05, 4.11,  4.67, 3.06]),
        ("0.9",   [5.96,  6.24, 5.43,  5.88, 3.49]),
        ("0.95",  [7.62,  7.83, 8.66,  7.92, 3.45]),
    ]
    
    hotspot_data = [
        ("100%",   [3.85, 3.86, 3.17,  3.17, 2.33]),
        ("10%",    [4.25,  4.53, 3.44,  3.59, 2.42]),
        ("1%",     [6.1,   6.44, 5.08,  6.18, 4.02]),
        ("0.1%",   [7.4,  8.12, 6.94, 7.43, 5.23])
    ]

    # Figure setup: 1 row, 2 columns, sharing Y axis
    fig_w, fig_h = 14, 5
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(fig_w, fig_h), sharey=True)
    
    # Bar configuration
    total_width = 0.8       
    n_sys = len(systems)    
    bar_width = total_width / n_sys

    # Plot Subgroups
    plot_subgroup(ax1, zipfian_data, systems, colors, hatches, bar_width, xlabel=r"Skewness ($\theta$)")
    plot_subgroup(ax2, hotspot_data, systems, colors, hatches, bar_width, xlabel="Hotspot Fraction")
    
    # Set Y-label only for the first plot
    ax1.set_ylabel("Average Execution Time (ms)", fontsize=14)
    ax2.set_ylabel("Average Execution Time (ms)", fontsize=14)

    # Common Legend
    # We take handles and labels from one of the axes
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles, 
        labels,
        loc='lower center', 
        bbox_to_anchor=(0.5, 0.89), # Higher position
        prop={'weight': 'bold', 'size': 14}, 
        handlelength=2.5,
        handleheight=1.5,
        frameon=False,
        ncol=5 
    )
    
    plt.tight_layout()
    # Adjust top margin to accommodate the legend
    plt.subplots_adjust(top=0.88, wspace=0.1)
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
