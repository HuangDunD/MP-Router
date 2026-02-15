#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot System Throughput barchart.
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

# Configure fonts for academic papers (Times New Roman is standard)
rcParams['font.family'] = 'serif'
# Use Times New Roman as the primary serif font
rcParams['font.serif'] = ['Arial']
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 14
rcParams['xtick.labelsize'] = 12
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 12

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
    ax.set_xticklabels(labels, rotation=0, ha='center', fontsize=14)
    
    # Reduce margins on left and right
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)

    ax.grid(axis='y', linestyle='--', alpha=0.5, zorder=0)
    
    # Set Y-axis range
    # You can set the range manually using ax.set_ylim(bottom, top)
    # Here we set it from 0 to 6 to cover all data points (max ~5.14)
    ax.set_ylim(0, 5.5)
    
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=16)

def main():
    # Output file
    outdir = "figs"
    outfile = "system_cache_fusion_combined.pdf"
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
    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#e47474"] 
    # Hatches
    hatches = ['////', '\\\\\\\\', 'xxxx', 'oo', '....']

    zipfian_data = [
        ("0.6",   [2.37,  2.22, 1.36,  1.83,  0.88]),
        ("0.7",   [2.59,  2.41, 1.57,  2.18,  0.99]),
        ("0.8",   [3.05,  2.85, 1.97,  2.57,  1.15]),
        ("0.9",   [3.79,  3.62, 2.71,  3.41,  1.33]),
        ("0.95",  [4.35,  4.16, 3.39,  4.04,  1.33]),
    ]
    
    hotspot_data = [
        ("100%",   [2.21,  2.04, 1.33,  1.70,  0.86]),
        ("10%",    [2.48,  2.30, 1.43,  1.95,  0.85]),
        ("1%",     [3.74,  3.62, 2.44,  3.44,  1.63]),
        ("0.1%",   [5.14,  4.99, 4.26,  4.83,  2.89])
    ]

    # Figure setup: 1 row, 2 columns, separate Y axis to allow individual zooming
    fig_w, fig_h = 8, 3
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(fig_w, fig_h), sharey=False)
    
    # Bar configuration
    total_width = 0.8       
    n_sys = len(systems)    
    bar_width = total_width / n_sys

    # Plot Subgroups
    plot_subgroup(ax1, zipfian_data, systems, colors, hatches, bar_width, xlabel=r"Skewness ($\theta$)")
    plot_subgroup(ax2, hotspot_data, systems, colors, hatches, bar_width, xlabel="Hotspot Fraction")
    
    # Set Y-label for both plots as they have different scales
    ax1.set_ylabel("Ownership Transfers / Txn", fontsize=14)
    ax2.set_ylabel("Ownership Transfers / Txn", fontsize=14)
    ax1.yaxis.set_label_coords(-0.08, 0.42)
    ax2.yaxis.set_label_coords(-0.08, 0.42)

    # Common Legend
    # We take handles and labels from one of the axes
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles, 
        labels,
        loc='lower center', 
        bbox_to_anchor=(0.5, 0.86), # Higher position
        prop={'weight': 'bold', 'size': 12}, 
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=5,
        columnspacing=1.0
    )
    
    plt.tight_layout(rect=[0, 0, 1, 0.92])
    # Adjust top margin to accommodate the legend
    # Add wspace to prevent Y-axis overlap
    plt.subplots_adjust(top=0.88, wspace=0.20)
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.05)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
