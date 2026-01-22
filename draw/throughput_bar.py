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
        system_values.append([item[1][i] / 1000.0 for item in data])
        
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
    
    # Calculate min value to set start of Y-axis
    all_values = [val for sublist in system_values for val in sublist]
    min_val = min(all_values)
    # Start y-axis at ~60% of min value to emphasize top differences, but keep bar visible
    bottom_val = int(5)
    ax.set_ylim(bottom=bottom_val)
    
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=16)

def main():
    # Output file
    outdir = "figs"
    outfile = "system_throughput_combined.pdf"
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
        ("0.6",   [15153.36, 15677.88, 19228.33, 18096.38, 27000.52]),
        ("0.7",   [14196.76, 14418.65, 18075.71, 15786.33, 24866.46]),
        ("0.8",   [12857.31, 12789.5,  15467.65, 13634.97, 21108.93]),
        ("0.9",   [10486.94, 10454.78, 11695.23, 10949.11, 16722.47]),
        ("0.95",  [8287.47,  8222.43,  7373.93,  8110.59,  14183.9]),
    ]
    
    hotspot_data = [
        ("100%",   [14411.01, 16357.22, 20074.59, 19134.37, 26707.86]),
        ("10%",    [14743.19, 14150.31, 18504.18, 17196.27, 26156.22]),
        ("1%",     [10308.37, 10461.43, 12545.3,  10876.99, 17248.04]),
        ("0.1%",   [8510.3,   7899.38,  9208.47,  8608.64,  12055.23])
    ]

    # Figure setup: 1 row, 2 columns, separate Y axis to allow individual zooming
    # Reduced size for better fit in papers (approx single/double column width context)
    fig_w, fig_h = 8, 3.5 
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(fig_w, fig_h), sharey=False)
    
    # Bar configuration
    total_width = 0.8       
    n_sys = len(systems)    
    bar_width = total_width / n_sys

    # Plot Subgroups
    plot_subgroup(ax1, zipfian_data, systems, colors, hatches, bar_width, xlabel=r"Skewness ($\theta$)")
    plot_subgroup(ax2, hotspot_data, systems, colors, hatches, bar_width, xlabel="Hotspot Fraction")
    
    # Set Y-label for both plots as they have different scales
    ax1.set_ylabel("Throughput (KTPS)", fontsize=16)
    ax2.set_ylabel("Throughput (KTPS)", fontsize=16)

    # Common Legend
    # We take handles and labels from one of the axes
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(
        handles, 
        labels,
        loc='lower center', 
        bbox_to_anchor=(0.5, 0.86), # Adjusted position
        prop={'weight': 'bold', 'size': 12},  # Reduced size to fit
        handlelength=1.5, # Reduced handle length
        handleheight=1.2,
        frameon=False,
        ncol=5,
        columnspacing=1.0
    )
    
    # Use rect parameter to reserve space for legend at the top
    # [left, bottom, right, top] in normalized (0, 1) figure coordinates
    plt.tight_layout(rect=[0, 0, 1, 0.92])
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
