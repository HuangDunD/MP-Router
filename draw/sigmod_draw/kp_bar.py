#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot System Throughput barchart against Key-Page Map Capacity.
Style matches affinity_ratio_bar.py
"""

import matplotlib.pyplot as plt
import os
import shutil
from matplotlib import rcParams
import matplotlib.ticker as ticker

# Configuration (Consistent with throughput_bar.py)
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

# Configure fonts
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Arial'] # Matches the unusual setting in throughput_bar.py
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 14
rcParams['xtick.labelsize'] = 12
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 12

def plot_single_group(ax, data, systems, colors, hatches, bar_width, xlabel=None):
    labels = [item[0] for item in data]
    # Transpose data
    system_values = []
    for i in range(len(systems)):
        # Convert to KTPS (assuming input is raw TPS)
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
    ax.set_xticklabels(labels, rotation=0, ha='center', fontsize=14)
    
    # Margins
    ax.set_xlim(-0.6, len(labels) - 1 + 0.6)

    ax.grid(axis='y', linestyle='--', alpha=0.5, zorder=0)
    
    # Y-axis scaling
    ax.set_ylim(5, 25)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
    
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=16)
    
    ax.set_ylabel("Throughput (KTPS)", fontsize=16)

def main():
    # Output file
    outdir = "figs"
    outfile = "system_throughput_kp.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # Systems to plot
    systems = [
        "PHR",
        "PAR",
        "CPR",
        "MP-Router"
    ]
    
    # Colors & Hatches (Mapped consistently from affinity_ratio_bar.py)
    # Page Hash: "#2ca02c99", 'xxxx'
    # Page Affinity: "#B157D790", 'oo'
    # MP-Router: "#e47474", '....'
    colors = ["#2ca02c99", "#B157D790", "#d6d027", "#e47474"] 
    hatches = ['xxxx', 'oo', 'OOOO', '....']

    # Key Page Map Ratio Data (from original kp_bar.py text)
    # Format: ("Ratio Label", [Val_Sys1, Val_Sys2, Val_Sys3])
    data = [
        ("20%", [11496.55, 12200.75, 13101.61, 17446.65]),
        ("40%", [13194.30, 12556.18, 13382.58, 18315.40]),
        ("60%", [12473.91, 12979.45, 15195.31, 19276.57]),
        ("80%", [15044.52, 13345.97, 15732.82, 19616.36]),
        ("100%", [15859.42, 14018.83, 16060.25, 21509.88]),
    ]

    # Figure setup: Single plot
    fig_w, fig_h = 4, 3 # Adjusted for single plot aspect ratio
    fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h))
    
    # Bar configuration
    total_width = 0.6       
    n_sys = len(systems)    
    bar_width = total_width / n_sys

    # Plot
    plot_single_group(ax, data, systems, colors, hatches, bar_width, xlabel="Key-Page Map Capacity")
    
    # Legend (Placed at top)
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles, 
        labels,
        loc='upper center', 
        bbox_to_anchor=(0.5, 1.01), # Slightly above the plot
        prop={'weight': 'bold', 'size': 11},
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=4, # 1 row
        columnspacing=0.6
    )
    
    # Layout adjustment
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.05)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
