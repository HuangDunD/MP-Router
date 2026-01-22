#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot System Throughput barchart.
Data hardcoded from user request.
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

def main():
    # Output file
    outdir = "figs"
    outfile = "avg_exectime.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # -----------------------------
    # Data Setup
    # -----------------------------
    
    # Systems (Legend)
    systems = [
        "Random", 
        "MinWaiting", 
        "Page Hash",
        "Page Affinity", 
        "MP-Router"
    ]
    
    # Workloads (X-axis) & Values
    # Format: (Workload Name, [Value_Sys1, Value_Sys2, Value_Sys3, Value_Sys4])
    # Note: Values will be converted to KTPS (divided by 1000)

    raw_data = [
        ("Zipfian 0.6",   [4.03,  4.06, 0,  3.4,  2.34]),
        ("Zipfian 0.7",   [4.37,  4.47, 0,  4,    2.58]),
        ("Zipfian 0.8",   [4.87,  5.05, 0,  4.67, 3.06]),
        ("Zipfian 0.9",   [5.96,  6.24, 0,  4.17, 3.49]),
        ("Zipfian 0.95",  [7.62,  7.83, 0,  7.92, 3.72]),
        ("Hotspot 1.0",   [3.85,  3.86, 0,  3.17, 2.33]),
        ("Hotspot 0.1",   [4.25,  4.53, 0,  3.59, 2.42]),
        ("Hotspot 0.01",  [6.1,   6.44, 0,  6.18, 4.02]),
        ("Hotspot 0.001", [7.4,  0, 0, 7.43, 5.23])
    ]

    labels = [item[0] for item in raw_data]
    # Transpose data to get a list of values for each system
    # system_values[i] corresponds to the list of values for system[i] across all workloads
    # Convert to KTPS
    system_values = []
    for i in range(len(systems)):
        system_values.append([item[1][i] for item in raw_data])
        
    # -----------------------------

    # Plotting
    # -----------------------------
    
    # Figure setup
    fig_w, fig_h = 10, 5
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    
    # Bar configuration
    total_width = 0.8       # Total width of the group of bars
    n_vars = len(raw_data)  # Number of groups (workloads)
    n_sys = len(systems)    # Number of bars per group
    bar_width = total_width / n_sys
    
    # X positions
    # x_base corresponds to the center of each group
    x_base = range(n_vars)
    
    # Colors or Patterns (You can customize these)
    # Using a distinct color palette
    # colors = ['#cccccc', '#969696', '#636363', '#d62728'] # 3 greys + 1 red for MP-Router highlight?
    # Or standard matplotlib colors
    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#e47474"] 
    
    # Hatches patterns (网格/纹理)
    # 'xx' = cross hatch, '//' = diagonal hatch, etc.
    # Increase density by repeating characters (e.g. '////')
    hatches = ['////', '\\\\\\\\', 'xxxx', 'oooo', '....']

    # Plot bars
    for i in range(n_sys):
        # Calculate offset for each bar
        # x_base is center. 
        # offset should center the GROUP of bars around x_base
        offset = (i - (n_sys - 1) / 2) * bar_width
        
        ax.bar(
            [x + offset for x in x_base], 
            system_values[i], 
            width=bar_width, 
            label=systems[i],
            color=colors[i],
            hatch=hatches[i],
            edgecolor='#404040',
            linewidth=0.8,
            zorder=3
        )

    # -----------------------------
    # Formatting
    # -----------------------------
    
    ax.set_ylabel("Average Execution Time (ms)", fontsize=14)
    # ax.set_xlabel("Workload", fontsize=14)
    # ax.set_title("System Throughput Comparison", fontsize=16, pad=12) # Removed title
    
    # X-axis ticks
    ax.set_xticks(x_base)
    ax.set_xticklabels(labels, rotation=35, ha='right', fontsize=11)
    
    # Reduce margins on left and right
    # Bars occupy [center - 0.4, center + 0.4] since total_width=0.8
    # Set limits to leave just a small gap (e.g. 0.5 total distance from center)
    ax.set_xlim(-0.55, len(labels) - 1 + 0.55)

    # Y-axis configs
    ax.grid(axis='y', linestyle='--', alpha=0.5, zorder=0)
    ax.set_ylim(bottom=0)
    
    # Legend - outside top center
    ax.legend(
        loc='lower center', 
        bbox_to_anchor=(0.5, 1.02),
        fontsize=11, 
        frameon=False,
        ncol=4  # Horizontal legend
    )
    
    # Layout adjustment
    plt.tight_layout()
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=300) # Increased DPI for better raster quality (though PDF is vector)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
