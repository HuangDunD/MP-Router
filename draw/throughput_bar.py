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
    outfile = "system_throughput.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # -----------------------------
    # Data Setup
    # -----------------------------
    
    # Systems (Legend)
    systems = [
        "Random", 
        "Static Partition", 
        "Static Partition + Dynamic Ownership", 
        "MP-Router"
    ]
    
    # Workloads (X-axis) & Values
    # Format: (Workload Name, [Value_Sys1, Value_Sys2, Value_Sys3, Value_Sys4])
    # Note: Values will be converted to KTPS (divided by 1000)
    raw_data = [
        ("Random",        [13002.34, 14481.03, 24791.16, 26404.4]),
        ("Zipfian 0",     [13678.44, 14086.24, 22834.1,  23034.43]),
        ("Zipfian 0.3",   [14140.61, 12894,    22984.48, 27622.47]),
        ("Zipfian 0.6",   [11707.75, 14114.56, 20035.43, 26198.87]),
        ("Zipfian 0.7",   [10863.29, 11698.69, 20440.6,  24455.41]),
        ("Zipfian 0.8",   [10427.71, 9866.34,  14725.59, 15745.76]),
        ("Zipfian 0.9",   [5322.47,  8383.64,  4898.87,  13160.49]),
        ("Zipfian 0.95",  [3302.7,   3636.72,  3274.09,  8438.13]),
        ("Hotspot 0.1",   [12390.51, 13676.31, 23939.11, 26181.11]),
        ("Hotspot 0.01",  [9634.82,  10263.93, 15986.37, 15751.76]),
        ("Hotspot 0.001", [7428.37,  7903.49,  8601.1,   9463.23]),
    ]

    labels = [item[0] for item in raw_data]
    # Transpose data to get a list of values for each system
    # system_values[i] corresponds to the list of values for system[i] across all workloads
    # Convert to KTPS
    system_values = []
    for i in range(len(systems)):
        system_values.append([item[1][i] / 1000.0 for item in raw_data])
        
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
    colors = ['#cccccc', '#969696', '#636363', '#d62728'] # 3 greys + 1 red for MP-Router highlight?
    # Or standard matplotlib colors
    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#e47474"] 
    
    # Hatches patterns (网格/纹理)
    # 'xx' = cross hatch, '//' = diagonal hatch, etc.
    # Increase density by repeating characters (e.g. '////')
    hatches = ['////', '\\\\\\\\', 'xxxx', '....']

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
    
    ax.set_ylabel("Throughput (KTPS)", fontsize=14)
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
