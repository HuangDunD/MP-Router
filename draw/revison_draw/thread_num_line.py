#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot System Throughput line chart varying thread number.
"""

import matplotlib.pyplot as plt
import os
import shutil
import numpy as np
from matplotlib import rcParams
try:
    import revision_extracted_data as revision_data
except ImportError:
    revision_data = None

# Configuration
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

# Configure fonts for academic papers
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Arial'] # Try Times New Roman first
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 14
rcParams['xtick.labelsize'] = 12
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 12

def main():
    # Output file
    outdir = "figs"
    outfile = "throughput_thread_num_line.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # Systems
    systems = [
        # "Random", 
        # "MinWaiting", 
        # "Page Hash",
        # "Page Affinity", 
        "RR",
        "MWR",
        "PHR",
        "PAR",
        "CPR",
        "MP-Router"
    ]
    if revision_data:
        systems = revision_data.MAIN_SYSTEMS
    
    # Data from user provided file
    # x-axis (Thread Count / Number of Connections)
    thread_counts = [2, 4, 8, 16, 32, 64, 128]
    
    # y-axis data (Throughput)
    # 线程数量	Random	MinWaiting	Page-Hash	Page Affinity	MP-Router
    # Data is in Row-Major order from the table
    # 2	1542.44	3242.1	2643.57	3009.41	4154.37
    # ...
    
    # Let's verify the data input
    # Format: [Random, MinWaiting, Page-Hash, Page Affinity, MP-Router]

    data_points = [
        [3216.34, 3242.1, 2643.57, 3009.41, 4337.1, 4154.37],   # 2
        [6345.65, 6152.64, 8748.55, 6162.55, 7841.18, 9689.27],   # 4
        [11028.25, 10433.35, 13132.58, 10398.09, 13353.68, 15344.3], # 8
        [16126.5, 14918.56, 20608.81, 14586, 20761.28, 27164.24], # 16
        [17037.13, 18659.13, 24316.22, 19416.2, 23876.09, 33023.04], # 32
        [22087.77, 22408.79, 30857.59, 27126.44, 26532.45, 37409.3], # 64
        [21334.01, 22189.82, 28714.79, 28181.52, 24640.16, 35214.9]  # 128
    ]
    if revision_data:
        thread_counts = [int(label) for label, _ in revision_data.thread_num_data]
        data_points = [values for _, values in revision_data.thread_num_data]
    
    # Transpose to get series for each system
    system_series = list(map(list, zip(*data_points)))
    
    # Convert to KTPS
    for i in range(len(system_series)):
        system_series[i] = [val / 1000.0 for val in system_series[i]]

    # Colors (matching bar chart) : Random, MinWaiting, Page Hash, Page Affinity, MP-Router
    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#d6d027", "#e47474"]
    
    # Markers for line chart distinction
    markers = ['o', 's', '^', 'p', '8','D'] # Circle, Square, Triangle Up, Diamond, Plus (Pentagon)
    linestyles = ['--', '--', '--', '--', '--', '-'] # MP-Router solid, others dashed
    linewidths = [3, 3, 3, 3, 3, 3.5] # Make MP-Router slightly thicker

    # Figure setup
    fig, ax = plt.subplots(figsize=(6, 4))
    
    x = np.arange(len(thread_counts))
    
    # Plot lines
    for i in range(len(systems)):
        ax.plot(
            x, 
            system_series[i], 
            label=systems[i], 
            color=colors[i], 
            marker=markers[i],
            markersize=8,
            linewidth=linewidths[i],
            linestyle=linestyles[i],
            markeredgecolor='white',
            markeredgewidth=0.5
        )

    # Axis configs
    ax.set_xticks(x)
    ax.set_xticklabels([str(tc) for tc in thread_counts])
    
    ax.set_xlabel("Number of Connections", fontsize=20)
    ax.set_ylabel("Throughput (KTPS)", fontsize=20)
    
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.grid(axis='x', linestyle='--', alpha=0.3)
    
    # Set ylim to start from 0 
    ax.set_ylim(bottom=0)

    # Legend
    ax.legend(
        loc='upper left',
        bbox_to_anchor=(0.02, 0.98),
        # prop={'size': 10},
        frameon=True,
        edgecolor='lightgray',
        framealpha=0.9,
        ncol=1,
        fancybox=True
    )
    
    plt.tight_layout()
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.05)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
