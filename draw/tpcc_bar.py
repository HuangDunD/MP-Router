#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot TPCC Throughput barchart.
Grouped by Access Pattern (Normal, 50%, 80%, 90%).
"""

import matplotlib.pyplot as plt
import os
import shutil
import numpy as np
from matplotlib import rcParams

# Configuration (same as throughput_bar.py)
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

# Configure fonts for academic papers
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Arial']
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 14
rcParams['xtick.labelsize'] = 12
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 12

def main():
    # Output file
    outdir = "figs"
    outfile = "tpcc_throughput_bar.pdf"
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

    # Data Structure: list of (Label, values_list)
    # Values are in TPS
    # We will convert to K-TPM inside the loop (val * 60 / 1000)
    
    # Raw Data in TPS
    raw_data_map = [
        ("Normal", [1167.04, 1300.29, 915.01, 1078.9, 5588.79]),
        ("50%",    [1354.9,  1330.77, 1289.93, 1320.62, 5909.92]),
        ("80%",    [1317.72, 1281.79, 1270.28, 1239.66, 5165.38]),
        ("90%",    [1247.59, 1193.56, 1294.28, 1202.52, 6172.2]),
    ]

    # Prepare data for plotting (Transpose: list of [val_sys1_norm, val_sys1_50, ...])
    # Actually matplotlib bar grouped needs: for each system, a list of values across groups
    
    n_groups = len(raw_data_map)
    labels = [item[0] for item in raw_data_map]
    
    # Transpose to [System][Group]
    system_values = []
    for i in range(len(systems)):
        # Calculate K-TPM for this system across all groups
        vals = []
        for _, group_vals in raw_data_map:
            tps = group_vals[i]
            ktpm = tps * 60 / 1000.0
            vals.append(ktpm)
        system_values.append(vals)

    # Figure setup
    # Single plot size (approx half of the 2-subplot figure width)
    fig_w, fig_h = 6.5, 5.5
    fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h))
    
    # Bar configuration
    total_width = 0.8       
    n_sys = len(systems)    
    bar_width = total_width / n_sys
    x_base = np.arange(n_groups)

    # Plot bars
    for i in range(n_sys):
        offset = (i - (n_sys - 1) / 2) * bar_width
        ax.bar(
            x_base + offset, 
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
    ax.set_xlabel("Hot-Spot Concentration", fontsize=14)
    
    # Margins and Grid
    ax.set_xlim(-0.55, n_groups - 1 + 0.55)
    ax.grid(axis='y', linestyle='--', alpha=0.5, zorder=0)
    
    # Start y-axis
    bottom_val = 0
    ax.set_ylim(bottom=bottom_val)
    
    # Y-axis Label
    # Using raw string for latex math
    ax.set_ylabel(r"Throughput ($\times 10^3$ TPM)", fontsize=16)

    # Legend
    # Similar style to throughput_bar.py: Outside top/bottom
    handles, legend_labels = ax.get_legend_handles_labels()
    
    # throughput_bar puts legend at bottom center bbox_to_anchor=(0.5, 0.86) ?? 
    # Wait, in throughput_bar it was low because it had 2 subplots. 
    # Here meaningful legend should be top or bottom. 
    # Usually top for academic specific graphs unless space constrained.
    # User's ref used: loc='lower center', bbox_to_anchor=(0.5, 0.86) relative to fig?
    # Let's try placing it above.
    
    ax.legend(
        handles,
        legend_labels,
        loc='upper center', 
        bbox_to_anchor=(0.45, 1.15), 
        prop={'weight': 'bold', 'size': 10}, 
        handlelength=1.5, 
        handleheight=1.2,
        frameon=False,
        ncol=5,
        columnspacing=1.0
    )
    
    plt.tight_layout(rect=[0, 0, 1, 0.80])
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.05)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
