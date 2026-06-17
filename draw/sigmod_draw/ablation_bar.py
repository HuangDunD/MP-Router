#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot Ablation Study barchart.
Comparing MP-Router with an ablated version (e.g., w/o Optimization).
"""

import matplotlib.pyplot as plt
import os
import shutil
from matplotlib import rcParams

# Configuration
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

rcParams['hatch.linewidth'] = 0.3 
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Arial']
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 8
rcParams['xtick.labelsize'] = 8
rcParams['ytick.labelsize'] = 8
rcParams['legend.fontsize'] = 12

def main():
    # Output file
    outdir = "figs"
    outfile = "ablation_bar.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # Systems to compare
    systems = [
        "MP-Router", 
        "w/o barrier",
        "w/o critical-path",
        "w/o scheduling" 
    ]
    
    # Colors
    colors = ["#e47474", "#74a8e4c2", "#74e4bfce", "#e4e474"]
    # Hatches
    hatches = ['....', '////', 'xxxx', '----']

    # Data from ablation.py attachment
    # Format: (Skewness, [MP-Router, w/o Scheduling])
    # 19333.63	18128.2
    # 16090.22	15202.88
    # 7346.82	10850.71
    data = [
        ("0.8",   [21108.93, 19333.63, 18128.2, 18930.5]),
        ("0.9",   [17183.29, 16090.22, 15202.88, 12773.06]),
        ("0.95",  [14183.9, 7346.82, 10850.71, 6688.58]),
    ]
    
    # Figure setup: Slimmer single plot
    fig_w, fig_h = 2.5, 3.5
    fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h))
    
    # Bar configuration
    total_width = 0.6       
    n_sys = len(systems)    
    bar_width = total_width / n_sys

    labels = [item[0] for item in data]
    # Transpose data
    system_values = []
    for i in range(n_sys):
        system_values.append([item[1][i] / 1000.0 for item in data])
        
    n_vars = len(data)
    x_base = range(n_vars)

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
    ax.set_xticklabels(labels, rotation=0, ha='center', fontsize=10)
    ax.set_xlim(-0.5, len(labels) - 1 + 0.5)

    ax.grid(axis='y', linestyle='--', alpha=0.5, zorder=0)
    
    # Y-axis settings
    ax.set_ylabel("Throughput (KTPS)", fontsize=12)
    ax.set_ylim(bottom=0)
    
    # Reduce number of y-ticks
    ax.locator_params(axis='y', nbins=5)
    
    ax.set_xlabel(r"Skewness ($\theta$)", fontsize=12)

    # Legend
    ax.legend(
        loc='upper center', 
        bbox_to_anchor=(0.4, 1.30),
        prop={'weight': 'bold', 'size': 8}, 
        handlelength=1.5, 
        handleheight=1.2,
        frameon=False,
        ncol=2,
        columnspacing=1.0
    )
    
    # Adjust layout to make room for legend
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.05)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
