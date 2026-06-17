#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot System Throughput barchart against Zipfian Skew for MLP vs MP-Router.
Style matches kp_bar.py
"""

import matplotlib.pyplot as plt
import os
import shutil
from matplotlib import rcParams
import matplotlib.ticker as ticker
import math
try:
    import revision_extracted_data as revision_data
except ImportError:
    revision_data = None

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
    
    all_values = [val for sublist in system_values for val in sublist]
    min_val = min(all_values)
    max_val = max(all_values)
    bottom_val = 0 if min_val / max_val < 0.35 else math.floor(min_val * 0.8 / 10) * 10
    upper_val = math.ceil(max_val * 1.12 / 10) * 10
    ax.set_ylim(bottom_val, upper_val)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(20 if upper_val > 80 else 10))
    
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=16)
    
    ax.set_ylabel("Throughput (KTPS)", fontsize=16)

def main():
    # Output file
    outdir = "figs"
    outfile = "system_throughput_mlp.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # Systems to plot
    systems = [
        "MP-Router with MLP",
        "MP-Router"
    ]
    if revision_data:
        systems = revision_data.MLP_SYSTEMS
    
    # Colors & Hatches (Mapped consistently from affinity_ratio_bar.py)
    colors = ["#B157D790", "#e47474"] 
    hatches = ['oo', '....']

    # MLP vs MP-Router Data
    # Format: ("Zipf Data Label", [Val_Sys1, Val_Sys2])
    data = [
        ("0.6", [3780.39, 27000.52]),
        ("0.7", [2770.00, 24866.46]),
        ("0.8", [2081.37, 21108.93]),
        ("0.9", [1654.59, 17183.29]),
        ("0.95", [1621.85, 14183.90]),
    ]
    if revision_data:
        data = revision_data.mlp_data

    # Figure setup: Single plot
    fig_w, fig_h = 4, 3 # Adjusted for single plot aspect ratio
    fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h))
    
    # Bar configuration
    total_width = 0.5       
    n_sys = len(systems)    
    bar_width = total_width / n_sys

    # Plot
    plot_single_group(ax, data, systems, colors, hatches, bar_width, xlabel="Zipfian Skew")
    
    # Legend (Placed at top)
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles, 
        labels,
        loc='upper center', 
        bbox_to_anchor=(0.55, 1.01), # Slightly above the plot and slightly to the right
        prop={'weight': 'bold', 'size': 11},
        handlelength=1.5,
        handleheight=1.2,
        frameon=False,
        ncol=2, # 1 row
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
