#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot Time Breakdown horizontal barchart.
Stacked bar chart for Router Thread and DB Connector Thread.
"""

import matplotlib.pyplot as plt
import os
import shutil
import numpy as np
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

# Global setting
rcParams['hatch.linewidth'] = 0.3 
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Arial']
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 12
rcParams['xtick.labelsize'] = 10
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 10

def main():
    # Output file
    outdir = "figs"
    outfile = "time_breakdown_horiz.pdf"
    os.makedirs(outdir, exist_ok=True)
    
    # Categories (Two Bars)
    categories = ["Router Thread", "DB Connector"]
    
    # Data Mapping
    # Structure: { GroupName: { Category: [ (Label, Value, Color, Hatch), ... ] } }
    
    # Skew 0.8 Data (Calculated Previously)
    r_wait_8 = 45901.88
    r_fetch_8 = 2864.48
    r_prep_8 = 30141.85
    r_sched_cf_8 = 14243.89
    r_sched_c_8 = 5833.61
    r_total_8 = r_wait_8 + r_fetch_8 + r_prep_8 + r_sched_cf_8 + r_sched_c_8
    
    db_wait_8 = 328.47
    db_exec_8 = 102741.04
    db_update_8 = 250.85
    db_total_8 = db_wait_8 + db_exec_8 + db_update_8

    # Skew 0.95 Data (Placeholder - User needs to update these values)
    # Assuming higher conflict -> more sched_c, less wait?
    r_wait_95 = 101562.42
    r_fetch_95 = 2780.59
    r_prep_95 = 35071.09
    r_sched_cf_95 = 11924.21
    r_sched_c_95 = 19832.42 # Increased conflict time
    r_total_95 = r_wait_95 + r_fetch_95 + r_prep_95 + r_sched_cf_95 + r_sched_c_95

    db_wait_95 = 32587.21
    db_exec_95 = 144310.22
    db_update_95 = 281.86
    db_total_95 = db_wait_95 + db_exec_95 + db_update_95

    # Helper to generate props list
    def get_router_props(w, f, p, scf, sc, total):
        return [
            ("Wait", w / total * 100, "#d3d3d3", ""),
            ("Fetch Txn", f / total * 100, "#85c0e9", ""), 
            ("Preprocess", p / total * 100, "#4a90e2", "...."),
            ("Sched. (No-Confl.)", scf / total * 100, "#ffb347", ""),
            ("Sched. (Confl.)", sc / total * 100, "#e47474", "////")
        ]
    
    def get_db_props(w, e, u, total):
        return [
            ("Wait", w / total * 100, "#d3d3d3", ""),
            ("Execute Txn", e / total * 100, "#77dd77", "xxxx"),
            ("Update Key-Page", u / total * 100, "#ff9999", "////")
        ]

    # Organize Data
    # Y-axis will correspond to (Category, Skew) pairs
    # Order: [Router-0.8, DB-0.8, Router-0.95, DB-0.95]
    # Grouped by Skewness to show system state at that skew
    
    bar_groups = [
        ("Router Thread\n(θ:0.80)", get_router_props(r_wait_8, r_fetch_8, r_prep_8, r_sched_cf_8, r_sched_c_8, r_total_8)),
        ("DB Connector\n(θ:0.80)", get_db_props(db_wait_8, db_exec_8, db_update_8, db_total_8)),
        ("Router Thread\n(θ:0.95)", get_router_props(r_wait_95, r_fetch_95, r_prep_95, r_sched_cf_95, r_sched_c_95, r_total_95)),
        ("DB Connector\n(θ:0.95)", get_db_props(db_wait_95, db_exec_95, db_update_95, db_total_95))
    ]
    
    # Plotting
    fig_w, fig_h = 6, 4 # Increased height for 4 bars
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    
    y_pos = np.arange(len(bar_groups))
    # Add spacing between groups (Skew 0.8 vs Skew 0.95)
    # 0, 1 -> 0.8; 3, 4 -> 0.95 (Gap at 2)
    y_pos_visual = [0, 1, 2.5, 3.5]
    
    bar_width = 0.6
    legend_elements = {}
    
    for i, (name, props) in enumerate(bar_groups):
        left = 0
        for label, width, color, hatch in props:
            current_label = label if label not in legend_elements else "_" + label
            bar = ax.barh(
                y_pos_visual[i], width, height=bar_width, left=left, 
                label=current_label, color=color, hatch=hatch, 
                edgecolor="#404040AD", linewidth=0.8, zorder=3
            )
            if label not in legend_elements:
                legend_elements[label] = bar[0]
            left += width

    # Formatting
    ax.set_yticks(y_pos_visual)
    ax.set_yticklabels([g[0] for g in bar_groups], fontsize=8, multialignment='center')
    ax.invert_yaxis() 
    
    ax.set_xlabel("Time Breakdown (%)", fontsize=12)
    ax.set_xlim(0, 100)
    
    ax.grid(axis='x', linestyle='--', alpha=0.5, zorder=0)
    
    # Custom Legend
    legend_keys = ["Wait", "Fetch Txn", "Preprocess", "Sched. (No-Confl.)", "Sched. (Confl.)", "Execute Txn", "Update Key-Page"]
    handles = [legend_elements[k] for k in legend_keys]
    
    # Move legend higher up to avoid overlap
    ax.legend(
        handles,
        legend_keys,
        loc='upper center', 
        bbox_to_anchor=(0.44, 1.35), # Moved up from 1.25
        prop={'size': 9}, 
        handlelength=1.5, 
        handleheight=1.2,
        frameon=False,
        ncol=4,
        columnspacing=1.0
    )
    
    # Adjusted layout for larger legend, reserve more top space
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    
    # Save
    out_path = os.path.join(outdir, outfile)
    plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.05)
    print(f"Saved figure: {out_path}")
    plt.close(fig)

if __name__ == "__main__":
    main()
