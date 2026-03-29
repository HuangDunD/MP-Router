#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import re
import argparse
import sys
import os
import shutil
import csv
import itertools
from matplotlib import rcParams

# === 绘图风格配置 (参考 throughput_bar.py) ===
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
rcParams['font.serif'] = ['Arial'] # 或者 'Times New Roman'
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 14
rcParams['xtick.labelsize'] = 12
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 12
# ==========================================

def parse_file(filepath):
    """
    解析文件，提取 [Exec TPS] 后的数值。
    """
    tps_values = []
    
    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        return tps_values

    # Regex to capture the float value after [Exec TPS]
    # Example: [Exec TPS] 1238.88 txn/sec (total: 2478). ...
    pattern = re.compile(r'\[Exec TPS\]\s+([\d\.]+)\s+txn/sec')

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = pattern.search(line)
            if match:
                try:
                    val = float(match.group(1)) / 1000.0  # Convert to KTPS
                    tps_values.append(val)
                except ValueError:
                    continue
    return tps_values

def plot_tps(data_map, output_file="tps_timeline.pdf"):
    """
    绘制 TPS 时间序列图
    """
    fig, ax = plt.subplots(figsize=(8, 4.5))
    
    # 定义一些线条样式和颜色，以防同时画多条线
    linestyles = ['-', '--', '-.', ':']
    linestyles = [':', '-.', '--', '--', '-.', '--'] 
    # 6 distinct colors for the 6 modes
    # colors = ["#85c0e9", "#2ca02c", "#d62728", "#9467bd", "#ff7e0e", "#17becf"] 
    colors = ["#85c0e9", "#ff7e0e8f", "#2ca02c99", "#B157D790", "#e0e474dc", "#e47474"] 
    # colors = ["#e47474", "#85c0e9", "#2ca02c", "#ff7e0e"] 
    
    for i, (label, values) in enumerate(data_map.items()):
        # x = range(1, len(values) + 1)
        # Assuming each epoch is 5 seconds, convert to minutes
        x = [v * 5 / 60.0 for v in range(1, len(values) + 1)]
        
        ax.plot(x, values, 
                label=label.replace("_", "\_") if rcParams["text.usetex"] else label, 
                linewidth=1.5, 
                linestyle=linestyles[i % len(linestyles)],
                color=colors[i % len(colors)],
                alpha=0.9
                )

    # Add vertical lines for phases (Darker, Thicker)
    line_style = {'color': '#333333', 'linestyle': '--', 'alpha': 0.85, 'linewidth': 2}
    
    ax.axvline(x=5, **line_style)
    ax.axvline(x=10, **line_style)
    ax.axvline(x=15, **line_style)
    ax.axvline(x=20, **line_style)
    ax.axvline(x=25, **line_style)

    # Add centered text annotations between lines
    # Calculate a good Y position (92% of the max y-limit)
    current_ymin, current_ymax = ax.get_ylim()
    y_pos = current_ymax * 0.92
    
    text_style = {'ha': 'center', 'fontsize': 12, 'fontname': 'Arial', 'weight': 'bold', 'color': 'black'}

    ax.text(2.5, y_pos, "Warmup", **text_style)
    ax.text(7.5, y_pos, "Partitioned", **text_style)
    ax.text(12.5, y_pos, "Low Affinity", **text_style)
    ax.text(17.5, y_pos, "Change Affinity", **text_style)
    ax.text(22.5, y_pos, "High Skew", **text_style)

    ax.set_xlabel("Time (Minutes)", fontsize=14)
    ax.set_ylabel("Throughput (KTPS)", fontsize=14)
    ax.grid(True, linestyle='--', alpha=0.4)
    
    # x axis range
    ax.set_xlim(left=0, right=25)
    
    # 自动调整Y轴起始点
    ax.set_ylim(bottom=0)

    # 如果有多条线，或者用户指定了标签，显示图例
    if len(data_map) > 0:
        # ax.legend(frameon=False, loc='best')
        handles, labels = ax.get_legend_handles_labels()
        fig.legend(
            handles, 
            labels,
            loc='upper center', # Change to upper center to better control top alignment
            bbox_to_anchor=(0.5, 1.0), # Anchor at top center of figure
            prop={'weight': 'bold', 'size': 12},
            frameon=False,
            ncol=3, 
            columnspacing=1.5 # Increase spacing slightly
        )

    # Adjust layout to make room for legend at top
    # The top margin needs to be larger if legend is 2 rows
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    
    output_path = os.path.join("figs", output_file)
    os.makedirs("figs", exist_ok=True)
    
    plt.savefig(output_path, dpi=300)
    print(f"Successfully saved figure to: {output_path}")
    plt.close(fig)

def save_to_csv(data_map, output_file="tps_data.csv"):
    """
    Save the parsed TPS data to a CSV file for Excel analysis.
    """
    output_path = os.path.join("figs", output_file)
    os.makedirs("figs", exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        headers = ["Epoch (5s)"] + list(data_map.keys())
        writer.writerow(headers)
        
        # Write data rows
        # Use zip_longest to handle potentially different lengths
        # data_map.values() returns list of lists
        rows = itertools.zip_longest(*data_map.values(), fillvalue="")
        
        for i, row in enumerate(rows):
            epoch = i + 1
            writer.writerow([epoch] + list(row))
            
    print(f"Successfully saved data to: {output_path}")

def main():
    # Hardcoded file paths and labels
    # Expected Layout (2 Rows, 3 Columns):
    # Row 1: Random, MinWaiting, Page Hash
    # Row 2: Page Affinity, MP-Router w/o scheduling, MP-Router
    
    # Matplotlib's default legend behavior with ncol=3 fills via columns first.
    # To get row-wise filling (Row 1 then Row 2), we just provide them in the desired reading order
    # Random, MinWaiting, Page Hash, Page Affinity, MP-Router w/o scheduling, MP-Router
    # AND most importantly, allow Matplotlib to handle the layout.
    
    file_list = [
        "draw/t/m0.txt",
        "draw/t/m25.txt", 
        "draw/t/m2.txt",
        
        "draw/t/m23.txt",
        "draw/t/m13.txt",
        "draw/t/m11.txt",
    ]
    
    label_list = [
        "Random", 
        "MinWaiting", 
        "Page Hash", 

        "Page Affinity", 
        "MP-Router w/o scheduling", 
        "MP-Router", 
    ]

    output_file = "tps_timeline.pdf"

    data_map = {}
    
    for i, filepath in enumerate(file_list):
        label = label_list[i] if i < len(label_list) else os.path.basename(filepath)
        
        print(f"Parsing: {filepath} ...")
        values = parse_file(filepath)
        
        if values:
            print(f"  -> Found {len(values)} data points.")
            data_map[label] = values
        else:
            print(f"  -> No [Exec TPS] data found.")

    if data_map:
        plot_tps(data_map, output_file)
        save_to_csv(data_map, "tps_timeline_data.csv")
    else:
        print("No valid data found to plot.")

if __name__ == "__main__":
    main()
