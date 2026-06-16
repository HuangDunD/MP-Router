#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import re
import argparse
import sys
import os
import shutil
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
                    val = float(match.group(1))
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
    colors = ["#e47474", "#85c0e9", "#2ca02c", "#ff7e0e"] 
    
    for i, (label, values) in enumerate(data_map.items()):
        x = range(1, len(values) + 1)
        ax.plot(x, values, 
                label=label.replace("_", "\_") if rcParams["text.usetex"] else label, 
                linewidth=1.5, 
                linestyle=linestyles[i % len(linestyles)],
                color=colors[i % len(colors)],
                alpha=0.9
                )

    ax.set_xlabel("Time (Epochs)", fontsize=14)
    ax.set_ylabel("Execution TPS", fontsize=14)
    ax.grid(True, linestyle='--', alpha=0.4)
    
    # 自动调整Y轴起始点
    ax.set_ylim(bottom=0)

    # 如果有多条线，或者用户指定了标签，显示图例
    if len(data_map) > 0:
        ax.legend(frameon=False, loc='best')
    
    plt.tight_layout()
    
    output_path = os.path.join("figs", output_file)
    os.makedirs("figs", exist_ok=True)
    
    plt.savefig(output_path, dpi=300)
    print(f"Successfully saved figure to: {output_path}")
    plt.close(fig)

def main():
    parser = argparse.ArgumentParser(description="Plot Exec TPS time series from result files.")
    parser.add_argument("files", nargs='+', help="Path to result.txt file(s).")
    parser.add_argument("--output", "-o", default="tps_timeline.pdf", help="Output PDF file name (saved in figs/).")
    parser.add_argument("--labels", "-l", nargs='+', help="Custom labels for the legend (one per file).")

    args = parser.parse_args()

    data_map = {}
    
    for i, filepath in enumerate(args.files):
        # 确定图例标签
        if args.labels and i < len(args.labels):
            label = args.labels[i]
        else:
             # 尝试从路径中自动生成一个简短的标签
             dirname = os.path.basename(os.path.dirname(filepath))
             if "result_" in dirname:
                 # 提取一些关键参数作为标签，例如 m23
                 parts = dirname.split("_")
                 mode = next((p for p in parts if p.startswith("m")), "Unknown")
                 label = f"Mode {mode[1:]}"
             else:
                 label = os.path.basename(filepath)
        
        print(f"Parsing: {filepath} ...")
        values = parse_file(filepath)
        
        if values:
            print(f"  -> Found {len(values)} data points.")
            data_map[label] = values
        else:
            print(f"  -> No [Exec TPS] data found.")

    if data_map:
        plot_tps(data_map, args.output)
    else:
        print("No valid data found to plot.")

if __name__ == "__main__":
    main()
