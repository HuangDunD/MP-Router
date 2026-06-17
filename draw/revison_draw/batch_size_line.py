#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot throughput vs batch size (log-scale x-axis).
Style matches throughput_bar.py.
"""

import os
import shutil
import matplotlib.pyplot as plt
from matplotlib import rcParams
try:
	import revision_extracted_data as revision_data
except ImportError:
	revision_data = None

# Configuration (match throughput_bar.py)
if shutil.which("latex"):
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

rcParams["font.family"] = "serif"
rcParams["font.serif"] = ["Arial"]
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 14
rcParams['xtick.labelsize'] = 12
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 12

def main() -> None:
	# Data
	batch_sizes = [10, 100, 500, 1000, 5000, 10000, 50000, 100000]
	throughput = [2090.62, 16682.91, 17793.46, 18822.3, 19505.85, 21858.69, 21844.15, 23121.71]
	if revision_data:
		pairs = [
			(int(label), values[0])
			for label, values in revision_data.batch_size_data
			if values[0] is not None
		]
		batch_sizes = [label for label, _ in pairs]
		throughput = [value for _, value in pairs]

	# Convert to KTPS for consistency with other plots
	throughput_k = [val / 1000.0 for val in throughput]

	# Figure setup (slightly smaller than previous plots)
	fig_w, fig_h = 6, 4
	fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h))

	ax.plot(
		batch_sizes,
		throughput_k,
		color="#215aecc1",
		marker="s",
		markersize=8,
		linewidth=3,
		# zorder=3,
	)

	ax.set_xscale("log")
	ax.set_xlabel("Txn Window Size", fontsize=20)
	ax.set_ylabel("Throughput (KTPS)", fontsize=20)
	ax.grid(axis="both", linestyle="--", alpha=0.5, zorder=0)

	# Keep tight margins
	ax.set_xlim(min(batch_sizes) * 0.8, max(batch_sizes) * 1.2)

	# Save
	outdir = "figs"
	outfile = "throughput_vs_batch_size.pdf"
	os.makedirs(outdir, exist_ok=True)
	out_path = os.path.join(outdir, outfile)
	plt.tight_layout(pad=0.2)
	fig.subplots_adjust(left=0.12, right=0.98, top=0.96, bottom=0.22)
	plt.savefig(out_path, dpi=600, bbox_inches="tight", pad_inches=0.02)
	print(f"Saved figure: {out_path}")
	plt.close(fig)


if __name__ == "__main__":
	main()
