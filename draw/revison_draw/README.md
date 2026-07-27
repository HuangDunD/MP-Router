# Revision Plot Commands

This directory uses one extracted-data module per paper figure. Each plotting
script should import only its corresponding module under `extracted_data/` to
avoid mixing results from different experiment batches.

Run commands from this directory:

## Summary Generator

`summarize_mp_router_results.py` is a legacy/convenience summarizer used only
when a figure or table consumes `summary.csv` / `summary.md`. Most current
figure extractors read `result.txt` or KWR HTML directly.

Command:

```bash
python3 summarize_mp_router_results.py "../../../vldb_res/RESULT_DIR"
```

Currently, `Fig. 4` SmallBank and `Table 1` use `summary.csv`; regenerate it
only when those source experiment folders change.

## Fig. 4: Overall Throughput

Data module:

```text
extracted_data/fig4_overall.py
```

Extraction script:

```text
extracted_data/extract_fig4_overall.py
```

Data sources:

```text
SmallBank: vldb_res/20260709000616(main)/summary.csv
TPC-C:     vldb_res/20260716015113(tpcc-200)/*/m*/result.txt
```

Output:

```text
figs/fig4_workload_throughput_combined.pdf
```

Commands:

```bash
python3 summarize_mp_router_results.py "../../../vldb_res/20260709000616(main)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig4_overall.py \
  --smallbank-dir "../../../vldb_res/20260709000616(main)" \
  --tpcc-dir "../../../vldb_res/20260716015113(tpcc-200)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig4_workload_throughput_combined.py
```

Notes:

- `Fig4_workload_throughput_combined.py` is the active Fig. 4 plotting script.
- The first command is needed only if the SmallBank `summary.csv` needs to be
  regenerated from raw result folders.
- `smallbank_zipfian_bar.py` and `tpcc_bar.py` are older standalone plotting
  scripts and are not used for the combined Fig. 4 figure.
- `Fig4_workload_throughput_combined.py` imports only
  `extracted_data.fig4_overall`; it should not fall back to
  `revision_extracted_data.py`.

## Fig. 5: Ownership Transfers

Data module:

```text
extracted_data/fig5_cache_fusion.py
```

Extraction script:

```text
extracted_data/extract_fig5_cache_fusion_4kwr.py
```

Data sources:

```text
SmallBank: vldb_res/20260716160331(smallbank-4kwr)/*/m*/kwr_*node*_end.html
TPC-C:     vldb_res/20260716232528(tpcc-4kwr)/*/m*/kwr_*node*_end.html
```

Metric:

```text
SmallBank: ownership transfers / txn from KWR cache-fusion waits
TPC-C:     ownership transfers / txn from KWR cache-fusion waits
```

Output:

```text
figs/fig5_cache_fusion_bar.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig5_cache_fusion_4kwr.py \
  --smallbank-dir "../../../vldb_res/20260716160331(smallbank-4kwr)" \
  --tpcc-dir "../../../vldb_res/20260716232528(tpcc-4kwr)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig5_cache_fusion_bar.py
```

Notes:

- `Fig5_cache_fusion_bar.py` is the active Fig. 5 plotting script.
- The extractor reads KWR HTML reports directly and does not use
  `summary.md`, `summary.csv`, `case_id`, or `case_plan`.
- The extractor uses all available KWR reports per run. It computes a
  transaction-weighted average when per-report transaction counts are available;
  otherwise, it averages the KWR-reported waits-per-transaction values. If only
  one report exists, the metric reduces to that report's value.

## Fig. 6: Affinity Transaction Ratio

Data module:

```text
extracted_data/fig6_affinity.py
```

Extraction script:

```text
extracted_data/extract_fig6_affinity.py
```

Data source:

```text
vldb_res/20260716102209(affinity)/*/m*/result.txt
```

Output:

```text
figs/fig6_affinity_ratio_bar.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig6_affinity.py \
  --affinity-dir "../../../vldb_res/20260716102209(affinity)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig6_affinity_ratio_bar.py
```

Notes:

- The extractor reads `Throughput (after warmup)` directly from `result.txt`.
- The extractor does not use `summary.md`, `summary.csv`, `case_id`, or
  `case_plan`.
- The extractor auto-detects both current folder names with `_n4` and older
  folder names without `_n4`.

## Fig. 7: Uniform-Hotspot Distribution

Data module:

```text
extracted_data/fig7_uniform_hotspot.py
```

Extraction script:

```text
extracted_data/extract_fig7_uniform_hotspot.py
```

Data source:

```text
vldb_res/20260709000616(main)/smallbank_p2_HsFrac*_HsProb0.8_*/m*/result.txt
```

Output:

```text
figs/fig7_uniform_hotspot_bar.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig7_uniform_hotspot.py \
  --main-dir "../../../vldb_res/20260709000616(main)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig7_uniform_hotspot_bar.py
```

Notes:

- The x-axis sweeps the hot-spot fraction with hot-spot access probability
  fixed at `0.8`.
- The extractor reads `Throughput (after warmup)` directly from `result.txt`
  and does not use `summary.md`, `summary.csv`, `case_id`, or `case_plan`.

## Fig. 8-9: Scalability

Data modules:

```text
extracted_data/fig8_thread_scalability.py
extracted_data/fig9_node_scalability.py
```

Extraction scripts:

```text
extracted_data/extract_fig8_thread_scalability.py
extracted_data/extract_fig9_node_scalability.py
```

Data sources:

```text
Connection scalability: vldb_res/20260709000616(main)/smallbank_p1_ZipfTheta0.8_*_t*_r0.8_b10000_*/m*/result.txt
Primary scalability:    vldb_res/20260718005118(8-node)/smallbank_p1_ZipfTheta0.8_*_n*_nb1_whpart0_kp1.1_mlp0/m*/result.txt
```

Output:

```text
figs/fig8_9_scalability_combined.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig8_thread_scalability.py \
  --main-dir "../../../vldb_res/20260709000616(main)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig9_node_scalability.py \
  --node-dir "../../../vldb_res/20260718005118(8-node)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig8_9_scalability_combined.py
```

Notes:

- The left panel sweeps the number of connections from `2` to `32`; the right
  panel sweeps the number of primary instances from `1` to `8`.
- Both panels use the default SmallBank setting with `theta=0.8`.
- The extractors read `Throughput (after warmup)` directly from `result.txt`
  and do not use `summary.md`, `summary.csv`, `case_id`, or `case_plan`.

## Fig. 10: Transaction Window Size

Data module:

```text
extracted_data/fig10_batch_size.py
```

Extraction script:

```text
extracted_data/extract_fig10_batch_size.py
```

Data source:

```text
vldb_res/20260717100527(batch-size)/smallbank_p1_ZipfTheta0.8_*_b*_n4_*/m11/result.txt
```

Output:

```text
figs/fig10_batch_size_line.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig10_batch_size.py \
  --batch-dir "../../../vldb_res/20260717100527(batch-size)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig10_batch_size_line.py
```

Notes:

- The x-axis uses log scale and sweeps `Txn Window Size`.
- The extractor reads `Throughput (after warmup)` directly from `result.txt`
  and does not use `summary.md`, `summary.csv`, `case_id`, or `case_plan`.

## Fig. 11: Ablation Study

Data module:

```text
extracted_data/fig14_ablation_breakdown.py
```

Extraction script:

```text
extracted_data/extract_fig14_ablation_breakdown.py
```

Data sources:

```text
Main results: vldb_res/20260709000616(main)/*/m*/result.txt
Ablation:     vldb_res/20260718190512(abaltion-new)/*/m*/result.txt
```

Output:

```text
figs/fig11_ablation_bar.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig14_ablation_breakdown.py \
  --main-dir "../../../vldb_res/20260709000616(main)" \
  --ablation-dir "../../../vldb_res/20260718190512(abaltion-new)" \
  --breakdown-dir "../../../vldb_res/20260718190512(abaltion-new)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig11_ablation_bar.py
```

Notes:

- The figure compares RR, MP-Router, and MP-Router variants without scheduling,
  critical-path prioritization, or page barrier.
- The extractor reads raw `result.txt` files directly and does not use
  `summary.md`, `summary.csv`, `case_id`, or `case_plan`.

## Fig. 12: Key-Page Map Capacity

Data module:

```text
extracted_data/fig12_key_page_map.py
```

Extraction script:

```text
extracted_data/extract_fig12_key_page_map.py
```

Data source:

```text
vldb_res/20260709000616(main)/smallbank_p1_ZipfTheta0.8_*_kp*/m*/result.txt
```

Output:

```text
figs/fig12_key_page_map_bar.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig12_key_page_map.py \
  --main-dir "../../../vldb_res/20260709000616(main)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig12_key_page_map_bar.py
```

Notes:

- The x-axis sweeps the key-page map capacity from `20%` to `100%`.
- The extractor reads `Throughput (after warmup)` directly from `result.txt`
  and does not use `summary.md`, `summary.csv`, `case_id`, or `case_plan`.


## Table 1: SmallBank Latency

Script:

```text
Tab1_latency_table.py
```

Data source:

```text
vldb_res/20260709000616(main)/summary.csv
```

Output:

```text
figs/smallbank_latency_table.tex
figs/smallbank_latency_table.md
```

Command:

```bash
python3 summarize_mp_router_results.py "../../../vldb_res/20260709000616(main)"
python3 Tab1_latency_table.py
```

Notes:

- The table reports DB-side and end-to-end latency at `theta=0.6` and
  `theta=0.9`.
- The summary-generation command is needed only when the source result folder
  has changed.
- The script matches rows by workload parameters and does not use `case_id` or
  `case_plan`.

## Fig. 13: MLP

Data module:

```text
extracted_data/fig13_mlp.py
```

Extraction script:

```text
extracted_data/extract_fig13_mlp.py
```

Data sources:

```text
MP-Router baseline: vldb_res/20260709000616(main)/*/m11/result.txt
MLP:                vldb_res/20260716071500(mlp)/*/m11/result.txt
```

Output:

```text
figs/fig13_mlp.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig13_mlp.py \
  --baseline-dir "../../../vldb_res/20260709000616(main)" \
  --mlp-dir "../../../vldb_res/20260716071500(mlp)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig13_mlp.py
```

Notes:

- The extractor reads `Throughput (after warmup)` directly from `result.txt`.
- The extractor does not use `summary.md`, `summary.csv`, `case_id`, or
  `case_plan`.

## Fig. 14: Router-Only Throughput and Time Breakdown

Data modules:

```text
extracted_data/fig11_route_only.py
extracted_data/fig14_ablation_breakdown.py
```

Extraction scripts:

```text
extracted_data/extract_fig11_route_only.py
extracted_data/extract_fig14_ablation_breakdown.py
```

Data sources:

```text
Router-only:    vldb_res/20260717234540(route-only)/smallbank_p1_ZipfTheta*_rt16_gt8/m11/result.txt
Time breakdown: vldb_res/20260718190512(abaltion-new)/*/m11/result.txt
```

Output:

```text
figs/fig14_router_efficiency_breakdown.pdf
```

Commands:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig11_route_only.py \
  --route-only-dir "../../../vldb_res/20260717234540(route-only)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 extracted_data/extract_fig14_ablation_breakdown.py \
  --main-dir "../../../vldb_res/20260709000616(main)" \
  --ablation-dir "../../../vldb_res/20260718190512(abaltion-new)" \
  --breakdown-dir "../../../vldb_res/20260718190512(abaltion-new)"

MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig14_router_efficiency_breakdown.py
```

Notes:

- The left panel shows router-only throughput and transaction mix.
- The time-breakdown panel uses `theta=0.6,0.9` and the latest timing block
  from each `result.txt`.
- The extractors read raw `result.txt` files directly and do not use
  `summary.md`, `summary.csv`, `case_id`, or `case_plan`.

## Fig. 15: YashanDB Timeline

Script:

```text
Fig15_yashan_tps_timeline.py
```

Data source:

```text
vldb_res/20260715020857(yashan)/smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0/m*/result.txt
```

Output:

```text
figs/fig15_yashan_tps_timeline.pdf
```

Command:

```bash
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.mplconfig" python3 Fig15_yashan_tps_timeline.py \
  "../../../vldb_res/20260715020857(yashan)/smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0/m0/result.txt" \
  "../../../vldb_res/20260715020857(yashan)/smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0/m25/result.txt" \
  "../../../vldb_res/20260715020857(yashan)/smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0/m2/result.txt" \
  "../../../vldb_res/20260715020857(yashan)/smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0/m23/result.txt" \
  "../../../vldb_res/20260715020857(yashan)/smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0/m28/result.txt" \
  "../../../vldb_res/20260715020857(yashan)/smallbank_p1_ZipfTheta0.8_ZipfGenfinite_acc10000000_t16_r0.8_b10000_nb1_whpart0_kp1.1_mlp0/m11/result.txt" \
  --labels RR MWR PHR PAR CPR MP-Router \
  --target-interval-seconds 10 \
  --output fig15_yashan_tps_timeline.pdf
```

Notes:

- The script reads per-interval `[Exec TPS]` lines directly from `result.txt`.
- The command uses 10-second averaging for a smoother timeline.

Phase statistics extractor:

```text
extracted_data/extract_fig15_yashan_phases.py
```

Command:

```bash
python3 extracted_data/extract_fig15_yashan_phases.py
```

Generated data module:

```text
extracted_data/fig15_yashan_phases.py
```

The extractor computes each five-minute phase from the raw two-second TPS
samples, without the timeline's 10-second plot smoothing. It reports mean TPS,
retention relative to the Partitioned phase, and MP-Router's throughput ratio
over each baseline. The 15--20 minute phase restores the affinity transaction
ratio to 80% and replaces about 50% of the friend relationships; the runtime
log records the friend-graph change but does not print the ratio restoration.
