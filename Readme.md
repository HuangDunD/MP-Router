# MP-Router

Code artifact for the paper:

**MP-Router: An Efficient Transaction Router for Multi-Primary Shared-Storage Database Systems**

## 1. Overview

Multi-primary shared-storage architectures are increasingly adopted over
primary-secondary deployments as a promising approach to scaling transactional
workloads. However, allowing any primary to process any transaction introduces
new challenges for transaction routing. Suboptimal routing may trigger
excessive page ownership transfers and higher contention, thereby degrading
overall system performance.

MP-Router is a middleware-based transaction router that adaptively dispatches
transactions to suitable primaries without requiring database kernel
modifications. It combines lightweight external metadata for page-access and
ownership inference with a lookahead assignment/scheduling engine.

Core ideas implemented in this repository include:

- Fast path for conflict-free transaction assignment.
- Greedy ownership-evolution planning for conflicting transactions.
- Critical-path oriented transaction dispatching.
- A page barrier mechanism for coordination under contention.

## 2. Repository Layout

- `serve/test/`: router driver, workload runners, scheduling logic, and
  benchmark entrypoint (`run.cc`).
- `serve/region/`: region generation and related utilities.
- `serve/mlp/`: optional MLP components.
- `serve/log/`: logging implementation.
- `core/`: partitioning and core helper code.
- `core/util/`: utility functions, including distributions and JSON helpers.
- `config/`: workload and cluster configuration examples.
- `scripts/`: result summarization helpers.
- `draw/`: figure-generation scripts.
- `thirdparty/rapidjson/`: bundled RapidJSON headers.

## 3. Build and Runtime Requirements

### 3.1 System

- Linux.
- CMake >= 3.10.
- GCC/G++ with C++20 support.
- `make`.

### 3.2 Required Libraries

- `libpq` (PostgreSQL client library).
- `libpqxx` (C++ PostgreSQL client).
- METIS and GKlib.
- PostgreSQL common libraries (`pgcommon`, `pgport`) available in your
  PostgreSQL installation path.
- YashanDB client library (`libyascli`) and headers, because the current
  `CMakeLists.txt` links `yashan_test` and the benchmark binary with it.

### 3.3 Optional Libraries

- MySQL client library and headers. If available, CMake enables
  `WITH_MYSQL_CLIENT` and DB type `2`; otherwise DB type `2` is unavailable.

The current CMake configuration expects:

- METIS/GKlib under `$HOME/local/include` and `$HOME/local/lib`.
- PostgreSQL libraries in standard system paths and
  `/usr/lib/postgresql/14/lib`.
- YashanDB client under `/root/yashandb-client`.

If your environment differs, update include/library paths in `CMakeLists.txt`.

## 4. Environment Setup Example

The following is an example for Debian/Ubuntu-like systems:

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config libpq-dev libpqxx-dev postgresql-server-dev-14
```

For METIS/GKlib, install them and ensure headers/libs are visible at:

- `$HOME/local/include/metis.h`
- `$HOME/local/lib/libmetis.*`
- `$HOME/local/lib/libGKlib.*`

## 5. Build

From repository root:

```bash
mkdir -p build
cd build
cmake ..
make -j
```

Main executable:

- `build/serve/test/run`

## 6. Database Connection Configuration

Before running experiments, pass cluster endpoints with repeated
`--db-connection` options. `ComputeNodeCount` is inferred from the number of
configured endpoints.

For PostgreSQL-compatible deployments (`--db-type 0`):

```bash
--db-connection "host=<host1> port=<port> user=<user> password=<password> dbname=<db>" \
--db-connection "host=<host2> port=<port> user=<user> password=<password> dbname=<db>"
```

For YashanDB (`--db-type 1`), use either `ip_port=<host:port>` or
`host=<host> port=<port>` plus user/password fields.

For MySQL (`--db-type 2`), build with MySQL client headers/libraries available.

## 7. Running MP-Router

### 7.1 Check CLI Options

```bash
./build/serve/test/run --help
```

Important options:

- `--workload <smallbank|ycsb|tpcc>`
- `--system-mode <int>`
- `--db-type <0|1|2>`
- `--db-connection <conninfo>`
- `--account-count <int>`
- `--warehouse-count <int>` (TPC-C)
- `--worker-threads <int>`
- `--try-count <int>`
- `--partition-interval <int>`
- `--sys_extend_size <int>`
- `--sys_index_extend_size <int>`
- `--affinity-txn-ratio <double>`
- `--batch-size <int>`
- `--num-bucket <int>`
- `--router-threads <int>`
- `--time-run`
- `--warmup-seconds <int>`
- `--run-seconds <int>`
- `--skip-load-data`

### 7.2 Example Run Command

From repository root:

```bash
./build/serve/test/run \
  --workload smallbank \
  --system-mode 11 \
  --db-type 0 \
  --db-connection "host=<host1> port=<port> user=<user> password=<password> dbname=smallbank" \
  --db-connection "host=<host2> port=<port> user=<user> password=<password> dbname=smallbank" \
  --access-pattern 0 \
  --zipfian-theta 0.90 \
  --account-count 5000000 \
  --worker-threads 16 \
  --try-count 35000 \
  --sys_extend_size 300000 \
  --sys_index_extend_size 30000 \
  --affinity-txn-ratio 0.8 \
  --batch-size 10000 \
  --num-bucket 4
```

### 7.3 Notes on `system-mode`

Different `system-mode` values correspond to different routing
baselines/variants. In the current implementation, notable values include:

- `0`: random router baseline.
- `2`: key/page hash routing baseline.
- `23`: score-based router with METIS and load balancing.
- `28`: Chimera-inspired phased baseline.
- `11`: MP-Router pipeline mode.
- `13`: score-based router.
- `26`: MP-Router without page barrier (ablation).
- `27`: MP-Router without critical queue (ablation).

Refer to `serve/test/run.cc` for the full switch-case mapping used by the
current code version.

## 8. Outputs

By default, runtime output is mirrored to:

- `result.txt`

Additional logs/metrics may include:

- `partitioning_log.log`
- KWR reports, when enabled.
- Workload/router statistics printed during execution.

## 9. Reproducibility Tips

- Keep the same dataset size and thread count across compared modes.
- Warm up before collecting final metrics.
- Use consistent data loading behavior across compared configurations.
- Run each configuration multiple times and report average/variance.

## 10. Citation

If you use this artifact, please cite:

```text
MP-Router: An Efficient Transaction Router for Multi-Primary Shared-Storage Database Systems
```
