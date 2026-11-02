<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Full-text Top-K benchmark

Run the production full-text scan/read path against a temporary local filesystem
catalog. Each mode uses the same data, index archives and snapshot. The program
checks exact row IDs and scores on every run, warms both modes, alternates their
execution order, and emits JSON with every sample plus p50 and p95 latency.

```bash
cargo run --release --locked -p paimon --features internal-api \
  --example full_text_top_k_benchmark -- \
  10000 10 10 2500
```

Arguments are rows, measured iterations per mode, K, and rows per index shard.
All must be positive. Increase rows and iterations for a representative workload.
The `skewed` corpus gives each shard distinct high scores followed by many common
low-score matches. The `tied` corpus has identical text in every row and exercises
the exhaustive fallback required by Paimon's row-ID tie-break.

The benchmark uses the `internal-api` feature's exhaustive reference reader for
the baseline. Production searches select the optimization automatically without
a table option. The fast path is limited to FAST-mode global full-text
reads over disjoint index row ranges, with no raw-data sources, deletion vectors
or later deletion exclusions. Native search still evaluates the complete query
DSL and applies its allow-list before selecting candidates. A shard requests
K+1 hits and retains K only if the score boundary is strict and the selected row
IDs are distinct. Boundary ties fall back to the original full candidate limit.
FULL/DETAIL, overlapping ranges, and deletion/raw-fallback paths remain exhaustive.
Primary-key and raw temporary-index searches keep their existing behavior.

This is a synthetic, warm-cache local benchmark, not an object-store, cold-cache,
Lance, or whole-system memory comparison. It reports no cross-shard BM25 corpus
normalization change. Record the revision, compiler, CPU, RAM, OS and command with
results. The JSON field `optimized_build` must be true for performance reports.

## Local sample

A [sample run with raw timings](sample-results.md) records 100,000 matching rows,
four shards, K=10 and 20 iterations on an Apple M4 Pro. It illustrates this
synthetic workload only; use the protocol above to evaluate other workloads.
