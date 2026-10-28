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

# Local full-text Top-K sample

- Date: 2026-10-28.
- Source: base `016731e4` plus the accompanying certified Top-K change.
- Compiler: Rust 1.98.0, Cargo release profile.
- Machine: Apple M4 Pro, 48 GiB RAM, macOS 27.0.1.
- Storage: temporary local filesystem; both modes warmed before measurement.
- Workload: 100,000 matching documents, four shards, K=10; 20 samples per mode
  with alternating order. Every run asserts identical IDs and scores.

```bash
cargo run --release --locked -p paimon --example full_text_top_k_benchmark -- \
  100000 20 10 25000
```

| Corpus | Exhaustive p50 | Certified p50 | Exhaustive p95 | Certified p95 |
| --- | ---: | ---: | ---: | ---: |
| skewed | 65.680 ms | 1.300 ms | 71.194 ms | 1.584 ms |
| tied | 61.498 ms | 59.544 ms | 65.693 ms | 65.988 ms |

The skewed corpus has a strict per-shard score cutoff. The tied corpus requires
exhaustive fallback to preserve row-ID ordering. These are warm-cache synthetic
local results, not a general speedup claim or a comparison with Lance. The
benchmark does not measure peak RSS, cloud requests, or cold-cache behavior.

Raw JSON output (milliseconds):

```json
{
  "cache": "warm; alternating path order",
  "certified_ms": [
    1.493625,
    1.3470419999999999,
    1.584334,
    1.265542,
    1.442334,
    1.35375,
    1.527584,
    1.574292,
    1.428708,
    1.104042,
    1.428958,
    0.9946670000000001,
    1.1383750000000001,
    1.020333,
    2.98675,
    1.178459,
    1.2998750000000001,
    1.14975,
    1.281458,
    1.223833
  ],
  "certified_p50_ms": 1.2998750000000001,
  "certified_p95_ms": 1.584334,
  "corpus": "skewed",
  "exhaustive_ms": [
    63.123042000000005,
    64.412542,
    65.869167,
    61.668333,
    73.404917,
    67.536125,
    62.193084000000006,
    67.91274999999999,
    65.6795,
    66.166958,
    65.237833,
    62.372875,
    71.194334,
    64.104334,
    65.31225,
    65.99804099999999,
    68.3735,
    68.70008299999999,
    63.100249999999996,
    70.09375
  ],
  "exhaustive_p50_ms": 65.6795,
  "exhaustive_p95_ms": 71.194334,
  "iterations": 20,
  "k": 10,
  "optimized_build": true,
  "rows": 100000,
  "shards": 4,
  "storage": "local temporary filesystem"
}
```

```json
{
  "cache": "warm; alternating path order",
  "certified_ms": [
    60.786959,
    65.987917,
    63.890249999999995,
    66.674334,
    64.062834,
    58.520042000000004,
    64.140084,
    59.344916999999995,
    59.02675,
    59.543541,
    56.94875,
    59.583791999999995,
    58.512415999999995,
    58.018041,
    59.953208000000004,
    56.848166,
    61.324375,
    60.756958,
    56.141957999999995,
    55.769375000000004
  ],
  "certified_p50_ms": 59.543541,
  "certified_p95_ms": 65.987917,
  "corpus": "tied",
  "exhaustive_ms": [
    61.498125,
    61.992708,
    64.92604200000001,
    63.23775,
    60.250625,
    65.69324999999999,
    63.07212500000001,
    69.43341699999999,
    62.332499999999996,
    59.03325,
    59.881334,
    56.568625,
    61.359958,
    58.073375000000006,
    61.913416999999995,
    61.050541,
    62.432625,
    61.058542,
    57.116375,
    63.045041
  ],
  "exhaustive_p50_ms": 61.498125,
  "exhaustive_p95_ms": 65.69324999999999,
  "iterations": 20,
  "k": 10,
  "optimized_build": true,
  "rows": 100000,
  "shards": 4,
  "storage": "local temporary filesystem"
}
```

