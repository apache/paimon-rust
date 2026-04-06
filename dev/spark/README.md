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

# Spark Provisioning for Integration Tests

This directory contains the Spark + Paimon setup that provisions test tables into `/tmp/paimon-warehouse`.

## Prerequisites

- Docker (via colima or Docker Desktop)
- `docker compose`

## Provision Test Data

```bash
# Build and run (from repo root):
make docker-up

# Or manually:
docker compose -f dev/docker-compose.yaml build --build-arg PAIMON_VERSION=1.3.1
docker compose -f dev/docker-compose.yaml run --rm spark-paimon
```

`provision.py` automatically clears the warehouse directory before creating tables, so re-running is always safe.

### colima Users

With colima, Docker volumes mount inside the colima VM, **not** on the macOS host filesystem. After provisioning, copy the data to the host:

```bash
colima ssh -- sudo tar cf - -C /tmp paimon-warehouse | tar xf - -C /tmp
```

## Run Integration Tests

```bash
cargo test -p paimon-integration-tests
```

## Files

- `Dockerfile` — Spark 3.5 + Paimon connector image
- `spark-defaults.conf` — Spark config with Paimon catalog
- `provision.py` — Creates all test tables and inserts data
