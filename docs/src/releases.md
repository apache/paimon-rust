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

# Releases

## Release Policy

Apache Paimon Rust follows [Semantic Versioning](https://semver.org/). All releases are published to [crates.io](https://crates.io/crates/paimon).

## Upcoming

### 0.3.0 (In Development)

See [GitHub Issues](https://github.com/apache/paimon-rust/issues) for the current roadmap.

## Past Releases

### [0.2.0](https://github.com/apache/paimon-rust/releases/tag/v0.2.0)

Key features:

- Primary-key table read/write support with sort-merge deduplication
- DataFusion DML support (INSERT OVERWRITE, TRUNCATE TABLE, DROP PARTITION, CALL procedures)
- System tables ($snapshots, $tags, $manifests, $schemas, $partitions, $files, $table_indexes)
- Multi-catalog support, session-scoped dynamic options (SET/RESET), and temporary tables
- Lumina vector index read infrastructure and Vortex columnar file format support
- Exact COUNT(*) pushdown via partition statistics
- Partial-update merge engine support (fixed-bucket and dynamic-bucket)

### [0.1.0](https://github.com/apache/paimon-rust/releases/tag/v0.1.0)

The first release of Apache Paimon Rust.

Key features:

- Paimon table format reader (Parquet, ORC, Avro) with schema evolution
- Filesystem and REST Catalog support
- Apache DataFusion integration (catalog, predicate/projection push-down, time travel, partition pruning, statistics)
- Deletion vectors, data-level stats pruning, BTree global index, Tantivy full-text search index
- Commit pipeline with SnapshotCommit abstraction
- Go / Python bindings
