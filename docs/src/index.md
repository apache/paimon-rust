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

# Apache Paimon Rust

The Rust implementation of [Apache Paimon](https://paimon.apache.org/) — a streaming data lake platform with high-speed data ingestion, changelog tracking, and efficient real-time analytics.

## Overview

Apache Paimon Rust provides native Rust libraries for reading and writing Paimon tables, enabling high-performance data lake access from the Rust ecosystem.

Key features:

- Native Rust reader for Paimon table format
- Support for local filesystem, S3, and OSS storage backends
- REST Catalog integration
- Apache DataFusion integration for SQL queries

## Status

The project is under active development, tracking the [0.1.0 milestone](https://github.com/apache/paimon-rust/issues/3).

## License

Apache Paimon Rust is licensed under the [Apache License 2.0](https://github.com/apache/paimon-rust/blob/main/LICENSE).
