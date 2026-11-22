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

# Java centroid index fixture

Apache Paimon's `VectorTrainingModelIO` and `CentroidIndexMeta` from the
centroid-sharding implementation wrote the router file and manifest envelopes.
The Java paimon-vector-index 0.7.0-SNAPSHOT JNI trainer and writer produced the
two IVF-FLAT shards. The router training input is four two-dimensional L2
vectors, `[0, 0]`, `[1, 0]`, `[100, 0]`, and `[101, 0]`, with two outer
centers and one local IVF list in each shard. Native labels are 0 through 3.
The Rust test publishes the files at row range [2, 5] to verify label rebasing.
