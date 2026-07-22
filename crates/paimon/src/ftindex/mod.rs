// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Full-text index reader backed by the shared `paimon-ftindex-core` engine
//! (the same native core the Java/Python bindings use), reading the v1
//! archive format.

pub mod reader;

// Re-export the core I/O types that appear in this module's public API so that
// consumers can implement `SeekRead` (e.g. a remote range-reader) or name the
// in-memory reader while depending only on `paimon`, without pulling in
// `paimon-ftindex-core` directly.
pub use paimon_ftindex_core::io::{ReadRequest, SeekRead, SliceReader};
pub use reader::{BytesReader, FullTextArchiveReader, FullTextHits};
