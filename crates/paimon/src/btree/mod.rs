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

//! BTree index implementation compatible with Apache Paimon Java.
//!
//! File layout:
//! ```text
//!    +-----------------------------------+------+
//!    |             Footer                |      |
//!    +-----------------------------------+      |
//!    |           Index Block             |      +--> Loaded on open
//!    +-----------------------------------+      |
//!    |        Bloom Filter Block         |      |
//!    +-----------------------------------+------+
//!    |         Null Bitmap Block         |      |
//!    +-----------------------------------+      |
//!    |            Data Block             |      |
//!    +-----------------------------------+      +--> Loaded on requested
//!    |              ......               |      |
//!    +-----------------------------------+      |
//!    |            Data Block             |      |
//!    +-----------------------------------+------+
//! ```

mod block;
mod footer;
pub(crate) mod key_serde;
mod meta;
pub(crate) mod query;
mod reader;
mod sst_file;
mod var_len;
mod writer;

pub use footer::BTreeFileFooter;
pub use key_serde::{make_key_comparator, serialize_datum};
pub use meta::BTreeIndexMeta;
pub use query::IndexQuery;
pub use reader::BTreeIndexReader;
pub use writer::BTreeIndexWriter;

#[cfg(test)]
pub(crate) mod test_util;
#[cfg(test)]
mod tests;
