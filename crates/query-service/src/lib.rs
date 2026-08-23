// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Snapshot-consistent, budgeted point queries over Paimon tables.
//!
//! The initial query capability returns BLOB descriptors. Additional projected
//! field types can be added behind this crate's service boundary.

mod error;
mod key;
mod lookup;
mod model;
mod policy;

pub use error::{LookupError, Result};
pub use lookup::{BlobLookupOptions, BlobLookupService, DescriptorCacheStats, TableCacheStats};
pub use model::{
    BatchGetRequest, BatchGetResponse, BlobDescriptorDto, DescriptorFormat, LookupKey,
    LookupResult, LookupScanStats, LookupStatus, TableRef,
};
pub use policy::{LookupStrategy, QueryBudget, TableLookupPolicy};
