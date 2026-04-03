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

//! Table API for Apache Paimon

pub(crate) mod bin_pack;
mod read_builder;
pub(crate) mod schema_manager;
mod snapshot_manager;
mod source;
mod table_scan;
mod tag_manager;

use crate::Result;
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
pub use read_builder::{ReadBuilder, TableRead};
pub use schema_manager::SchemaManager;
pub use snapshot_manager::SnapshotManager;
pub use source::{DataSplit, DataSplitBuilder, DeletionFile, PartitionBucket, Plan};
pub use table_scan::TableScan;
pub use tag_manager::TagManager;

use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::spec::TableSchema;
use std::collections::HashMap;

/// Table represents a table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    identifier: Identifier,
    location: String,
    schema: TableSchema,
    schema_manager: SchemaManager,
}

#[allow(dead_code)]
impl Table {
    /// Create a new table.
    pub fn new(
        file_io: FileIO,
        identifier: Identifier,
        location: String,
        schema: TableSchema,
    ) -> Self {
        let schema_manager = SchemaManager::new(file_io.clone(), location.clone());
        Self {
            file_io,
            identifier,
            location,
            schema,
            schema_manager,
        }
    }

    /// Get the table's identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Get the table's location.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Get the table's schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Get the FileIO instance for this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Get the SchemaManager for this table.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    /// Create a read builder for scan/read.
    ///
    /// Reference: [pypaimon FileStoreTable.new_read_builder](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/table/file_store_table.py).
    pub fn new_read_builder(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }

    /// Create a copy of this table with extra options merged into the schema.
    pub fn copy_with_options(&self, extra: HashMap<String, String>) -> Self {
        Self {
            file_io: self.file_io.clone(),
            identifier: self.identifier.clone(),
            location: self.location.clone(),
            schema: self.schema.copy_with_options(extra),
            schema_manager: self.schema_manager.clone(),
        }
    }
}

/// A stream of arrow [`RecordBatch`]es.
pub type ArrowRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;
