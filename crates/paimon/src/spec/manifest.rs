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


use crate::spec::{DataFileMeta, RowType, SchemaManager};
use crate::spec::BinaryRow;
use crate::spec::manifest::FileKind::{ADD, DELETE};

enum FileKind {
    ADD,
    DELETE,
}

impl FileKind {
    pub fn byte_value(&self) -> u8 {
        match self {
            ADD => 0,
            DELETE => 1,
        }
    }
}

impl From<u8> for FileKind {
    fn from(value: u8) -> Self {
        match value {
            0 => ADD,
            1 => DELETE,
            _ => unimplemented!()
        }
    }
}

struct Identifier {
    partition: BinaryRow,
    bucket: i32,
    level: i32,
    file_name: String
}

impl Identifier {
    pub fn new(partition: BinaryRow, bucket: i32, level: i32, file_name: String) -> Self {
        Self { partition, bucket, level, file_name }
    }
}

pub trait FileEntry {
    fn partition(&self) -> BinaryRow;
    fn bucket(&self) -> i32;
    fn level(&self) -> i32;
    fn file_name(&self) -> String;

    fn identifier(&self) -> Identifier;

    fn min_key(&self) -> BinaryRow;
    fn max_key(&self) -> BinaryRow;

    // TODO Implement default methods
}

struct ManifestEntry {
    kind: FileKind,
    partition: BinaryRow,
    bucket: i32,
    total_buckets: i32,
    file: DataFileMeta
}
impl FileEntry for ManifestEntry {
    fn partition(&self) -> BinaryRow {
        self.partition
    }

    fn bucket(&self) -> i32 {
        self.bucket
    }

    fn level(&self) -> i32 {
        self.file.level
    }

    fn file_name(&self) -> String {
        self.file.file_name.clone()
    }

    fn identifier(&self) -> Identifier {
        Identifier::new(self.partition, self.bucket, self.file.level, self.file.file_name.clone())
    }

    fn min_key(&self) -> BinaryRow {
        self.file.min_key
    }

    fn max_key(&self) -> BinaryRow {
        self.file.max_key
    }
}

struct ManifestFileMeta {
    file_name: String,
    file_size: i64,
    num_added_files: i64,
    num_deleted_files: i64,
    // FIXME: add missing SimpleStats
    schema_id: i64,
}

//! This file includes several [ManifestEntry], representing the additional changes since last snapshot.
struct ManifestFile {
    row_type: RowType,
    suggested_file_size: u64,
    schema_manager: SchemaManager
}