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

//! Adapt a captured DE shard to the shared native index writer.
use super::extraction::build_read_splits_for_shard;
use super::planning::SortedGlobalIndexShard;
use super::{ScalarIndexOptions, SortedGlobalIndexBuildBuilder};
use crate::spec::{DataField, GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME};
use crate::table::index_build::{scalar, IndexFileWriter};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::RowRange;
use crate::Result;

impl SortedGlobalIndexBuildBuilder<'_> {
    pub(super) async fn build_index_file(
        &self,
        shard: &SortedGlobalIndexShard,
        fields: &[&DataField],
        options: &ScalarIndexOptions,
    ) -> Result<IndexFileMeta> {
        let kind = options.index_type();
        let splits = build_read_splits_for_shard(shard)?;
        let mut read = self.table.new_read_builder();
        let mut projection = fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        projection.push(ROW_ID_FIELD_NAME);
        read.with_projection(&projection)?;
        let stream = read.new_read()?.to_arrow(&splits)?;
        let range = RowRange::new(shard.row_range_start, shard.row_range_end);
        let meta = GlobalIndexMeta {
            row_range_start: range.from(),
            row_range_end: range.to(),
            index_field_id: fields[0].id(),
            extra_field_ids: (fields.len() > 1)
                .then(|| fields.iter().skip(1).map(|f| f.id()).collect()),
            source_meta: Some(
                crate::spec::DataEvolutionIndexSourceMeta::new(shard.snapshot_id)?.serialize(),
            ),
            index_meta: None,
        };
        IndexFileWriter::new(
            self.table,
            kind,
            crate::table::global_index_schema::index_file_name(kind, self.table.schema().id()),
            IndexFileLocation::Global {
                table_path: self.table.location().trim_end_matches('/'),
            },
            "",
            meta,
        )
        .await?
        .write(self.table, |output| async move {
            scalar::write_payload(stream, output, fields, range, options)
                .await
                .map(Some)
        })
        .await
        .map(|file| file.expect("a scalar shard contains source rows"))
    }
}
