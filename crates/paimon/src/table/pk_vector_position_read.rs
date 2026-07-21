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

//! Primary-key vector position read (Rust equivalent of Java
//! `PrimaryKeyVectorPositionReader`).
//!
//! Materializes the selected physical rows of one data file and appends
//! `_PKEY_VECTOR_POSITION` (+ optional `__paimon_search_score`) metadata columns.
//! This is the lowest layer of the PK-vector read kernel; the sibling
//! `pk_vector_indexed_split_read` and `pk_vector_orchestrator` modules build the
//! indexed-split contract and cross-bucket merge on top of it.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, Float32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema};
use futures::StreamExt;

use crate::deletion_vector::DeletionVector;
use crate::spec::{DataField, DataFileMeta, ROW_ID_FIELD_NAME};
use crate::table::data_file_reader::DataFileReader;
use crate::table::source::DataSplit;
use crate::table::ArrowRecordBatchStream;

pub(crate) const PKEY_VECTOR_POSITION_COLUMN: &str = "_PKEY_VECTOR_POSITION";
// Unified user-visible vector-search score column (matches the engine metadata
// column name used by Spark and the DataFusion table function).
pub(crate) const SEARCH_SCORE_COLUMN: &str = "__paimon_search_score";

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Reads selected physical rows of one data file, appending position (+ optional
/// score) metadata columns. Rust equivalent of Java
/// `PrimaryKeyVectorPositionReader`.
pub(crate) struct PkVectorPositionRead<'a> {
    reader: &'a DataFileReader,
}

impl<'a> PkVectorPositionRead<'a> {
    pub(crate) fn new(reader: &'a DataFileReader) -> Self {
        Self { reader }
    }

    pub(crate) fn read(
        &self,
        split: &DataSplit,
        file_meta: DataFileMeta,
        data_fields: Option<Vec<DataField>>,
        dv: Option<Arc<DeletionVector>>,
        positions: impl IntoIterator<Item = i64>,
        scores: Option<BTreeMap<i64, f32>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        // (1) normalize + validate positions
        let mut sorted: Vec<i64> = positions.into_iter().collect();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.is_empty() {
            return Err(data_invalid("Selected row positions must not be empty"));
        }
        if let Some(&first) = sorted.first() {
            if first < 0 {
                return Err(data_invalid(format!(
                    "Vector position must not be negative: {first}"
                )));
            }
        }
        if let Some(&last) = sorted.last() {
            if last >= file_meta.row_count {
                return Err(data_invalid(format!(
                    "Vector position {last} is outside data file row count {}",
                    file_meta.row_count
                )));
            }
        }

        // (2) scores contract on the input
        if let Some(scores) = scores.as_ref() {
            let key_ok =
                scores.len() == sorted.len() && scores.keys().copied().eq(sorted.iter().copied());
            if !key_ok {
                return Err(data_invalid(
                    "Scores keys must exactly match the selected row positions",
                ));
            }
        }

        // (3) reserved-column-name check against the requested output fields
        for field in self.reader.read_type() {
            if field.name() == PKEY_VECTOR_POSITION_COLUMN || field.name() == SEARCH_SCORE_COLUMN {
                return Err(data_invalid(format!(
                    "Reserved metadata column name conflicts with a table column: {}",
                    field.name()
                )));
            }
            if field.name() == ROW_ID_FIELD_NAME {
                return Err(data_invalid(
                    "PK vector position read does not support a requested _ROW_ID column; \
                     it is used internally for physical-position recovery",
                ));
            }
        }

        // (4) predicate guard: a residual row-filtering predicate would desync
        // positional row-id assignment, so reject it here.
        if self.reader.has_row_filtering_predicate() {
            return Err(data_invalid(
                "PK vector position read requires a predicate-free reader",
            ));
        }

        // Effective selection: the requested positions minus any the deletion
        // vector marks deleted, ascending. `read_single_file_stream_local` folds
        // the same DV into its row selection, so the reader returns exactly these
        // rows in this order; the cursor below maps each returned batch back onto
        // its slice of `effective` to recover file-LOCAL positions (no
        // `first_row_id`, no `_ROW_ID` round-trip).
        let effective: Vec<i64> = match dv.as_deref() {
            Some(dv) => sorted
                .iter()
                .copied()
                .filter(|&p| !dv.is_deleted(p as u64))
                .collect(),
            None => sorted.clone(),
        };

        let inner =
            self.reader
                .read_single_file_stream_local(split, file_meta, data_fields, dv, sorted)?;

        let want_score_col = scores.is_some();

        let stream = async_stream::try_stream! {
            futures::pin_mut!(inner);
            let mut cursor = 0usize;
            while let Some(batch) = inner.next().await {
                let batch = batch?;
                let n = batch.num_rows();
                let end = cursor + n;
                if end > effective.len() {
                    let overflow: crate::Result<()> = Err(data_invalid(format!(
                        "PK vector position read returned {end} rows but only {} positions were \
                         selected",
                        effective.len()
                    )));
                    overflow?;
                }
                let out = append_metadata_columns(
                    batch,
                    &effective[cursor..end],
                    want_score_col,
                    scores.as_ref(),
                )?;
                cursor = end;
                yield out;
            }
            if cursor != effective.len() {
                let mismatch: crate::Result<()> = Err(data_invalid(format!(
                    "PK vector position read returned {cursor} rows but {} positions were selected",
                    effective.len()
                )));
                mismatch?;
            }
        };
        Ok(Box::pin(stream))
    }
}

/// Append `_PKEY_VECTOR_POSITION` (and, when `want_score_col`, `__paimon_search_score`)
/// to `batch`. `positions` are the file-LOCAL physical positions of the batch's
/// rows, supplied by the caller's cursor into the effective (DV-filtered)
/// selection, so they align 1:1 with the batch rows in order. Scores are looked
/// up by position. The batch carries no `_ROW_ID` column, so every existing
/// column is retained.
fn append_metadata_columns(
    batch: RecordBatch,
    positions: &[i64],
    want_score_col: bool,
    scores: Option<&BTreeMap<i64, f32>>,
) -> crate::Result<RecordBatch> {
    debug_assert_eq!(
        batch.num_rows(),
        positions.len(),
        "position slice must align with batch rows"
    );
    let schema = batch.schema();

    let score_vals = if want_score_col {
        let mut sv = Vec::with_capacity(positions.len());
        for &position in positions {
            let score = scores
                .and_then(|m| m.get(&position).copied())
                .ok_or_else(|| {
                    data_invalid(format!(
                        "internal: no score for returned position {position}"
                    ))
                })?;
            sv.push(score);
        }
        Some(sv)
    } else {
        None
    };

    // Retain every existing column (no `_ROW_ID` to strip), then append metadata.
    let mut fields: Vec<ArrowField> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
    fields.push(ArrowField::new(
        PKEY_VECTOR_POSITION_COLUMN,
        ArrowDataType::Int64,
        false,
    ));
    columns.push(Arc::new(Int64Array::from(positions.to_vec())));
    if let Some(sv) = score_vals {
        fields.push(ArrowField::new(
            SEARCH_SCORE_COLUMN,
            ArrowDataType::Float32,
            false,
        ));
        columns.push(Arc::new(Float32Array::from(sv)));
    }

    let out_schema = Arc::new(ArrowSchema::new(Fields::from(fields)));
    RecordBatch::try_new(out_schema, columns).map_err(|e| crate::Error::UnexpectedError {
        message: format!("Failed to build position-read batch: {e}"),
        source: Some(Box::new(e)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::deletion_vector::DeletionVectorFactory;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BigIntType, DataFileMeta, DataType, Datum, IntType, PredicateBuilder, ROW_ID_FIELD_ID,
        ROW_ID_FIELD_NAME,
    };
    use crate::table::data_file_reader::DataFileReader;
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile};
    use arrow_array::{Array, Float32Array, Int32Array, Int64Array, RecordBatch};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use roaring::RoaringBitmap;
    use std::collections::BTreeMap;
    use std::io;
    use std::sync::Arc;

    struct MemOutputFile {
        data: Vec<u8>,
    }

    impl MemOutputFile {
        fn new() -> Self {
            Self { data: Vec::new() }
        }
    }

    impl OutputFile for MemOutputFile {
        fn write(&mut self, data: &[u8]) -> io::Result<()> {
            self.data.extend_from_slice(data);
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn pos(&self) -> u64 {
            self.data.len() as u64
        }
    }

    fn id_field() -> DataField {
        DataField::new(0, "id".to_string(), DataType::Int(IntType::new()))
    }

    /// A single `id: Int32` field is the reader's read-type in every test.
    fn id_fields() -> Vec<DataField> {
        vec![id_field()]
    }

    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        let schema = build_target_arrow_schema(&id_fields()).unwrap();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    /// `DataFileMeta` with a specified `first_row_id` (the read path requires it).
    fn data_file(
        file_name: &str,
        file_size: i64,
        row_count: i64,
        schema_id: i64,
        first_row_id: Option<i64>,
    ) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id,
            write_cols: None,
        }
    }

    fn write_mosaic_single_group(batch: &RecordBatch) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batch.schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
        writer.write_batch(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    /// Writes one row group per batch, so the reader yields one Arrow batch per
    /// input batch — used for the cross-batch alignment test.
    fn write_mosaic_multi_group(batches: &[RecordBatch]) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batches[0].schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: 1,
                ..Default::default()
            },
        )
        .unwrap();
        for batch in batches {
            writer.write_batch(batch).unwrap();
        }
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    async fn write_deletion_file(
        file_io: &crate::io::FileIO,
        path: &str,
        deleted_rows: &[u32],
    ) -> DeletionFile {
        const MAGIC_NUMBER: i32 = 1581511376;
        let mut bitmap = RoaringBitmap::new();
        for row in deleted_rows {
            bitmap.insert(*row);
        }
        let mut bitmap_bytes = Vec::new();
        bitmap.serialize_into(&mut bitmap_bytes).unwrap();

        let bitmap_length = 4 + bitmap_bytes.len() as i32;
        let mut blob = Vec::new();
        blob.extend_from_slice(&bitmap_length.to_be_bytes());
        blob.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
        blob.extend_from_slice(&bitmap_bytes);
        blob.extend_from_slice(&0i32.to_be_bytes());
        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from(blob))
            .await
            .unwrap();
        DeletionFile::new(
            path.to_string(),
            0,
            bitmap_length as i64,
            Some(deleted_rows.len() as i64),
        )
    }

    /// Build a `DataFileReader` over an in-memory mosaic file plus the matching
    /// `DataSplit`, pinning the data file's `first_row_id = Some(0)`.
    /// `read_type`/`predicates` override the reader's projection and filter;
    /// `deleted_rows`, when non-empty, writes a DV into the split.
    async fn build_reader_and_split(
        table_path: &str,
        data: &Bytes,
        row_count: i64,
        read_type: Vec<DataField>,
        predicates: Vec<crate::spec::Predicate>,
        deleted_rows: &[u32],
    ) -> (DataFileReader, DataSplit, Option<Arc<DeletionVector>>) {
        build_reader_and_split_with_first_row_id(
            table_path,
            data,
            row_count,
            read_type,
            predicates,
            deleted_rows,
            Some(0),
        )
        .await
    }

    /// As `build_reader_and_split`, but the caller controls the data file's
    /// `first_row_id`. Real Java primary-key tables never write `first_row_id`
    /// (row-tracking is forbidden for PK tables), so `None` is the shape the read
    /// path must handle by keying off file-local physical positions.
    #[allow(clippy::too_many_arguments)]
    async fn build_reader_and_split_with_first_row_id(
        table_path: &str,
        data: &Bytes,
        row_count: i64,
        read_type: Vec<DataField>,
        predicates: Vec<crate::spec::Predicate>,
        deleted_rows: &[u32],
        first_row_id: Option<i64>,
    ) -> (DataFileReader, DataSplit, Option<Arc<DeletionVector>>) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.mosaic";
        file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();

        let schema_id = 1;
        let mut split_builder = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                data.len() as i64,
                row_count,
                schema_id,
                first_row_id,
            )]);
        let mut dv = None;
        if !deleted_rows.is_empty() {
            let df =
                write_deletion_file(&file_io, &format!("{table_path}/index/dv-0"), deleted_rows)
                    .await;
            dv = Some(Arc::new(
                DeletionVectorFactory::read(&file_io, &df).await.unwrap(),
            ));
            split_builder = split_builder.with_data_deletion_files(vec![Some(df)]);
        }
        let split = split_builder.build().unwrap();

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            schema_id,
            id_fields(),
            read_type,
            predicates,
        );
        (reader, split, dv)
    }

    fn column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Arc<dyn Array>> {
        batch
            .schema()
            .index_of(name)
            .ok()
            .map(|idx| batch.column(idx))
    }

    fn collect_i32(batches: &[RecordBatch], name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                column_by_name(b, name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn collect_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                column_by_name(b, name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn collect_f32(batches: &[RecordBatch], name: &str) -> Vec<f32> {
        batches
            .iter()
            .flat_map(|b| {
                column_by_name(b, name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_reads_selected_positions_with_position_column() {
        // rows id=[10,11,12,13,14], first_row_id=0, select positions [0,2,4]
        // -> output ids [10,12,14], _PKEY_VECTOR_POSITION [0,2,4], ascending;
        // no _ROW_ID leak and no __paimon_search_score column.
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12, 13, 14]));
        let (reader, split, _dv) = build_reader_and_split(
            "memory:/pkvpr_basic",
            &data,
            5,
            id_fields(),
            Vec::new(),
            &[],
        )
        .await;

        let batches = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![0, 2, 4],
                None,
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_i32(&batches, "id"), vec![10, 12, 14]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 2, 4]
        );
        for batch in &batches {
            assert!(
                column_by_name(batch, ROW_ID_FIELD_NAME).is_none(),
                "_ROW_ID must not leak into output"
            );
            assert!(
                column_by_name(batch, SEARCH_SCORE_COLUMN).is_none(),
                "__paimon_search_score must be absent when no scores are supplied"
            );
        }
    }

    #[tokio::test]
    async fn test_score_alignment_non_contiguous() {
        // select [0,2,5] with scores {0:0.9, 2:0.5, 5:0.1}
        // -> __paimon_search_score aligned by returned position: [0.9,0.5,0.1].
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12, 13, 14, 15]));
        let (reader, split, _dv) = build_reader_and_split(
            "memory:/pkvpr_scores",
            &data,
            6,
            id_fields(),
            Vec::new(),
            &[],
        )
        .await;

        let scores = BTreeMap::from([(0, 0.9f32), (2, 0.5), (5, 0.1)]);
        let batches = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![0, 2, 5],
                Some(scores),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_i32(&batches, "id"), vec![10, 12, 15]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 2, 5]
        );
        assert_eq!(
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
            vec![0.9, 0.5, 0.1]
        );
    }

    #[tokio::test]
    async fn test_score_omitted_no_score_column() {
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12]));
        let (reader, split, _dv) = build_reader_and_split(
            "memory:/pkvpr_noscore",
            &data,
            3,
            id_fields(),
            Vec::new(),
            &[],
        )
        .await;

        let batches = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![0, 1],
                None,
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        for batch in &batches {
            assert!(column_by_name(batch, SEARCH_SCORE_COLUMN).is_none());
        }
    }

    #[tokio::test]
    async fn test_deletion_vector_skips_positions_and_keeps_alignment() {
        // select [0,1,2,3] scores {0:.4,1:.3,2:.2,3:.1}, DV deletes position 1.
        // -> returned positions [0,2,3], scores [.4,.2,.1]; position 1 absent.
        // scores STILL contains key 1 (input contract), though row 1 is deleted.
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12, 13]));
        let (reader, split, dv) =
            build_reader_and_split("memory:/pkvpr_dv", &data, 4, id_fields(), Vec::new(), &[1])
                .await;

        let scores = BTreeMap::from([(0, 0.4f32), (1, 0.3), (2, 0.2), (3, 0.1)]);
        let batches = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                dv,
                vec![0, 1, 2, 3],
                Some(scores),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_i32(&batches, "id"), vec![10, 12, 13]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 2, 3]
        );
        assert_eq!(
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
            vec![0.4, 0.2, 0.1]
        );
    }

    #[tokio::test]
    async fn test_multi_batch_alignment() {
        // Three row groups [10,11] [12,13] [14,15] -> reader yields >1 batch.
        // Select [1,2,4] spanning batch boundaries; assert position/score
        // alignment holds across batches (row_id_offset advances correctly).
        let data = write_mosaic_multi_group(&[
            id_batch(vec![10, 11]),
            id_batch(vec![12, 13]),
            id_batch(vec![14, 15]),
        ]);
        let (reader, split, _dv) = build_reader_and_split(
            "memory:/pkvpr_multibatch",
            &data,
            6,
            id_fields(),
            Vec::new(),
            &[],
        )
        .await;

        let scores = BTreeMap::from([(1, 0.9f32), (2, 0.5), (4, 0.1)]);
        let batches = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![1, 2, 4],
                Some(scores),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(
            batches.len() > 1,
            "expected multiple batches, got {}",
            batches.len()
        );
        assert_eq!(collect_i32(&batches, "id"), vec![11, 12, 14]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![1, 2, 4]
        );
        assert_eq!(
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
            vec![0.9, 0.5, 0.1]
        );
    }

    #[tokio::test]
    async fn pk_position_read_aligns_positions_across_dv_and_batches_without_first_row_id() {
        // The Java primary-key shape: the data file carries NO first_row_id, rows
        // span multiple row groups (batches), a DV deletes a NON-candidate
        // position, and the candidates sit at non-contiguous local positions. The
        // read must recover each row's FILE-LOCAL position (not a global row id),
        // skip only the deleted row, and keep id/position/score aligned best-first.
        //
        // Three row groups [10,11] [12,13] [14,15] -> reader yields >1 batch.
        // Candidates [1,3,4,5] with scores keyed by local position; DV deletes
        // position 2 (a NON-candidate) -> it must not perturb the surviving rows.
        // Expected surviving rows: ids [11,13,14,15], positions [1,3,4,5].
        let data = write_mosaic_multi_group(&[
            id_batch(vec![10, 11]),
            id_batch(vec![12, 13]),
            id_batch(vec![14, 15]),
        ]);
        let (reader, split, dv) = build_reader_and_split_with_first_row_id(
            "memory:/pkvpr_local_dv_multibatch",
            &data,
            6,
            id_fields(),
            Vec::new(),
            &[2],
            None,
        )
        .await;

        let scores = BTreeMap::from([(1, 0.9f32), (3, 0.5), (4, 0.3), (5, 0.1)]);
        let batches = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                dv,
                vec![1, 3, 4, 5],
                Some(scores),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(
            batches.len() > 1,
            "expected multiple batches, got {}",
            batches.len()
        );
        assert_eq!(collect_i32(&batches, "id"), vec![11, 13, 14, 15]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![1, 3, 4, 5]
        );
        assert_eq!(
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
            vec![0.9, 0.5, 0.3, 0.1]
        );
    }

    #[tokio::test]
    async fn test_empty_positions_is_error() {
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12]));
        let (reader, split, _dv) = build_reader_and_split(
            "memory:/pkvpr_empty",
            &data,
            3,
            id_fields(),
            Vec::new(),
            &[],
        )
        .await;

        let err = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                Vec::<i64>::new(),
                None,
            )
            .err()
            .expect("empty positions must be an error");
        assert!(
            format!("{err:?}").contains("must not be empty"),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_position_out_of_range_is_error() {
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12]));
        let (reader, split, _dv) =
            build_reader_and_split("memory:/pkvpr_oor", &data, 3, id_fields(), Vec::new(), &[])
                .await;

        let err = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![5],
                None,
            )
            .err()
            .expect("out-of-range position must be an error");
        let msg = format!("{err:?}");
        assert!(msg.contains('5') && msg.contains('3'), "got: {msg}");
    }

    #[tokio::test]
    async fn test_negative_position_is_error() {
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12]));
        let (reader, split, _dv) =
            build_reader_and_split("memory:/pkvpr_neg", &data, 3, id_fields(), Vec::new(), &[])
                .await;

        let err = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![-1],
                None,
            )
            .err()
            .expect("negative position must be an error");
        assert!(
            format!("{err:?}").contains("must not be negative"),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_scores_key_mismatch_is_error() {
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12]));
        let (reader, split, _dv) = build_reader_and_split(
            "memory:/pkvpr_scoremismatch",
            &data,
            3,
            id_fields(),
            Vec::new(),
            &[],
        )
        .await;

        // select [0,1] but scores only has key 0 -> mismatch.
        let scores = BTreeMap::from([(0, 0.5f32)]);
        let err = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![0, 1],
                Some(scores),
            )
            .err()
            .expect("score key mismatch must be an error");
        assert!(format!("{err:?}").contains("Scores keys"), "got: {err:?}");
    }

    #[tokio::test]
    async fn test_predicate_reader_is_rejected() {
        // A row-filtering predicate on the reader must be rejected.
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema_manager = SchemaManager::new(file_io.clone(), "memory:/pkvpr_pred".to_string());
        let predicate = PredicateBuilder::new(&id_fields())
            .equal("id", Datum::Int(10))
            .unwrap();
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            1,
            id_fields(),
            id_fields(),
            vec![predicate],
        );
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/pkvpr_pred/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![data_file("part-0.mosaic", 1, 5, 1, Some(0))])
            .build()
            .unwrap();

        let err = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![0],
                None,
            )
            .err()
            .expect("predicate reader must be rejected");
        assert!(
            format!("{err:?}").contains("predicate-free"),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_reserved_column_name_conflict_is_error() {
        // A reader whose read_type contains "_PKEY_VECTOR_POSITION" must be rejected.
        let reserved = DataField::new(
            0,
            PKEY_VECTOR_POSITION_COLUMN.to_string(),
            DataType::Int(IntType::new()),
        );
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema_manager =
            SchemaManager::new(file_io.clone(), "memory:/pkvpr_reserved".to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            1,
            id_fields(),
            vec![reserved],
            Vec::new(),
        );
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/pkvpr_reserved/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![data_file("part-0.mosaic", 1, 5, 1, Some(0))])
            .build()
            .unwrap();

        let err = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![0],
                None,
            )
            .err()
            .expect("reserved column name conflict must be an error");
        assert!(
            format!("{err:?}").contains("Reserved metadata column name"),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_requested_row_id_column_is_rejected() {
        // A reader whose read_type requests _ROW_ID must be rejected loudly rather
        // than have the column silently stripped from the output.
        let requested_row_id = DataField::new(
            ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::new()),
        );
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema_manager =
            SchemaManager::new(file_io.clone(), "memory:/pkvpr_reqrowid".to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            1,
            id_fields(),
            vec![id_field(), requested_row_id],
            Vec::new(),
        );
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/pkvpr_reqrowid/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![data_file("part-0.mosaic", 1, 5, 1, Some(0))])
            .build()
            .unwrap();

        let err = PkVectorPositionRead::new(&reader)
            .read(
                &split,
                split.data_files()[0].clone(),
                None,
                None,
                vec![0],
                None,
            )
            .err()
            .expect("requested _ROW_ID must be an error");
        assert!(format!("{err:?}").contains("_ROW_ID"), "got: {err:?}");
    }
}
