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

//! Streaming exact vector search over one data file's vector column. Mirrors
//! Java `org.apache.paimon.index.pkvector.PkVectorDataFileReader` +
//! `PkVectorExactSearcher`.
//!
//! The factory projects the single vector column and, per uncovered file,
//! streams the column one Arrow batch at a time — feeding each row into per-query
//! bounded Top-K heaps and dropping the batch — so peak memory is one batch plus
//! the heaps rather than the whole column. Deletion vectors and residual filters
//! are deliberately NOT applied by the stream itself: physical position must stay
//! in lockstep with the segment ordinal, so exclusion is folded in via the
//! caller-supplied `is_excluded(position)` predicate. A NULL row is not scored but
//! still advances the physical position.

use std::collections::BinaryHeap;

use arrow_array::{Array, FixedSizeListArray, Float32Array, ListArray};
use futures::TryStreamExt;

use crate::spec::{DataField, DataType};
use crate::table::data_file_reader::DataFileReader;
use crate::table::source::DataSplit;
use crate::vindex::pkvector::bucket::BucketActiveFile;
use crate::vindex::pkvector::exact::{drain_best_first, push_bounded, validate_query, WorstFirst};
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::pkvector::result::PkVectorSearchResult;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Runs a streaming exact [`PkVectorSearchResult`] search over one data file's
/// vector column.
///
/// `reader` is configured (via [`DataFileReader::with_read_type`]) to project
/// only the vector column, so each read returns a single-column batch. Mirrors
/// Java `PkVectorDataFileReader` (as a factory owning the projected reader).
pub(crate) struct DataFilePkVectorReaderFactory {
    reader: DataFileReader,
    data_split: DataSplit,
    vector_field: DataField,
    dimension: usize,
}

impl DataFilePkVectorReaderFactory {
    /// Configure `reader` to project the vector column only and capture the
    /// vector dimension from the schema field. The field must be a fixed-length
    /// `Vector` type; anything else is rejected as invalid.
    pub(crate) fn new(
        reader: DataFileReader,
        data_split: DataSplit,
        vector_field: DataField,
    ) -> crate::Result<Self> {
        let dimension = match vector_field.data_type() {
            DataType::Vector(vector_type) => vector_type.length() as usize,
            other => {
                return Err(data_invalid(format!(
                    "PK-vector reader requires a fixed-length Vector field, got {other:?}"
                )));
            }
        };
        let reader = reader.with_read_type(vec![vector_field.clone()]);
        Ok(Self {
            reader,
            data_split,
            vector_field,
            dimension,
        })
    }

    /// Stream the vector column of `file` one Arrow batch at a time and return one
    /// bounded, BEST_FIRST Top-K list per query (outer index aligned to `queries`).
    /// `file` must name a data file present in this factory's split.
    ///
    /// All queries are validated (dimension + finite) BEFORE the file stream is
    /// opened. Each surviving physical position (not NULL, not `is_excluded`) is
    /// scored against every query into that query's bounded heap; a NULL row is
    /// skipped but still advances the position so the position stays in lockstep
    /// with `is_excluded`. The drained row count is checked against the file's
    /// `DataFileMeta.row_count` (both truncation and overrun fail loud).
    pub(crate) async fn search_file(
        &self,
        file: &BucketActiveFile,
        queries: &[&[f32]],
        metric: VectorSearchMetric,
        exact_limit: usize,
        is_excluded: &(dyn Fn(i64) -> bool + Sync),
    ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
        if exact_limit == 0 {
            return Err(data_invalid("vector search limit must be positive"));
        }
        // Validate every query before opening the stream (validate-before-POLL);
        // a malformed query fails loud before any file I/O.
        for query in queries {
            validate_query(query, self.dimension)?;
        }

        let file_meta = self
            .data_split
            .data_files()
            .iter()
            .find(|meta| meta.file_name == file.file_name)
            .cloned()
            .ok_or_else(|| {
                data_invalid(format!(
                    "data file '{}' not found in split for PK-vector read",
                    file.file_name
                ))
            })?;
        let row_count = file_meta.row_count;

        let data_fields = self.reader.derive_data_fields(&file_meta).await?;
        let mut stream = self.reader.read_single_file_stream(
            &self.data_split,
            file_meta,
            data_fields,
            None,
            None,
        )?;

        let mut heaps: Vec<BinaryHeap<WorstFirst>> = (0..queries.len())
            .map(|_| BinaryHeap::with_capacity(exact_limit + 1))
            .collect();
        // One reused buffer per batch; a NULL row leaves it untouched (and is not
        // scored). `position` is the monotonic physical row counter across batches.
        let mut batch_vectors: Vec<Option<Vec<f32>>> = Vec::new();
        let mut position: i64 = 0;
        while let Some(batch) = stream.try_next().await? {
            batch_vectors.clear();
            append_batch_vectors(
                &batch,
                self.vector_field.name(),
                self.dimension,
                &mut batch_vectors,
            )?;
            for entry in &batch_vectors {
                let pos = position;
                position += 1;
                if pos >= row_count {
                    return Err(data_invalid(
                        "data file produced more rows than DataFileMeta.row_count",
                    ));
                }
                let Some(vector) = entry else {
                    continue; // NULL row: not scored, position already advanced.
                };
                if is_excluded(pos) {
                    continue;
                }
                for (query, heap) in queries.iter().zip(heaps.iter_mut()) {
                    let candidate = PkVectorSearchResult {
                        data_file_name: file.file_name.clone(),
                        row_position: pos,
                        distance: metric.compute_distance(query, vector),
                    };
                    push_bounded(heap, candidate, exact_limit);
                }
            }
        }

        if position > row_count {
            return Err(data_invalid(
                "data file produced more rows than DataFileMeta.row_count",
            ));
        }
        if position < row_count {
            return Err(data_invalid(
                "data file ended before DataFileMeta.row_count",
            ));
        }

        Ok(heaps.into_iter().map(drain_best_first).collect())
    }
}

/// Extract one batch's vector column into `out`, one entry per row (NULL row =
/// `None`). The column must be a `FixedSizeList`/`List` of `Float32`; every
/// non-null row's child slice must have exactly `dimension` elements. Mirrors
/// the layout handling in `vector_search_builder`.
pub(crate) fn append_batch_vectors(
    batch: &arrow_array::RecordBatch,
    field_name: &str,
    dimension: usize,
    out: &mut Vec<Option<Vec<f32>>>,
) -> crate::Result<()> {
    let index = batch
        .schema()
        .index_of(field_name)
        .map_err(|e| data_invalid(format!("vector column '{field_name}' not found: {e}")))?;
    let column = batch.column(index);

    enum VectorLayout<'a> {
        List(&'a ListArray),
        Fixed(&'a FixedSizeListArray),
    }
    let layout = if let Some(a) = column.as_any().downcast_ref::<ListArray>() {
        VectorLayout::List(a)
    } else if let Some(a) = column.as_any().downcast_ref::<FixedSizeListArray>() {
        VectorLayout::Fixed(a)
    } else {
        return Err(data_invalid(
            "PK-vector read requires Arrow List<Float32> or FixedSizeList<Float32>",
        ));
    };

    let values = match layout {
        VectorLayout::List(a) => a.values(),
        VectorLayout::Fixed(a) => a.values(),
    }
    .as_any()
    .downcast_ref::<Float32Array>()
    .ok_or_else(|| data_invalid("PK-vector read requires Float32 vector elements"))?;

    for row in 0..batch.num_rows() {
        let is_null = match layout {
            VectorLayout::List(a) => a.is_null(row),
            VectorLayout::Fixed(a) => a.is_null(row),
        };
        if is_null {
            out.push(None);
            continue;
        }
        let (start, end) = match layout {
            VectorLayout::List(a) => {
                let offsets = a.value_offsets();
                (offsets[row] as usize, offsets[row + 1] as usize)
            }
            VectorLayout::Fixed(a) => {
                let len = a.value_length() as usize;
                (row * len, (row + 1) * len)
            }
        };
        if end - start != dimension {
            return Err(data_invalid(format!(
                "vector row has {} elements, expected dimension {dimension}",
                end - start
            )));
        }
        let mut vector = Vec::with_capacity(dimension);
        for i in start..end {
            if values.is_null(i) {
                return Err(data_invalid(format!(
                    "vector row {row} has a null element at index {}",
                    i - start
                )));
            }
            vector.push(values.value(i));
        }
        out.push(Some(vector));
    }
    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::{FormatFileWriter, ParquetFormatWriter};
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, FloatType, VectorType};
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::DataSplitBuilder;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
    use std::sync::Arc;

    fn vector_field() -> DataField {
        let vector_type = VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap();
        DataField::new(0, "embedding".to_string(), DataType::Vector(vector_type))
    }

    fn data_file(file_name: &str, file_size: i64, row_count: i64, schema_id: i64) -> DataFileMeta {
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
            first_row_id: None,
            write_cols: None,
        }
    }

    /// A present (non-null) vector row whose child slice contains a NULL
    /// element must fail loud rather than silently defaulting the element to
    /// `0.0` and corrupting the distance.
    #[test]
    fn append_batch_vectors_fails_loud_on_null_element() {
        let field = vector_field();
        let read_fields = vec![field.clone()];
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        // Row is present, but element index 1 in its child slice is NULL: [1.0, null].
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        builder.values().append_value(1.0);
        builder.values().append_null();
        builder.append(true);
        let vec_array = builder.finish();
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(vec_array)]).unwrap();

        let mut out: Vec<Option<Vec<f32>>> = Vec::new();
        let err = append_batch_vectors(&batch, field.name(), 2, &mut out)
            .expect_err("null child element must fail loud");
        let msg = err.to_string();
        assert!(
            msg.contains("null") && msg.contains("element"),
            "got: {msg}"
        );
    }

    /// Build a FixedSizeList<Float32, 2> vector column from `rows` (`None` = NULL
    /// row), write it as one parquet data file across `batches` write calls, and
    /// return a factory over its split plus the file name. `stated_row_count` is
    /// what the `DataFileMeta` claims (usually the true row count, but a test can
    /// pass a wrong value to exercise the row-count guard).
    async fn build_factory(
        rows: &[Option<Vec<f32>>],
        stated_row_count: i64,
        table_path: &str,
    ) -> (DataFilePkVectorReaderFactory, String) {
        let field = vector_field();
        let read_fields = vec![field.clone()];
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        for row in rows {
            match row {
                Some(v) => {
                    builder.values().append_value(v[0]);
                    builder.values().append_value(v[1]);
                    builder.append(true);
                }
                None => {
                    builder.values().append_value(0.0);
                    builder.values().append_value(0.0);
                    builder.append(false);
                }
            }
        }
        let vec_array = builder.finish();
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)]).unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.parquet";
        let file_path = format!("{bucket_path}/{file_name}");
        let output = file_io.new_output(&file_path).unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(
                &output,
                arrow_schema.clone(),
                "zstd",
                1,
                None,
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap().file_size;

        let table_schema_id = 1;
        let data_split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(
                file_name,
                file_size as i64,
                stated_row_count,
                table_schema_id,
            )])
            .build()
            .unwrap();

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            table_schema_id,
            read_fields.clone(),
            read_fields.clone(),
            Vec::new(),
        );
        let factory = DataFilePkVectorReaderFactory::new(reader, data_split, field).unwrap();
        (factory, file_name.to_string())
    }

    /// The streaming per-file search must produce candidates byte-identical to
    /// the reference `exact_search` over an in-memory `ArrayReader` of the same
    /// data, including a NULL row and a residual/DV exclusion.
    #[tokio::test]
    async fn search_file_matches_exact_search_reference() {
        use crate::vindex::pkvector::exact::exact_search;
        use crate::vindex::pkvector::reader::test_support::ArrayReader;

        let rows = vec![
            Some(vec![3.0, 0.0]),
            None,
            Some(vec![1.0, 0.0]),
            Some(vec![2.0, 0.0]),
        ];
        let (factory, file_name) =
            build_factory(&rows, rows.len() as i64, "memory:/pkvdfr_equiv").await;
        let active = BucketActiveFile {
            file_name: file_name.clone(),
            row_count: rows.len() as i64,
        };
        // Exclude physical position 2 (residual/DV fold): mirrors the closure the
        // bucket search passes in.
        let is_excluded = |pos: i64| pos == 2;
        let query = [0.0f32, 0.0];

        let streamed = factory
            .search_file(&active, &[&query], VectorSearchMetric::L2, 2, &is_excluded)
            .await
            .unwrap();

        let mut ref_reader = ArrayReader::new(2, rows.clone());
        let reference = exact_search(
            &file_name,
            &mut ref_reader,
            &query,
            VectorSearchMetric::L2,
            2,
            &is_excluded,
        )
        .unwrap();

        assert_eq!(streamed.len(), 1, "one query in, one result list out");
        assert_eq!(streamed[0], reference);
    }

    /// Same streaming-vs-reference equivalence, but with more scorable rows than
    /// `exact_limit` so the bounded heap's eviction branch is exercised on both
    /// paths (the shared `push_bounded` must evict identically).
    #[tokio::test]
    async fn search_file_matches_exact_search_reference_with_eviction() {
        use crate::vindex::pkvector::exact::exact_search;
        use crate::vindex::pkvector::reader::test_support::ArrayReader;

        // Five scorable rows, no NULL/exclusion; keep only the 2 closest to [0,0].
        let rows = vec![
            Some(vec![4.0, 0.0]),
            Some(vec![1.0, 0.0]),
            Some(vec![3.0, 0.0]),
            Some(vec![2.0, 0.0]),
            Some(vec![5.0, 0.0]),
        ];
        let (factory, file_name) =
            build_factory(&rows, rows.len() as i64, "memory:/pkvdfr_evict").await;
        let active = BucketActiveFile {
            file_name: file_name.clone(),
            row_count: rows.len() as i64,
        };
        let query = [0.0f32, 0.0];

        let streamed = factory
            .search_file(&active, &[&query], VectorSearchMetric::L2, 2, &|_| false)
            .await
            .unwrap();

        let mut ref_reader = ArrayReader::new(2, rows.clone());
        let reference = exact_search(
            &file_name,
            &mut ref_reader,
            &query,
            VectorSearchMetric::L2,
            2,
            &|_| false,
        )
        .unwrap();

        assert_eq!(streamed[0], reference);
        // The two closest are positions 1 ([1,0]) then 3 ([2,0]), best-first.
        assert_eq!(streamed[0].len(), 2, "bounded to exact_limit");
        assert_eq!(streamed[0][0].row_position, 1);
        assert_eq!(streamed[0][1].row_position, 3);
    }

    /// A multi-query `search_file` returns independent per-query Top-K lists: the
    /// slot for a query in a batch is identical to that query searched alone (no
    /// cross-query bleed), and a shared `is_excluded` applies to every query.
    #[tokio::test]
    async fn search_file_multi_query_returns_independent_per_query_top_k() {
        let rows = vec![
            Some(vec![0.0, 0.0]),
            Some(vec![1.0, 0.0]),
            Some(vec![2.0, 0.0]),
            Some(vec![3.0, 0.0]),
        ];
        let (factory, file_name) =
            build_factory(&rows, rows.len() as i64, "memory:/pkvdfr_multiquery").await;
        let active = BucketActiveFile {
            file_name,
            row_count: rows.len() as i64,
        };
        // Exclude physical position 1 for all queries (shared predicate).
        let is_excluded = |pos: i64| pos == 1;
        let q0 = [0.0f32, 0.0]; // nearest is pos 0
        let q1 = [3.0f32, 0.0]; // nearest is pos 3

        let batch = factory
            .search_file(
                &active,
                &[&q0, &q1],
                VectorSearchMetric::L2,
                2,
                &is_excluded,
            )
            .await
            .unwrap();
        assert_eq!(batch.len(), 2, "one result list per query");

        // Each query searched alone must equal its slot in the batch.
        let only_q0 = factory
            .search_file(&active, &[&q0], VectorSearchMetric::L2, 2, &is_excluded)
            .await
            .unwrap();
        let only_q1 = factory
            .search_file(&active, &[&q1], VectorSearchMetric::L2, 2, &is_excluded)
            .await
            .unwrap();
        assert_eq!(batch[0], only_q0[0]);
        assert_eq!(batch[1], only_q1[0]);

        // Sanity: distinct nearest neighbours, and the excluded position is absent.
        assert_eq!(batch[0][0].row_position, 0);
        assert_eq!(batch[1][0].row_position, 3);
        assert!(batch
            .iter()
            .all(|list| list.iter().all(|r| r.row_position != 1)));
    }

    /// A `DataFileMeta.row_count` larger than the file's real row count means the
    /// stream ends early; the search must fail loud rather than return a short
    /// result.
    #[tokio::test]
    async fn search_file_fails_loud_on_row_count_truncation() {
        let rows = vec![Some(vec![1.0, 0.0]), Some(vec![2.0, 0.0])];
        // Claim 3 rows but only write 2.
        let (factory, file_name) = build_factory(&rows, 3, "memory:/pkvdfr_trunc").await;
        let active = BucketActiveFile {
            file_name,
            row_count: 3,
        };
        let query = [0.0f32, 0.0];
        let err = factory
            .search_file(&active, &[&query], VectorSearchMetric::L2, 2, &|_| false)
            .await
            .expect_err("row-count truncation must fail loud");
        assert!(err.to_string().contains("ended before"), "got: {err}");
    }

    /// A malformed query (wrong dimension / non-finite element) fails loud, and a
    /// file name absent from the split is rejected as invalid.
    #[tokio::test]
    async fn search_file_validates_query_and_rejects_absent_file() {
        let rows = vec![Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])];
        let (factory, file_name) =
            build_factory(&rows, rows.len() as i64, "memory:/pkvdfr_validate").await;
        let present = BucketActiveFile {
            file_name: file_name.clone(),
            row_count: rows.len() as i64,
        };

        // Wrong dimension.
        let bad_dim = [1.0f32];
        let err = factory
            .search_file(&present, &[&bad_dim], VectorSearchMetric::L2, 2, &|_| false)
            .await
            .expect_err("dimension mismatch must fail loud");
        assert!(err.to_string().contains("dimension"), "got: {err}");

        // Non-finite element.
        let bad_finite = [f32::NAN, 0.0];
        let err = factory
            .search_file(&present, &[&bad_finite], VectorSearchMetric::L2, 2, &|_| {
                false
            })
            .await
            .expect_err("non-finite query must fail loud");
        assert!(err.to_string().contains("finite"), "got: {err}");

        // Absent file.
        let missing = BucketActiveFile {
            file_name: "absent.parquet".to_string(),
            row_count: 3,
        };
        let query = [0.0f32, 0.0];
        let err = factory
            .search_file(&missing, &[&query], VectorSearchMetric::L2, 2, &|_| false)
            .await
            .expect_err("absent file must be rejected");
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }
}
