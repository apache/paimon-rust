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

//! Exact sequential vector reader over one data file's vector column. Mirrors
//! Java `org.apache.paimon.index.pkvector.PkVectorDataFileReader`.
//!
//! The factory projects the single vector column, reads the whole file in
//! physical order, and preloads it into memory as `Vec<Option<Vec<f32>>>`
//! (a NULL row is `None`). Deletion vectors are deliberately NOT applied here:
//! physical position must stay in lockstep with the segment ordinal so the
//! bucket search can address rows by position. The returned reader then serves
//! vectors from memory one physical row at a time.

use arrow_array::{Array, FixedSizeListArray, Float32Array, ListArray};
use futures::TryStreamExt;

use crate::spec::{DataField, DataType};
use crate::table::data_file_reader::DataFileReader;
use crate::table::source::DataSplit;
use crate::vindex::pkvector::bucket::BucketActiveFile;
use crate::vindex::pkvector::reader::PkVectorReader;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Builds an exact [`PkVectorReader`] over one data file's vector column.
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

    /// Preload the whole vector column of `file` into memory and return a
    /// sequential reader over it. `file` must name a data file present in this
    /// factory's split. The drained row count is checked against the file's
    /// `DataFileMeta.row_count`.
    pub(crate) async fn create(
        &self,
        file: &BucketActiveFile,
    ) -> crate::Result<Box<dyn PkVectorReader>> {
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

        let mut vectors: Vec<Option<Vec<f32>>> = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            append_batch_vectors(
                &batch,
                self.vector_field.name(),
                self.dimension,
                &mut vectors,
            )?;
        }

        let drained = vectors.len() as i64;
        if drained > row_count {
            return Err(data_invalid(
                "data file produced more rows than DataFileMeta.row_count",
            ));
        }
        if drained < row_count {
            return Err(data_invalid(
                "data file ended before DataFileMeta.row_count",
            ));
        }

        Ok(Box::new(DataFilePkVectorReader {
            dimension: self.dimension,
            row_count,
            vectors,
            position: 0,
        }))
    }
}

/// Extract one batch's vector column into `out`, one entry per row (NULL row =
/// `None`). The column must be a `FixedSizeList`/`List` of `Float32`; every
/// non-null row's child slice must have exactly `dimension` elements. Mirrors
/// the layout handling in `vector_search_builder`.
fn append_batch_vectors(
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
            vector.push(values.value(i));
        }
        out.push(Some(vector));
    }
    Ok(())
}

/// In-memory sequential reader over one file's preloaded vector column. Each
/// [`read_next_vector`](PkVectorReader::read_next_vector) advances exactly one
/// physical row; a NULL row returns `false` but still advances the position.
struct DataFilePkVectorReader {
    dimension: usize,
    row_count: i64,
    /// Preloaded whole-file column in physical order; `None` = NULL row.
    vectors: Vec<Option<Vec<f32>>>,
    position: usize,
}

impl PkVectorReader for DataFilePkVectorReader {
    fn dimension(&self) -> usize {
        self.dimension
    }

    fn row_count(&self) -> i64 {
        self.row_count
    }

    fn read_next_vector(&mut self, reuse: &mut [f32]) -> crate::Result<bool> {
        if reuse.len() != self.dimension {
            return Err(data_invalid(format!(
                "reuse buffer length {} does not match vector dimension {}",
                reuse.len(),
                self.dimension
            )));
        }
        if self.position as i64 >= self.row_count {
            return Err(data_invalid("read past row count"));
        }
        let entry = &self.vectors[self.position];
        self.position += 1;
        match entry {
            Some(vector) => {
                reuse.copy_from_slice(vector);
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_row_consumes_ordinal() {
        let mut r = DataFilePkVectorReader {
            dimension: 2,
            row_count: 3,
            vectors: vec![Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])],
            position: 0,
        };
        let mut buf = [0.0f32; 2];
        assert!(r.read_next_vector(&mut buf).unwrap());
        assert_eq!(buf, [1.0, 2.0]);
        assert!(!r.read_next_vector(&mut buf).unwrap()); // null: false, ordinal advanced
        assert!(r.read_next_vector(&mut buf).unwrap());
        assert_eq!(buf, [3.0, 4.0]);
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.dimension(), 2);
    }

    #[test]
    fn read_past_row_count_errors() {
        let mut r = DataFilePkVectorReader {
            dimension: 1,
            row_count: 1,
            vectors: vec![Some(vec![1.0])],
            position: 0,
        };
        let mut buf = [0.0f32; 1];
        assert!(r.read_next_vector(&mut buf).unwrap());
        assert!(r.read_next_vector(&mut buf).is_err()); // past row_count
    }

    #[test]
    fn reuse_len_mismatch_errors() {
        let mut r = DataFilePkVectorReader {
            dimension: 2,
            row_count: 1,
            vectors: vec![Some(vec![1.0, 2.0])],
            position: 0,
        };
        let mut buf = [0.0f32; 1];
        assert!(r.read_next_vector(&mut buf).is_err());
    }
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

    /// Write a FixedSizeList<Float32, 2> vector column
    /// (`[1,2]`, NULL, `[3,4]`) as a parquet data file, build the factory over
    /// its split, preload via `create`, and assert the whole-file sequential
    /// read plus the "file not in split" error.
    #[tokio::test]
    async fn create_preloads_and_reads_whole_file() {
        let field = vector_field();
        let read_fields = vec![field.clone()];
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        builder.values().append_value(1.0);
        builder.values().append_value(2.0);
        builder.append(true);
        builder.values().append_value(0.0);
        builder.values().append_value(0.0);
        builder.append(false); // NULL vector row
        builder.values().append_value(3.0);
        builder.values().append_value(4.0);
        builder.append(true);
        let vec_array = builder.finish();
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)]).unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/pk_vector_data_file_reader";
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
                3,
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

        let factory =
            DataFilePkVectorReaderFactory::new(reader, data_split, field.clone()).unwrap();

        let present = BucketActiveFile {
            file_name: file_name.to_string(),
            row_count: 3,
        };
        let mut pk_reader = factory.create(&present).await.unwrap();
        assert_eq!(pk_reader.dimension(), 2);
        assert_eq!(pk_reader.row_count(), 3);

        let mut buf = [0.0f32; 2];
        assert!(pk_reader.read_next_vector(&mut buf).unwrap());
        assert_eq!(buf, [1.0, 2.0]);
        assert!(!pk_reader.read_next_vector(&mut buf).unwrap()); // NULL row
        assert!(pk_reader.read_next_vector(&mut buf).unwrap());
        assert_eq!(buf, [3.0, 4.0]);
        assert!(pk_reader.read_next_vector(&mut buf).is_err()); // past row count

        // A file name absent from the split is rejected as invalid.
        let missing = BucketActiveFile {
            file_name: "absent.parquet".to_string(),
            row_count: 3,
        };
        let err = factory
            .create(&missing)
            .await
            .err()
            .expect("absent file must be rejected");
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }
}
