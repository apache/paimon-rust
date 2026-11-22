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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{types::Float32Type, Array, FixedSizeListArray, Float32Array, RecordBatch};
use futures::{StreamExt, TryStreamExt};

use super::super::{tests::data_file_meta_with_path, DataEvolutionReader};
use super::VectorFileRead;
use crate::arrow::{format::create_format_writer, ReadBudget};
use crate::io::{FileIOBuilder, FileIOProvider};
use crate::spec::{BinaryRow, DataField, DataType, FloatType, IntType, VectorType};
use crate::table::data_file_reader::DataFileReader;
use crate::table::{DataSplit, RowRange, SchemaManager};

#[derive(Debug)]
struct CountingProvider {
    operator: opendal::Operator,
    opens: Mutex<HashMap<String, usize>>,
}

#[async_trait::async_trait]
impl FileIOProvider for CountingProvider {
    async fn create(&self, path: &str) -> crate::Result<(opendal::Operator, String)> {
        if path.contains(".vector.") {
            let name = path.rsplit('/').next().unwrap().to_string();
            *self.opens.lock().unwrap().entry(name).or_default() += 1;
        }
        Ok((
            self.operator.clone(),
            path.strip_prefix("memory:/").unwrap().into(),
        ))
    }
}

fn vectors(values: &[Option<f32>]) -> FixedSizeListArray {
    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        values
            .iter()
            .map(|value| value.map(|v| vec![Some(v), Some(-v)])),
        2,
    )
}

fn embedding_field() -> DataField {
    DataField::new(
        1,
        "embedding".into(),
        DataType::Vector(VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap()),
    )
}

#[tokio::test]
async fn selected_rows_require_exact_stream_length_and_propagate_errors() {
    let field = embedding_field();
    let io = FileIOBuilder::new("memory").build().unwrap();
    let reader = DataFileReader::new(
        io.clone(),
        SchemaManager::new(io, "memory:/vector_stream_validation".into()),
        0,
        vec![field.clone()],
        |fields| Ok(fields.to_vec()),
        vec![field],
        Vec::new(),
    );
    let file = data_file_meta_with_path(
        "selected.vector.parquet",
        100,
        11,
        1,
        1,
        Some(vec!["embedding"]),
    );
    let split = DataSplit::builder()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_total_buckets(1)
        .with_bucket_path("memory:/vector_stream_validation/bucket-0".into())
        .with_data_files(vec![file.clone()])
        .build()
        .unwrap();
    let batch = RecordBatch::try_from_iter([(
        "embedding",
        Arc::new(vectors(&[Some(1.0), None, Some(3.0), Some(4.0)])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let empty = RecordBatch::new_empty(batch.schema());
    let decode_error = || crate::Error::DataInvalid {
        message: "injected Vector decoder failure".into(),
        source: None,
    };
    for (case, batches, expected_error) in [
        (
            "empty batches around selected rows",
            vec![Ok(empty.clone()), Ok(batch.slice(0, 3)), Ok(empty.clone())],
            None,
        ),
        (
            "premature EOF",
            vec![Ok(batch.slice(0, 2))],
            Some("selected row count"),
        ),
        (
            "extra rows in the last batch",
            vec![Ok(batch.clone())],
            Some("selected row count"),
        ),
        (
            "extra rows after the last batch",
            vec![Ok(batch.slice(0, 3)), Ok(empty), Ok(batch.slice(3, 1))],
            Some("selected row count"),
        ),
        (
            "decoder error before selected rows",
            vec![Err(decode_error())],
            Some("injected Vector decoder failure"),
        ),
        (
            "decoder error after selected rows",
            vec![Ok(batch.slice(0, 3)), Err(decode_error())],
            Some("injected Vector decoder failure"),
        ),
    ] {
        let mut source = VectorFileRead::new(
            file.clone(),
            vec![RowRange::new(100, 101), RowRange::new(110, 110)],
        );
        source.stream = Some(futures::stream::iter(batches).boxed());
        let result: crate::Result<Vec<RecordBatch>> = async {
            let mut output = Vec::new();
            for mut remaining in [2, 1] {
                while remaining > 0 {
                    let batch = source.next_batch(&reader, &split, remaining).await?;
                    remaining -= batch.num_rows() as i64;
                    output.push(batch);
                }
            }
            Ok(output)
        }
        .await;
        if let Some(message) = expected_error {
            let error = result.unwrap_err();
            assert!(
                matches!(error, crate::Error::DataInvalid { .. }),
                "{case}: {error}"
            );
            assert!(error.to_string().contains(message), "{case}: {error}");
        } else {
            let output = result.unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output[0], batch.slice(0, 2));
            assert_eq!(output[1], batch.slice(2, 1));
            assert!(source.stream.is_none());
            assert!(source.cursor.is_none());
        }
    }
}

#[tokio::test]
async fn sparse_versions_open_each_winning_file_once_across_ranges_and_batches() {
    for format in [
        "parquet",
        #[cfg(feature = "vortex")]
        "vortex",
    ] {
        let provider = Arc::new(CountingProvider {
            operator: opendal::Operator::from_config(opendal::services::MemoryConfig::default())
                .unwrap(),
            opens: Mutex::new(HashMap::new()),
        });
        let io = FileIOBuilder::new("memory")
            .build()
            .unwrap()
            .with_provider(provider.clone());
        let field = embedding_field();
        let fields = vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            field.clone(),
        ];
        let path = "memory:/sparse_vectors";
        let bucket = format!("{path}/bucket-0");
        let first = 1000;
        let count = 128;
        let mut files = vec![data_file_meta_with_path(
            "anchor.parquet",
            first,
            count,
            1,
            0,
            Some(vec!["id"]),
        )];
        let mut expected = vec![None; count as usize];
        let mut winners = vec![String::new(); count as usize];
        let mut versions = vec![
            (0, (0..count).map(|i| Some(i as f32)).collect::<Vec<_>>()),
            (
                8,
                (8..count - 8)
                    .map(|i| Some(1000.0 + i as f32))
                    .collect::<Vec<_>>(),
            ),
        ];
        for offset in (13..116).step_by(7) {
            versions.push((offset, vec![(offset % 2 == 0).then_some(-(offset as f32))]));
        }
        for (index, (offset, values)) in versions.into_iter().enumerate() {
            let name = format!("{index}.vector.{format}");
            let array = vectors(&values);
            let batch = RecordBatch::try_from_iter([(
                "embedding",
                Arc::new(array) as arrow_array::ArrayRef,
            )])
            .unwrap();
            let output = io.new_output(&format!("{bucket}/{name}")).unwrap();
            let mut writer = create_format_writer(
                &output,
                batch.schema(),
                "zstd",
                1,
                Some(io.clone()),
                Some(std::slice::from_ref(&field)),
                None,
            )
            .await
            .unwrap();
            for start in (0..batch.num_rows()).step_by(16) {
                writer
                    .write(&batch.slice(start, (batch.num_rows() - start).min(16)))
                    .await
                    .unwrap();
                writer.flush().await.unwrap();
            }
            let size = writer.close().await.unwrap().file_size;
            files.push(data_file_meta_with_path(
                &name,
                first + offset,
                values.len() as i64,
                index as i64 + 2,
                size as i64,
                Some(vec!["embedding"]),
            ));
            for (row, value) in values.into_iter().enumerate() {
                expected[offset as usize + row] = value;
                winners[offset as usize + row] = name.clone();
            }
        }
        files.push(data_file_meta_with_path(
            &format!("shadowed.vector.{format}"),
            first,
            count,
            1,
            1,
            Some(vec!["embedding"]),
        ));
        for (selected, prefetch) in [
            (None, false),
            (
                Some(vec![
                    RowRange::new(1002, 1019),
                    RowRange::new(1031, 1062),
                    RowRange::new(1123, 1127),
                ]),
                false,
            ),
            (Some(vec![RowRange::new(1000, 1007)]), true),
            (
                Some(vec![
                    RowRange::new(1000, 1001),
                    RowRange::new(1004, 1005),
                    RowRange::new(1008, 1009),
                ]),
                true,
            ),
        ] {
            for batch_size in [1, 7, 64] {
                provider.opens.lock().unwrap().clear();
                let selected_rows = (0..count as usize)
                    .filter(|row| {
                        selected.as_ref().is_none_or(|ranges| {
                            ranges.iter().any(|range| {
                                range.overlaps_inclusive(first + *row as i64, first + *row as i64)
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                let expected_opens = selected_rows
                    .iter()
                    .map(|row| (winners[*row].clone(), 1))
                    .collect::<HashMap<_, _>>();
                let mut split = DataSplit::builder()
                    .with_snapshot(1)
                    .with_partition(BinaryRow::new(0))
                    .with_bucket(0)
                    .with_total_buckets(1)
                    .with_bucket_path(bucket.clone())
                    .with_data_files(files.clone());
                if let Some(ranges) = selected.clone() {
                    split = split.with_row_ranges(ranges);
                }
                let split = split.build().unwrap();
                let budget = Arc::new(ReadBudget::new(2, 1).unwrap());
                budget.enable_diagnostics();
                let reader = DataEvolutionReader::new(
                    io.clone(),
                    SchemaManager::new(io.clone(), path.into()),
                    0,
                    fields.clone(),
                    vec![field.clone()],
                    vec![],
                    true,
                    HashSet::new(),
                    HashSet::new(),
                    false,
                    None,
                )
                .unwrap()
                .with_batch_size(Some(batch_size))
                .with_parquet_read_budget(Some(budget.clone()));
                let batches = tokio::time::timeout(
                    Duration::from_secs(5),
                    reader.read(&[split]).unwrap().try_collect::<Vec<_>>(),
                )
                .await
                .expect("interleaved Vector readers must not wait on each other's permits")
                .unwrap();
                let actual = arrow_select::concat::concat(
                    &batches
                        .iter()
                        .map(|batch| batch.column(0).as_ref())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                let actual = actual
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap();
                assert_eq!(actual.len(), selected_rows.len());
                assert_eq!(actual.value_length(), 2);
                for (output_row, input_row) in selected_rows.iter().enumerate() {
                    assert_eq!(actual.is_null(output_row), expected[*input_row].is_none());
                    if let Some(value) = expected[*input_row] {
                        let vector = actual.value(output_row);
                        let vector = vector.as_any().downcast_ref::<Float32Array>().unwrap();
                        assert_eq!(vector.null_count(), 0);
                        assert_eq!(
                            vector.values().as_ref(),
                            &[value, -value],
                            "{format}, batch size {batch_size}, row {input_row}"
                        );
                    }
                }
                if format == "parquet" {
                    assert_eq!(budget.diagnostics().current_inflight, 0);
                    assert_eq!(
                        budget.diagnostics().peak_inflight > 0,
                        prefetch,
                        "only interleaved file readers must disable prefetch"
                    );
                }
                assert_eq!(
                    *provider.opens.lock().unwrap(),
                    expected_opens,
                    "{format}, batch size {batch_size}"
                );
            }
        }
    }
}
