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

//! Read implementation for Java-compatible `type=format-table` metadata.

use super::data_file_reader::DataFileReader;
use super::read_builder::split_scan_predicates;
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::{build_target_arrow_schema, paimon_type_to_arrow};
use crate::spec::{extract_datum, BinaryRow, CoreOptions, DataField, DataType, Datum, Predicate};
use crate::{DataSplit, Error};
use arrow_array::{
    new_null_array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, RecordBatchOptions, StringArray,
    Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use async_stream::try_stream;
use futures::StreamExt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct FormatTableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
    data_predicates: Vec<Predicate>,
    limit: Option<usize>,
}

impl<'a> FormatTableRead<'a> {
    pub(crate) fn new(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            table,
            read_type,
            data_predicates,
            limit,
        }
    }

    pub(crate) fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    pub(crate) fn data_predicates(&self) -> &[Predicate] {
        &self.data_predicates
    }

    pub(crate) fn table(&self) -> &Table {
        self.table
    }

    pub(crate) fn with_filter(mut self, filter: Predicate) -> Self {
        self.data_predicates = split_scan_predicates(self.table, filter).1;
        self
    }

    pub(crate) fn to_arrow(
        &self,
        data_splits: &[DataSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let read_type = self.read_type.clone();
        let output_schema = build_target_arrow_schema(&read_type)?;
        let partition_keys = self.table.schema().partition_keys().to_vec();
        let partition_fields = self.table.schema().partition_fields();
        let (data_read_type, data_columns, partition_columns) =
            split_format_read_type(&read_type, &partition_keys);
        let table_fields = self.table.schema().fields().to_vec();
        let (data_table_fields, data_predicates) =
            split_format_table_fields(&table_fields, &partition_keys, &self.data_predicates);

        let splits = data_splits.to_vec();
        let file_io = self.table.file_io().clone();
        let schema_manager = self.table.schema_manager().clone();
        let schema_id = self.table.schema().id();
        let mut remaining = self.limit;

        Ok(try_stream! {
            for split in splits {
                if matches!(remaining, Some(0)) {
                    break;
                }

                let mut stream = DataFileReader::new(
                    file_io.clone(),
                    schema_manager.clone(),
                    schema_id,
                    data_table_fields.clone(),
                    data_read_type.clone(),
                    data_predicates.clone(),
                )
                .read(std::slice::from_ref(&split))?;

                while let Some(batch) = stream.next().await {
                    if matches!(remaining, Some(0)) {
                        break;
                    }

                    let batch = project_format_batch(
                        batch?,
                        &split,
                        &read_type,
                        &data_columns,
                        &partition_columns,
                        &partition_fields,
                        &output_schema,
                    )?;
                    let Some(batch) = apply_limit(batch, &mut remaining) else {
                        break;
                    };
                    yield batch;
                }
            }
        }
        .boxed())
    }
}

fn split_format_read_type(
    read_type: &[DataField],
    partition_keys: &[String],
) -> (Vec<DataField>, Vec<Option<usize>>, Vec<Option<usize>>) {
    let mut data_read_type = Vec::new();
    let mut data_columns = Vec::with_capacity(read_type.len());
    let mut partition_columns = Vec::with_capacity(read_type.len());

    for field in read_type {
        if let Some(partition_index) = partition_keys.iter().position(|key| key == field.name()) {
            data_columns.push(None);
            partition_columns.push(Some(partition_index));
        } else {
            data_columns.push(Some(data_read_type.len()));
            partition_columns.push(None);
            data_read_type.push(field.clone());
        }
    }

    (data_read_type, data_columns, partition_columns)
}

fn split_format_table_fields(
    table_fields: &[DataField],
    partition_keys: &[String],
    predicates: &[Predicate],
) -> (Vec<DataField>, Vec<Predicate>) {
    let mut data_fields = Vec::new();
    let mut data_mapping = Vec::with_capacity(table_fields.len());

    for field in table_fields {
        if partition_keys.iter().any(|key| key == field.name()) {
            data_mapping.push(None);
        } else {
            data_mapping.push(Some(data_fields.len()));
            data_fields.push(field.clone());
        }
    }

    let data_predicates = predicates
        .iter()
        .filter_map(|predicate| predicate.project_field_index_inclusive(&data_mapping))
        .collect();

    (data_fields, data_predicates)
}

fn project_format_batch(
    batch: RecordBatch,
    split: &DataSplit,
    read_type: &[DataField],
    data_columns: &[Option<usize>],
    partition_columns: &[Option<usize>],
    partition_fields: &[DataField],
    output_schema: &Arc<arrow_schema::Schema>,
) -> crate::Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns = Vec::with_capacity(read_type.len());

    for (idx, field) in read_type.iter().enumerate() {
        if let Some(data_index) = data_columns[idx] {
            columns.push(batch.column(data_index).clone());
            continue;
        }

        let Some(partition_index) = partition_columns[idx] else {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Format table read field '{}' is neither data nor partition",
                    field.name()
                ),
                source: None,
            });
        };
        let Some(partition_field) = partition_fields.get(partition_index) else {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Format table partition field '{}' is missing from partition schema",
                    field.name()
                ),
                source: None,
            });
        };
        columns.push(partition_array(
            split.partition(),
            partition_index,
            partition_field.data_type(),
            num_rows,
        )?);
    }

    if columns.is_empty() {
        RecordBatch::try_new_with_options(
            output_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
    } else {
        RecordBatch::try_new(output_schema.clone(), columns)
    }
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build format table RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn partition_array(
    partition: &BinaryRow,
    partition_index: usize,
    data_type: &DataType,
    num_rows: usize,
) -> crate::Result<ArrayRef> {
    let arrow_type = paimon_type_to_arrow(data_type)?;
    if partition.arity() <= partition_index as i32 || partition.is_null_at(partition_index) {
        return Ok(new_null_array(&arrow_type, num_rows));
    }

    let datum = extract_datum(partition, partition_index, data_type)?;
    let Some(datum) = datum else {
        return Ok(new_null_array(&arrow_type, num_rows));
    };

    Ok(match (datum, data_type) {
        (Datum::Bool(value), DataType::Boolean(_)) => {
            Arc::new(BooleanArray::from(vec![Some(value); num_rows]))
        }
        (Datum::TinyInt(value), DataType::TinyInt(_)) => {
            Arc::new(Int8Array::from(vec![Some(value); num_rows]))
        }
        (Datum::SmallInt(value), DataType::SmallInt(_)) => {
            Arc::new(Int16Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Int(value), DataType::Int(_)) => {
            Arc::new(Int32Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Long(value), DataType::BigInt(_)) => {
            Arc::new(Int64Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Float(value), DataType::Float(_)) => {
            Arc::new(Float32Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Double(value), DataType::Double(_)) => {
            Arc::new(Float64Array::from(vec![Some(value); num_rows]))
        }
        (Datum::String(value), DataType::Char(_) | DataType::VarChar(_)) => {
            let values = std::iter::repeat_with(|| Some(value.as_str()))
                .take(num_rows)
                .collect::<Vec<_>>();
            Arc::new(StringArray::from(values))
        }
        (Datum::Bytes(value), DataType::Binary(_) | DataType::VarBinary(_)) => {
            let values = std::iter::repeat_with(|| Some(value.as_slice()))
                .take(num_rows)
                .collect::<Vec<_>>();
            Arc::new(BinaryArray::from(values))
        }
        (Datum::Date(value), DataType::Date(_)) => {
            Arc::new(Date32Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Time(value), DataType::Time(_)) => {
            Arc::new(Time32MillisecondArray::from(vec![Some(value); num_rows]))
        }
        (Datum::Timestamp { millis, nanos }, DataType::Timestamp(ts)) => {
            timestamp_array(millis, nanos, ts.precision(), None, num_rows)?
        }
        (Datum::LocalZonedTimestamp { millis, nanos }, DataType::LocalZonedTimestamp(ts)) => {
            timestamp_array(millis, nanos, ts.precision(), Some("UTC"), num_rows)?
        }
        (_, other) => {
            return Err(Error::Unsupported {
                message: format!(
                    "Format table partition column type '{other:?}' is not supported by the Rust reader yet"
                ),
            });
        }
    })
}

fn timestamp_array(
    millis: i64,
    nanos: i32,
    precision: u32,
    timezone: Option<&'static str>,
    num_rows: usize,
) -> crate::Result<ArrayRef> {
    let array: ArrayRef = match precision {
        0..=3 => {
            let array = TimestampMillisecondArray::from(vec![Some(millis); num_rows]);
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        4..=6 => {
            let value = millis * 1_000 + (nanos as i64) / 1_000;
            let array = TimestampMicrosecondArray::from(vec![Some(value); num_rows]);
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        7..=9 => {
            let value = millis * 1_000_000 + (nanos as i64);
            let array = TimestampNanosecondArray::from(vec![Some(value); num_rows]);
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        _ => {
            return Err(Error::Unsupported {
                message: format!(
                    "Unsupported timestamp precision for format table partition: {precision}"
                ),
            });
        }
    };
    Ok(array)
}

fn apply_limit(batch: RecordBatch, remaining: &mut Option<usize>) -> Option<RecordBatch> {
    let Some(value) = remaining else {
        return Some(batch);
    };
    if *value == 0 {
        return None;
    }
    if batch.num_rows() > *value {
        let limited = batch.slice(0, *value);
        *value = 0;
        Some(limited)
    } else {
        *value -= batch.num_rows();
        Some(batch)
    }
}
