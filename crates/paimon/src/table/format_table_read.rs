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
use super::table_read::{configured_mosaic_prefetch, configured_parquet_read_budget};
use super::{ArrowRecordBatchStream, Table};
use crate::arrow::format::blob::DEFAULT_BLOB_READ_PARALLELISM;
use crate::arrow::partition::partition_array;
use crate::arrow::{build_target_arrow_schema, ReadBudget};
use crate::resource::ResourceContext;
use crate::spec::{DataField, Predicate};
use crate::{DataSplit, Error};
use arrow_array::{RecordBatch, RecordBatchOptions};
use async_stream::try_stream;
use futures::StreamExt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct FormatTableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
    data_predicates: Vec<Predicate>,
    row_filter_factory: Option<Arc<dyn crate::arrow::RowFilterFactory>>,
    parquet_read_budget: Option<Arc<ReadBudget>>,
    resources: Option<ResourceContext>,
    limit: Option<usize>,
    blob_parallelism: usize,
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
            row_filter_factory: None,
            parquet_read_budget: None,
            resources: None,
            limit,
            blob_parallelism: DEFAULT_BLOB_READ_PARALLELISM,
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

    pub(crate) fn with_row_filter_factory(
        mut self,
        factory: Arc<dyn crate::arrow::RowFilterFactory>,
    ) -> Self {
        self.row_filter_factory = Some(factory);
        self
    }

    pub(crate) fn with_parquet_read_budget(mut self, budget: Arc<ReadBudget>) -> Self {
        self.parquet_read_budget = Some(budget);
        self
    }

    pub(crate) fn with_blob_parallelism(mut self, blob_parallelism: usize) -> Self {
        self.blob_parallelism = blob_parallelism;
        self
    }

    pub(crate) fn with_resources(&mut self, resources: ResourceContext) {
        self.resources = Some(resources);
    }

    fn parquet_read_budget(&self) -> crate::Result<Arc<ReadBudget>> {
        let budget = match &self.parquet_read_budget {
            Some(budget) => Arc::clone(budget),
            None => configured_parquet_read_budget(self.table)?,
        };
        Ok(match &self.resources {
            Some(resources) => Arc::new(budget.with_resources(resources.clone())),
            None => budget,
        })
    }

    pub(crate) fn to_arrow(
        &self,
        data_splits: &[DataSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        let core_options = self.table.schema().core_options();
        core_options.ensure_read_authorized()?;
        // Mapping the conjunct onto the data fields drops it, so the read would
        // silently ignore the filter. Guard on the read path, not the builder:
        // `TableRead` is public and can be constructed and filtered directly.
        super::row_id_predicate::reject_row_id_filter(&self.data_predicates, "format tables")?;
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
        let batch_size = Some(core_options.read_batch_size()?);
        let mosaic_prefetch = configured_mosaic_prefetch(self.table)?;
        let row_filter_factory = self.row_filter_factory.clone();
        let parquet_read_budget = Some(self.parquet_read_budget()?);
        let table_options = self.table.schema().options().clone();
        let blob_parallelism = self.blob_parallelism;

        Ok(try_stream! {
            for split in splits {
                if matches!(remaining, Some(0)) {
                    break;
                }

                let mut reader = DataFileReader::new(
                    file_io.clone(),
                    schema_manager.clone(),
                    schema_id,
                    data_table_fields.clone(),
                    data_read_type.clone(),
                    data_predicates.clone(),
                )
                .with_batch_size(batch_size)
                .with_blob_parallelism(blob_parallelism)
                .with_parquet_read_budget(parquet_read_budget.clone())
                .with_table_options(table_options.clone())
                .with_mosaic_prefetch(mosaic_prefetch);
                if let Some(factory) = &row_filter_factory {
                    reader = reader.with_row_filter_factory(Arc::clone(factory));
                }
                let mut stream = reader.read(std::slice::from_ref(&split))?;

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
