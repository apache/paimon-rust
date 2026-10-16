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

//! Read-only metadata tables shared by Native bindings and query engines.

use crate::spec::{CoreOptions, DataField, Predicate, RowType};
use crate::table::Table;
use crate::{Error, Result};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use std::sync::Arc;

mod branches;
mod buckets;
mod consumers;
mod files;
mod manifests;
mod options;
mod row_string_cast;
mod schemas;
mod snapshots;
mod table_indexes;
mod tags;

const NAMES: &[&str] = &[
    "branches",
    "buckets",
    "consumers",
    "files",
    "file_key_ranges",
    "manifests",
    "options",
    "schemas",
    "snapshots",
    "table_indexes",
    "tags",
];

/// Reads a small metadata table in one batch using the resolved table environment.
#[derive(Debug, Clone)]
pub struct SystemRead {
    table: Table,
    name: &'static str,
    fields: Vec<DataField>,
    projection: Option<Vec<usize>>,
    predicate: Option<Predicate>,
    limit: Option<usize>,
}

impl SystemRead {
    pub(crate) fn new(table: Table, name: &str) -> Result<Self> {
        let options = CoreOptions::new(table.schema().options());
        if options.table_type()?.requires_table_engine() {
            return Err(invalid("System tables require a Paimon table"));
        }
        options.ensure_read_authorized()?;
        let name = NAMES
            .iter()
            .copied()
            .find(|candidate| candidate.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                invalid(format!(
                    "System table '{name}' requires a catalog or is unknown"
                ))
            })?;
        let fields = schema_for_name(name)
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let id = if name == "snapshots" && field.name() == "writer_version" {
                    15
                } else {
                    index as i32
                };
                Ok(DataField::new(
                    id,
                    field.name().clone(),
                    crate::arrow::arrow_to_paimon_type(field.data_type(), field.is_nullable())?,
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            table,
            name,
            fields,
            projection: None,
            predicate: None,
            limit: None,
        })
    }

    /// Full logical schema used to resolve predicates, including unprojected columns.
    pub fn full_row_type(&self) -> RowType {
        RowType::with_nullable(false, self.fields.clone())
    }

    /// Logical output fields, preserving the requested order and repeated columns.
    pub fn read_type(&self) -> Vec<DataField> {
        match &self.projection {
            Some(indices) => indices
                .iter()
                .map(|&index| self.fields[index].clone())
                .collect(),
            None => self.fields.clone(),
        }
    }

    /// Declared output schema, including an explicitly empty projection.
    pub fn schema(&self) -> SchemaRef {
        let schema = schema_for_name(self.name);
        match &self.projection {
            Some(indices) => Arc::new(Schema::new(
                indices
                    .iter()
                    .map(|&index| schema.field(index).clone())
                    .collect::<Vec<_>>(),
            )),
            None => schema,
        }
    }

    /// Select output columns by name. An empty list selects zero columns.
    pub fn with_projection(&mut self, columns: &[&str]) -> Result<&mut Self> {
        let schema = schema_for_name(self.name);
        self.projection = Some(
            columns
                .iter()
                .map(|name| schema.index_of(name).map_err(arrow_error))
                .collect::<Result<_>>()?,
        );
        Ok(self)
    }

    /// Filter metadata using the full schema, before output projection and limit.
    pub fn with_filter(&mut self, predicate: Predicate) -> Result<&mut Self> {
        self.predicate = Some(normalize_predicate(predicate, &self.fields)?);
        Ok(self)
    }

    /// Limit returned rows after filtering.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Materialize metadata once per call. The returned batch is independent of
    /// the catalog and file IO, and retains its declared schema even when empty.
    pub async fn read(&self) -> Result<RecordBatch> {
        if self.limit == Some(0) {
            return Ok(RecordBatch::new_empty(self.schema()));
        }
        let mut needed = self.projection.clone();
        if let (Some(indices), Some(predicate)) = (&mut needed, &self.predicate) {
            let mut leaves = Vec::new();
            crate::arrow::residual::collect_predicate_leaf_refs(predicate, &mut leaves);
            for (column, _) in leaves {
                let index = schema_for_name(self.name)
                    .index_of(column)
                    .map_err(arrow_error)?;
                if !indices.contains(&index) {
                    indices.push(index);
                }
            }
        }
        let mut batch = match self.name {
            "branches" => {
                branches::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "buckets" => {
                buckets::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "consumers" => {
                consumers::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "files" | "file_key_ranges" => {
                files::read(
                    &self.table,
                    self.name == "file_key_ranges",
                    needed.as_deref(),
                    self.predicate.as_ref(),
                )
                .await?
            }
            "manifests" => {
                manifests::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "options" => {
                options::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "schemas" => {
                schemas::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "snapshots" => {
                snapshots::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "table_indexes" => {
                table_indexes::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?
            }
            "tags" => tags::read(&self.table, needed.as_deref(), self.predicate.as_ref()).await?,
            _ => unreachable!("validated system-table name"),
        };
        if let Some(predicate) = &self.predicate {
            let mask = crate::arrow::residual::evaluate_predicates_mask(
                &batch,
                std::slice::from_ref(predicate),
                &self.fields,
                &self.fields,
            )?
            .ok_or_else(|| invalid("System-table predicate could not be evaluated exactly"))?;
            batch =
                arrow_select::filter::filter_record_batch(&batch, &mask).map_err(arrow_error)?;
        }
        if let Some(indices) = &self.projection {
            batch = batch.project(indices).map_err(arrow_error)?;
        }
        if let Some(limit) = self.limit {
            batch = batch.slice(0, limit.min(batch.num_rows()));
        }
        Ok(batch)
    }
}

fn normalize_predicate(predicate: Predicate, fields: &[DataField]) -> Result<Predicate> {
    Ok(match predicate {
        Predicate::Leaf {
            column,
            op,
            literals,
            ..
        } => {
            let (index, field) = fields
                .iter()
                .enumerate()
                .find(|(_, field)| field.name() == column)
                .ok_or_else(|| invalid(format!("Unknown system-table filter column '{column}'")))?;
            Predicate::Leaf {
                column,
                index,
                data_type: field.data_type().clone(),
                op,
                literals,
            }
        }
        Predicate::And(children) => Predicate::And(
            children
                .into_iter()
                .map(|p| normalize_predicate(p, fields))
                .collect::<Result<_>>()?,
        ),
        Predicate::Or(children) => Predicate::Or(
            children
                .into_iter()
                .map(|p| normalize_predicate(p, fields))
                .collect::<Result<_>>()?,
        ),
        Predicate::Not(child) => Predicate::Not(Box::new(normalize_predicate(*child, fields)?)),
        other => other,
    })
}

fn schema_for_name(name: &str) -> SchemaRef {
    match name {
        "branches" => branches::branches_schema(),
        "buckets" => buckets::buckets_schema(),
        "consumers" => consumers::consumers_schema(),
        "files" => files::files_schema(),
        "file_key_ranges" => files::key_ranges_schema(),
        "manifests" => manifests::manifests_schema(),
        "options" => options::options_schema(),
        "schemas" => schemas::schemas_schema(),
        "snapshots" => snapshots::snapshots_schema(),
        "table_indexes" => table_indexes::table_indexes_schema(),
        "tags" => tags::tags_schema(),
        _ => unreachable!("validated name"),
    }
}

fn record_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    RecordBatch::try_new(schema, columns).map_err(arrow_error)
}

fn local_timestamp_column(values: Vec<i64>) -> Result<ArrayRef> {
    use crate::spec::{DataType, LocalZonedTimestampType, TimestampType};
    use arrow_array::TimestampMillisecondArray;
    let values: ArrayRef = Arc::new(TimestampMillisecondArray::from(values));
    crate::arrow::cast_array_for_schema_evolution(
        &values,
        &DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3)?),
        &DataType::Timestamp(TimestampType::new(3)?),
    )
}

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}
fn arrow_error(error: arrow_schema::ArrowError) -> Error {
    Error::DataInvalid {
        message: error.to_string(),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        DataType, Datum, IntType, PredicateOperator, Schema as PaimonSchema, TableSchema,
    };
    use arrow_array::StringArray;

    async fn table() -> Table {
        let file_io = crate::io::FileIO::from_path("memory:///")
            .unwrap()
            .build()
            .unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("a", "skip")
            .option("b", "match")
            .option("c", "match")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        file_io
            .new_output("memory:///metadata/schema/schema-0")
            .unwrap()
            .write(serde_json::to_vec(&schema).unwrap().into())
            .await
            .unwrap();
        Table::from_resolved_schema(
            file_io,
            crate::catalog::Identifier::new("default", "metadata"),
            "memory:///metadata".to_string(),
            schema,
            "main",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn filter_precedes_projection_and_limit() {
        let mut read = table().await.new_system_read("options").unwrap();
        read.with_projection(&["key"]).unwrap();
        read.with_filter(Predicate::Leaf {
            column: "value".into(),
            index: 999,
            data_type: DataType::Int(IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::String("match".into())],
        })
        .unwrap();
        read.with_limit(1);
        let batch = read.read().await.unwrap();
        assert_eq!(batch.schema(), read.schema());
        assert_eq!(read.read_type()[0].name(), "key");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "b"
        );
        read.with_limit(0);
        let batch = read.read().await.unwrap();
        assert_eq!(batch.schema(), read.schema());
        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn empty_and_repeated_projections_preserve_rows() {
        let mut read = table().await.new_system_read("options").unwrap();
        read.with_projection(&[]).unwrap();
        let batch = read.read().await.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 0);
        assert!(read.read_type().is_empty());
        read.with_projection(&["value", "key", "value"]).unwrap();
        let batch = read.read().await.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["value", "key", "value"]
        );
        assert_eq!(batch.column(0), batch.column(2));
        assert!(read.with_projection(&["missing"]).is_err());
    }

    #[tokio::test]
    async fn unknown_filter_column_is_rejected() {
        let mut read = table().await.new_system_read("options").unwrap();
        assert!(read
            .with_filter(Predicate::Leaf {
                column: "missing".into(),
                index: 0,
                data_type: DataType::Int(IntType::new()),
                op: PredicateOperator::IsNull,
                literals: vec![],
            })
            .is_err());
    }
}
