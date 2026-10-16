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
use crate::table::{ArrowRecordBatchStream, Table};
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

/// A read-only system-table view retaining the resolved base table environment.
#[derive(Debug, Clone)]
pub struct SystemTable {
    table: Table,
    name: &'static str,
    fields: Vec<DataField>,
}

impl SystemTable {
    pub(crate) fn new(table: Table, name: &str) -> Result<Self> {
        let options = CoreOptions::new(table.schema().options());
        if options.table_type()?.requires_table_engine() {
            return Err(invalid("System tables require a Paimon table"));
        }
        options.ensure_read_authorized()?;
        let read = SystemTableRead::new(name)?;
        let name = read.name;
        let fields = read.fields;
        Ok(Self {
            table,
            name,
            fields,
        })
    }

    /// Registered system-table suffix.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Declared Arrow output schema.
    pub fn schema(&self) -> SchemaRef {
        schema_for_name(self.name)
    }

    /// Logical fields, preserving Java system-table field IDs.
    pub fn row_type(&self) -> RowType {
        RowType::with_nullable(false, self.fields.clone())
    }

    /// Configure a metadata scan and read using the ordinary read-builder shape.
    pub fn new_read_builder(&self) -> SystemReadBuilder {
        SystemReadBuilder {
            table: self.clone(),
            read: SystemTableRead::new(self.name).expect("validated system-table name"),
        }
    }

    async fn collect(
        &self,
        projection: Option<&[usize]>,
        predicate: Option<&Predicate>,
    ) -> Result<RecordBatch> {
        match self.name {
            "branches" => branches::read(&self.table, projection, predicate).await,
            "buckets" => buckets::read(&self.table, projection, predicate).await,
            "consumers" => consumers::read(&self.table, projection, predicate).await,
            "files" | "file_key_ranges" => {
                files::read(
                    &self.table,
                    self.name == "file_key_ranges",
                    projection,
                    predicate,
                )
                .await
            }
            "manifests" => manifests::read(&self.table, projection, predicate).await,
            "options" => options::read(&self.table, projection, predicate).await,
            "schemas" => schemas::read(&self.table, projection, predicate).await,
            "snapshots" => snapshots::read(&self.table, projection, predicate).await,
            "table_indexes" => table_indexes::read(&self.table, projection, predicate).await,
            "tags" => tags::read(&self.table, projection, predicate).await,
            _ => unreachable!("validated system-table name"),
        }
    }
}

/// Read configuration for a system table. Predicates always use the full schema.
#[derive(Debug, Clone)]
pub struct SystemReadBuilder {
    table: SystemTable,
    read: SystemTableRead,
}

impl SystemReadBuilder {
    /// Select output columns by name, preserving their requested order.
    pub fn with_projection(&mut self, columns: &[&str]) -> Result<&mut Self> {
        self.read.with_projection(columns)?;
        Ok(self)
    }

    /// Filter using a core Predicate, resolved against the full schema.
    pub fn with_filter(&mut self, predicate: Predicate) -> Result<&mut Self> {
        self.read.with_filter(predicate)?;
        Ok(self)
    }

    /// Limit visible rows across all splits, after filtering.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.read.with_limit(limit);
        self
    }

    /// Full logical schema used to resolve filter columns.
    pub fn full_row_type(&self) -> RowType {
        self.table.row_type()
    }

    /// Logical output fields.
    pub fn read_type(&self) -> Vec<DataField> {
        self.read.read_type()
    }

    /// Arrow output schema, including an explicitly empty projection.
    pub fn schema(&self) -> SchemaRef {
        self.read.schema()
    }

    /// Capture metadata for this table and the columns needed by the read.
    pub fn new_scan(&self) -> SystemTableScan {
        SystemTableScan {
            builder: self.clone(),
        }
    }

    /// Build an in-memory reader without retaining the source table environment.
    pub fn new_read(&self) -> SystemTableRead {
        self.read.clone()
    }
}

/// A scan that captures one metadata view. Reads never reload its source files.
#[derive(Debug, Clone)]
pub struct SystemTableScan {
    builder: SystemReadBuilder,
}

impl SystemTableScan {
    /// Capture the configured metadata, preserving an empty result's schema.
    pub async fn plan(&self) -> Result<SystemTablePlan> {
        let builder = &self.builder;
        let mut needed = builder.read.projection.clone();
        if let Some(indices) = &mut needed {
            let mut seen = std::collections::HashSet::new();
            indices.retain(|index| seen.insert(*index));
            if let Some(predicate) = &builder.read.predicate {
                let mut leaves = Vec::new();
                crate::arrow::residual::collect_predicate_leaf_refs(predicate, &mut leaves);
                for (column, _) in leaves {
                    let index = builder
                        .table
                        .schema()
                        .index_of(column)
                        .map_err(arrow_error)?;
                    if !indices.contains(&index) {
                        indices.push(index);
                    }
                }
            }
        }
        let batch = builder
            .table
            .collect(needed.as_deref(), builder.read.predicate.as_ref())
            .await?;
        let batch = match needed {
            Some(indices) => batch.project(&indices).map_err(arrow_error)?,
            None => batch,
        };
        Ok(SystemTablePlan {
            splits: vec![SystemSplit {
                name: builder.table.name.to_string(),
                batch,
            }],
        })
    }
}

/// In-memory metadata splits for one captured system-table scan.
#[derive(Debug, Clone)]
pub struct SystemTablePlan {
    splits: Vec<SystemSplit>,
}

impl SystemTablePlan {
    /// Splits can be read without consulting the catalog or source metadata again.
    pub fn splits(&self) -> &[SystemSplit] {
        &self.splits
    }
}

/// Captured metadata rows. Arrow transport preserves the scan's selected fields.
#[derive(Debug, Clone)]
pub struct SystemSplit {
    name: String,
    batch: RecordBatch,
}

impl SystemSplit {
    /// Reconstruct a captured split received over an Arrow transport.
    pub fn from_record_batch(name: &str, batch: RecordBatch) -> Result<Self> {
        let name = NAMES
            .iter()
            .copied()
            .find(|candidate| *candidate == name)
            .ok_or_else(|| invalid("Unknown system-table split"))?;
        let full = schema_for_name(name);
        let mut seen = std::collections::HashSet::new();
        let mut canonical = Vec::new();
        for (index, field) in batch.schema().fields().iter().enumerate() {
            let expected = full.field_with_name(field.name()).map_err(arrow_error)?;
            if expected.data_type() != field.data_type()
                || !seen.insert(field.name().clone())
                || !expected.is_nullable() && batch.column(index).null_count() != 0
            {
                return Err(invalid("Invalid system-table split schema or nullability"));
            }
            canonical.push(expected.clone());
        }
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(canonical)),
            batch.columns().to_vec(),
            &options,
        )
        .map_err(arrow_error)?;
        Ok(Self {
            name: name.to_string(),
            batch,
        })
    }

    /// System-table suffix identifying this split's schema.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Rows captured at scan time.
    pub fn row_count(&self) -> usize {
        self.batch.num_rows()
    }
    /// Captured batch for Arrow transport and serialization.
    pub fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

/// Exact metadata reader using the core Arrow predicate evaluator.
#[derive(Debug, Clone)]
pub struct SystemTableRead {
    name: &'static str,
    fields: Vec<DataField>,
    projection: Option<Vec<usize>>,
    predicate: Option<Predicate>,
    limit: Option<usize>,
}

impl SystemTableRead {
    /// Create a reader for captured metadata, with no catalog or file IO access.
    pub fn new(name: &str) -> Result<Self> {
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
            name,
            fields,
            projection: None,
            predicate: None,
            limit: None,
        })
    }

    /// Full logical schema used to resolve filter columns.
    pub fn full_row_type(&self) -> RowType {
        RowType::with_nullable(false, self.fields.clone())
    }

    /// Select output columns, preserving order and repeated columns.
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

    /// Filter captured rows using the full system-table schema.
    pub fn with_filter(&mut self, predicate: Predicate) -> Result<&mut Self> {
        self.predicate = Some(normalize_predicate(predicate, &self.fields)?);
        Ok(self)
    }

    /// Limit visible rows across all splits, after filtering.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Logical output fields.
    pub fn read_type(&self) -> Vec<DataField> {
        match &self.projection {
            Some(indices) => indices
                .iter()
                .map(|&index| self.fields[index].clone())
                .collect(),
            None => self.fields.clone(),
        }
    }

    /// Output schema available even when no batches are emitted.
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

    /// Read captured splits, enforcing filter, projection and a global limit.
    pub fn read(&self, splits: &[SystemSplit]) -> Result<ArrowRecordBatchStream> {
        let builder = self.clone();
        let splits = splits.to_vec();
        for split in &splits {
            if split.name != builder.name {
                return Err(invalid(
                    "System-table split belongs to a different table type",
                ));
            }
            let mut needed = builder.read_type();
            if let Some(predicate) = &builder.predicate {
                let mut leaves = Vec::new();
                crate::arrow::residual::collect_predicate_leaf_refs(predicate, &mut leaves);
                for (column, _) in leaves {
                    let field = builder
                        .fields
                        .iter()
                        .find(|field| field.name() == column)
                        .ok_or_else(|| invalid("Unknown predicate column"))?;
                    needed.push(field.clone());
                }
            }
            for field in needed {
                split
                    .batch
                    .schema()
                    .index_of(field.name())
                    .map_err(arrow_error)?;
            }
        }
        Ok(Box::pin(async_stream::try_stream! {
            let mut remaining = builder.limit.unwrap_or(usize::MAX);
            let output = builder.read_type();
            for split in splits {
                if remaining == 0 {
                    break;
                }
                let mut batch = split.batch;
                if let Some(predicate) = &builder.predicate {
                    let mask = crate::arrow::residual::evaluate_predicates_mask(
                        &batch, std::slice::from_ref(predicate), &builder.fields, &builder.fields,
                    )?.ok_or_else(|| invalid("System-table predicate could not be evaluated exactly"))?;
                    batch = arrow_select::filter::filter_record_batch(&batch, &mask)
                        .map_err(arrow_error)?;
                }
                let indices = output.iter()
                    .map(|field| batch.schema().index_of(field.name()).map_err(arrow_error))
                    .collect::<Result<Vec<_>>>()?;
                batch = batch.project(&indices).map_err(arrow_error)?;
                if batch.num_rows() > remaining {
                    batch = batch.slice(0, remaining);
                }
                remaining -= batch.num_rows();
                yield batch;
            }
        }))
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
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType as ArrowType, Field};
    use futures::TryStreamExt;

    fn table() -> SystemTable {
        let file_io = crate::io::FileIO::from_path("memory:///")
            .unwrap()
            .build()
            .unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        Table::from_resolved_schema(
            file_io,
            crate::catalog::Identifier::new("default", "metadata"),
            "memory:///metadata".to_string(),
            TableSchema::new(0, &schema),
            "main",
        )
        .unwrap()
        .system_table("options")
        .unwrap()
    }

    fn split(keys: Vec<&str>, values: Vec<&str>) -> SystemSplit {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", ArrowType::Utf8, true),
                Field::new("value", ArrowType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();
        SystemSplit::from_record_batch("options", batch).unwrap()
    }

    #[tokio::test]
    async fn filter_projection_and_global_limit_use_captured_batches() {
        let table = table();
        let splits = [
            split(vec!["a", "b"], vec!["skip", "first"]),
            split(vec!["b", "b"], vec!["second", "third"]),
        ];
        let mut builder = table.new_read_builder();
        builder.with_projection(&["value"]).unwrap();
        builder
            .with_filter(Predicate::Leaf {
                column: "key".into(),
                index: 999,
                data_type: DataType::VarChar(crate::spec::VarCharType::string_type()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::String("b".into())],
            })
            .unwrap();
        builder.with_limit(2);
        let result: Vec<_> = builder
            .new_read()
            .read(&splits)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let values: Vec<_> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect();
        assert_eq!(values, ["first", "second"]);
        assert_eq!(result[0].schema(), builder.schema());
        builder.with_limit(0);
        assert!(builder
            .new_read()
            .read(&splits)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn empty_projection_preserves_row_count_and_transport() {
        let mut builder = table().new_read_builder();
        builder.with_projection(&[]).unwrap();
        let batches: Vec<_> = builder
            .new_read()
            .read(&[split(vec!["a", "b"], vec!["1", "2"])])
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 2);
        let restored = SystemSplit::from_record_batch("options", batches[0].clone()).unwrap();
        assert_eq!(restored.row_count(), 2);
    }

    #[test]
    fn split_transport_checks_type_names_and_required_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", ArrowType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();
        assert!(SystemSplit::from_record_batch("options", batch).is_err());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            ArrowType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(SystemSplit::from_record_batch("options", batch).is_err());
        let mut builder = table().new_read_builder();
        assert!(builder.with_projection(&["missing"]).is_err());
        let wrong = SystemSplit {
            name: "snapshots".into(),
            batch: split(vec!["a"], vec!["b"]).batch,
        };
        assert!(builder.new_read().read(&[wrong]).is_err());
    }
}
