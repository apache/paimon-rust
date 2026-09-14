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

//! Audit row kinds, projection and current/incremental read policy.

use super::{
    cursor_cmp, diff_pairs, ensure_diff_supported_read_type, primary_key_indices,
    value_indices_for_diff, ArrowCursor, CursorOrd, PaimonTableRead, TableRead, TableReadKind,
    DIFF_BATCH_SIZE, MAX_MERGE_INPUT_STREAMS,
};
use crate::arrow::build_target_arrow_schema;
use crate::spec::{
    BigIntType, CoreOptions, DataField, DataType, MergeEngine, TinyIntType, ROW_KIND_FIELD_ID,
    ROW_KIND_FIELD_NAME, SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::incremental_scan::{IncrementalPlan, IncrementalScanMode};
use crate::table::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use crate::table::{ArrowRecordBatchStream, ReadBuilder, Table, TableScan};
use crate::DataSplit;
use arrow_array::{
    builder::StringBuilder, Array, ArrayRef, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_schema::Schema as ArrowSchema;
use arrow_select::interleave::interleave;
use futures::{stream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum AuditLogInput<'a> {
    Current(&'a [DataSplit]),
    Incremental(&'a IncrementalPlan),
}

impl<'a> From<&'a [DataSplit]> for AuditLogInput<'a> {
    fn from(splits: &'a [DataSplit]) -> Self {
        Self::Current(splits)
    }
}

impl<'a, const N: usize> From<&'a [DataSplit; N]> for AuditLogInput<'a> {
    fn from(splits: &'a [DataSplit; N]) -> Self {
        Self::Current(splits)
    }
}

impl<'a> From<&'a Vec<DataSplit>> for AuditLogInput<'a> {
    fn from(splits: &'a Vec<DataSplit>) -> Self {
        Self::Current(splits.as_slice())
    }
}

impl<'a> From<&'a IncrementalPlan> for AuditLogInput<'a> {
    fn from(plan: &'a IncrementalPlan) -> Self {
        Self::Incremental(plan)
    }
}

/// Audit reader retaining winning retract rows and exposing their physical row kind.
///
/// Reuses the projection, predicates and Parquet budget of the supplied read.
/// Without an explicit projection, adds `rowkind` and the configured sequence column.
#[derive(Debug, Clone)]
pub struct AuditLogRead<'a> {
    read: PaimonTableRead<'a>,
    projection: Option<Vec<DataField>>,
}

impl<'a> AuditLogRead<'a> {
    pub fn new(read: TableRead<'a>) -> crate::Result<Self> {
        read.ensure_query_auth_allowed()?;
        match read.0 {
            TableReadKind::Paimon(read) => {
                let projection = read.explicit_projection.then(|| read.read_type.clone());
                Ok(Self { read, projection })
            }
            TableReadKind::Format(_) => Err(crate::Error::Unsupported {
                message: "Format tables do not support audit log batch read".to_string(),
            }),
        }
    }

    /// Reads current-state splits or a validated incremental plan.
    pub fn to_arrow<'input>(
        &self,
        input: impl Into<AuditLogInput<'input>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        match input.into() {
            AuditLogInput::Current(splits) => self.audit_current_stream(splits),
            AuditLogInput::Incremental(plan) => {
                plan.validate()?;
                self.audit_incremental_stream(plan)
            }
        }
    }

    fn audit_current_stream(
        &self,
        data_splits: &[DataSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        let output_read_type = self.audit_read_type()?;
        let include_rowkind = audit_field_requested(&output_read_type, ROW_KIND_FIELD_ID);
        let include_sequence = audit_field_requested(&output_read_type, SEQUENCE_NUMBER_FIELD_ID);
        let user_read_type = self.audit_user_read_type();
        let audit_schema =
            audit_schema_for_read_type(&user_read_type, include_rowkind, include_sequence)?;
        let has_primary_keys = !self.read.table.schema().primary_keys().is_empty();

        let physical_stream = if has_primary_keys {
            let core_options = self.read.table.schema().core_options();
            let mut read_type = Vec::with_capacity(user_read_type.len() + 2);
            if include_sequence {
                read_type.push(DataField::new(
                    SEQUENCE_NUMBER_FIELD_ID,
                    SEQUENCE_NUMBER_FIELD_NAME.to_string(),
                    DataType::BigInt(BigIntType::new()),
                ));
            }
            if include_rowkind {
                read_type.push(DataField::new(
                    VALUE_KIND_FIELD_ID,
                    VALUE_KIND_FIELD_NAME.to_string(),
                    DataType::TinyInt(TinyIntType::new()),
                ));
            }
            read_type.extend(user_read_type.iter().cloned());

            let merge_engine = core_options.merge_engine()?;
            let (raw_splits, merge_splits) = partition_audit_splits(data_splits, merge_engine);
            let parquet_read_budget = self.read.parquet_read_budget()?;
            let raw_stream = DataFileReader::new(
                self.read.table.file_io.clone(),
                self.read.table.schema_manager().clone(),
                self.read.table.schema().id(),
                self.read.table.schema.fields().to_vec(),
                read_type.clone(),
                self.read.data_predicates.clone(),
            )
            .with_file_index_read_enabled(core_options.file_index_read_enabled())
            .with_batch_size(Some(core_options.read_batch_size()?))
            .with_parquet_read_budget(Some(Arc::clone(&parquet_read_budget)))
            .read(&raw_splits)?;
            let merge_reader = KeyValueFileReader::new(
                self.read.table.file_io.clone(),
                KeyValueReadConfig {
                    table_name: self.read.table.identifier().full_name(),
                    table_options: self.read.table.schema().options().clone(),
                    schema_manager: self.read.table.schema_manager().clone(),
                    table_schema_id: self.read.table.schema().id(),
                    table_fields: self.read.table.schema.fields().to_vec(),
                    read_type,
                    predicates: self.read.data_predicates.clone(),
                    primary_keys: self.read.table.schema.trimmed_primary_keys(),
                    merge_engine,
                    sequence_fields: core_options
                        .sequence_fields()
                        .iter()
                        .map(|field| field.to_string())
                        .collect(),
                    read_batch_size: core_options.read_batch_size()?,
                    keep_delete: true,
                    merge_splits: merge_engine == MergeEngine::FirstRow,
                    max_merge_input_streams: Some(MAX_MERGE_INPUT_STREAMS),
                    parquet_read_budget: Some(parquet_read_budget),
                },
            );
            let merge_stream = if merge_engine == MergeEngine::FirstRow {
                let mut groups: HashMap<(Vec<u8>, i32), Vec<DataSplit>> = HashMap::new();
                for split in merge_splits {
                    groups
                        .entry((split.partition().to_serialized_bytes(), split.bucket()))
                        .or_default()
                        .push(split);
                }
                Box::pin(async_stream::try_stream! {
                    for splits in groups.into_values() {
                        let mut group_stream = merge_reader.clone().read(&splits)?;
                        while let Some(batch) = group_stream.next().await {
                            yield batch?;
                        }
                    }
                }) as ArrowRecordBatchStream
            } else {
                merge_reader.read(&merge_splits)?
            };
            Box::pin(stream::select_all([raw_stream, merge_stream]))
        } else {
            self.read.to_arrow(data_splits)?
        };

        let stream = audit_stream_from_physical(
            physical_stream,
            audit_schema,
            user_read_type,
            include_rowkind,
            include_sequence,
            has_primary_keys && include_rowkind,
        );
        project_audit_stream(stream, self.projection.as_deref())
    }

    fn audit_incremental_stream(
        &self,
        plan: &IncrementalPlan,
    ) -> crate::Result<ArrowRecordBatchStream> {
        match plan.mode() {
            IncrementalScanMode::Diff => self.audit_diff_stream(plan),
            IncrementalScanMode::Delta => {
                self.audit_raw_stream(plan, !self.read.table.schema().primary_keys().is_empty())
            }
            IncrementalScanMode::Changelog => self.audit_raw_stream(plan, true),
            IncrementalScanMode::Auto => Err(crate::Error::DataInvalid {
                message: "Incremental plan mode Auto must be resolved before consumption"
                    .to_string(),
                source: None,
            }),
        }
    }

    fn audit_read_type(&self) -> crate::Result<Vec<DataField>> {
        let fields = self.projection.clone().unwrap_or_else(|| {
            audit_fields_for_read_type(
                &self.read.read_type,
                true,
                audit_sequence_number_enabled(self.read.table),
            )
        });
        if audit_field_requested(&fields, SEQUENCE_NUMBER_FIELD_ID)
            && !audit_sequence_number_enabled(self.read.table)
        {
            return Err(crate::Error::DataInvalid {
                message: "Audit read requested _SEQUENCE_NUMBER but table-read.sequence-number.enabled is false".to_string(),
                source: None,
            });
        }
        Ok(fields)
    }

    fn audit_user_read_type(&self) -> Vec<DataField> {
        self.read
            .read_type
            .iter()
            .filter(|field| !matches!(field.id(), ROW_KIND_FIELD_ID | SEQUENCE_NUMBER_FIELD_ID))
            .cloned()
            .collect()
    }

    fn audit_raw_stream(
        &self,
        plan: &IncrementalPlan,
        has_value_kind: bool,
    ) -> crate::Result<ArrowRecordBatchStream> {
        plan.validate()?;
        let core_options = self.read.table.schema().core_options();
        let data_splits = plan.data_splits();
        let output_read_type = self.audit_read_type()?;
        let user_read_type = self.audit_user_read_type();
        let include_rowkind = audit_field_requested(&output_read_type, ROW_KIND_FIELD_ID);
        let include_sequence = audit_field_requested(&output_read_type, SEQUENCE_NUMBER_FIELD_ID);
        let audit_schema =
            audit_schema_for_read_type(&user_read_type, include_rowkind, include_sequence)?;

        let mut read_type = user_read_type.clone();
        if include_sequence {
            read_type.insert(
                0,
                DataField::new(
                    SEQUENCE_NUMBER_FIELD_ID,
                    SEQUENCE_NUMBER_FIELD_NAME.to_string(),
                    DataType::BigInt(BigIntType::new()),
                ),
            );
        }
        if has_value_kind && include_rowkind {
            read_type.push(DataField::new(
                VALUE_KIND_FIELD_ID,
                VALUE_KIND_FIELD_NAME.to_string(),
                DataType::TinyInt(TinyIntType::new()),
            ));
        }

        let reader = DataFileReader::new(
            self.read.table.file_io.clone(),
            self.read.table.schema_manager().clone(),
            self.read.table.schema().id(),
            self.read.table.schema.fields().to_vec(),
            read_type,
            self.read.data_predicates.clone(),
        )
        .with_file_index_read_enabled(core_options.file_index_read_enabled())
        .with_batch_size(Some(core_options.read_batch_size()?))
        .with_parquet_read_budget(Some(self.read.parquet_read_budget()?));
        let raw_stream = reader.read(&data_splits)?;
        let stream = audit_stream_from_physical(
            raw_stream,
            audit_schema,
            user_read_type,
            include_rowkind,
            include_sequence,
            has_value_kind && include_rowkind,
        );
        project_audit_stream(stream, self.projection.as_deref())
    }

    fn audit_diff_stream(&self, plan: &IncrementalPlan) -> crate::Result<ArrowRecordBatchStream> {
        let pairs = diff_pairs(plan)?;
        let parallel = CoreOptions::new(self.read.table.schema().options()).diff_parallelism();
        let output_read_type = self.audit_read_type()?;
        let include_sequence = audit_field_requested(&output_read_type, SEQUENCE_NUMBER_FIELD_ID);
        let table = self.read.table.clone();
        let read_type = self.audit_user_read_type();
        let data_predicates = self.read.data_predicates.clone();
        let parquet_read_budget = self.read.parquet_read_budget()?;

        let stream: ArrowRecordBatchStream = Box::pin(async_stream::try_stream! {
            let mut workers = stream::iter(pairs.into_iter().map(|(before, after)| {
                let table = table.clone();
                let read_type = read_type.clone();
                let data_predicates = data_predicates.clone();
                let parquet_read_budget = Arc::clone(&parquet_read_budget);
                let worker: ArrowRecordBatchStream = Box::pin(async_stream::try_stream! {
                    let pair_read = AuditLogRead {
                        read: PaimonTableRead::new(&table, read_type, data_predicates)
                            .with_parquet_read_budget(parquet_read_budget),
                        projection: None,
                    };
                    let mut pair_stream =
                        pair_read.to_audit_log_arrow_for_diff(
                            &before,
                            &after,
                            include_sequence,
                        )?;
                    while let Some(batch) = pair_stream.next().await {
                        yield batch?;
                    }
                });
                worker
            }))
            .flatten_unordered(parallel);
            while let Some(batch) = workers.next().await {
                yield batch?;
            }
        });
        project_audit_stream(stream, self.projection.as_deref())
    }

    fn to_audit_log_arrow_for_diff(
        &self,
        before: &[DataSplit],
        after: &[DataSplit],
        include_sequence: bool,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let audit_schema =
            audit_schema_for_read_type(&self.read.read_type, true, include_sequence)?;

        let mut diff_read_type = self.read.table.schema().fields().to_vec();
        ensure_diff_supported_read_type(&diff_read_type)?;
        if include_sequence {
            diff_read_type.insert(
                0,
                DataField::new(
                    SEQUENCE_NUMBER_FIELD_ID,
                    SEQUENCE_NUMBER_FIELD_NAME.to_string(),
                    DataType::BigInt(BigIntType::new()),
                ),
            );
        }

        let key_indices = primary_key_indices(self.read.table, &diff_read_type)?;
        let value_indices = value_indices_for_diff(self.read.table, &diff_read_type);

        let before = before.to_vec();
        let after = after.to_vec();
        let table = self.read.table.clone();
        let read_type_for_output = self.read.read_type.clone();
        let data_predicates = self.read.data_predicates.clone();
        let parquet_read_budget = self.read.parquet_read_budget()?;

        Ok(Box::pin(async_stream::try_stream! {
            let core_options = CoreOptions::new(table.schema().options());
            let pair_read = PaimonTableRead::new(&table, diff_read_type.clone(), data_predicates)
                .with_parquet_read_budget(parquet_read_budget);
            let before_stream =
                pair_read.read_pk_sorted_for_diff_with_type(&before, &core_options, &diff_read_type)?;
            let after_stream =
                pair_read.read_pk_sorted_for_diff_with_type(&after, &core_options, &diff_read_type)?;
            let mut bc = ArrowCursor::new(before_stream).await?;
            let mut ac = ArrowCursor::new(after_stream).await?;
            let mut data_col_indices: Option<Vec<usize>> = None;
            let mut builder = AuditBatchBuilder::new(audit_schema.clone());

            while bc.alive() || ac.alive() {
                let indices = data_col_indices.get_or_insert_with(|| {
                    let sample = if bc.alive() {
                        bc.batch()
                    } else {
                        ac.batch()
                    };
                    diff_output_col_indices(sample, &read_type_for_output, include_sequence)
                        .expect("diff output column indices")
                });
                if !builder.has_data_columns() {
                    builder.set_data_col_indices(indices.clone());
                }
                match cursor_cmp(&bc, &ac, &key_indices, &value_indices)? {
                    CursorOrd::BeforeOnly => {
                        builder.push("-D", (0, bc.batch_id()), bc.batch(), bc.row());
                        bc.advance().await?;
                    }
                    CursorOrd::AfterOnly => {
                        builder.push("+I", (1, ac.batch_id()), ac.batch(), ac.row());
                        ac.advance().await?;
                    }
                    CursorOrd::EqualSame => {
                        bc.advance().await?;
                        ac.advance().await?;
                    }
                    CursorOrd::EqualDiff => {
                        builder.push("-U", (0, bc.batch_id()), bc.batch(), bc.row());
                        builder.push("+U", (1, ac.batch_id()), ac.batch(), ac.row());
                        bc.advance().await?;
                        ac.advance().await?;
                    }
                }
                if builder.len() >= DIFF_BATCH_SIZE {
                    yield builder.flush()?;
                }
            }
            if builder.len() > 0 {
                yield builder.flush()?;
            }
        }))
    }
}

impl TableRead<'_> {
    /// Returns audit-log rows for current splits or an incremental plan.
    pub fn to_audit_log_arrow<'input>(
        &self,
        input: impl Into<AuditLogInput<'input>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        AuditLogRead::new(self.clone())?.to_arrow(input)
    }
}

impl<'a> ReadBuilder<'a> {
    /// Create a current-state audit scan that retains every visible row version.
    pub fn new_audit_scan(&self) -> TableScan<'a> {
        self.new_scan().with_all_versions()
    }
}

// Legacy unknown delete counts and first-row level-0 files stay on the merge path.
fn audit_raw_convertible(split: &DataSplit, merge_engine: MergeEngine) -> bool {
    split.raw_convertible()
        && split.data_files().iter().all(|file| {
            file.delete_row_count == Some(0)
                && (merge_engine != MergeEngine::FirstRow || file.level != 0)
        })
}

fn partition_audit_splits(
    data_splits: &[DataSplit],
    merge_engine: MergeEngine,
) -> (Vec<DataSplit>, Vec<DataSplit>) {
    if merge_engine != MergeEngine::FirstRow {
        return data_splits
            .iter()
            .cloned()
            .partition(|split| audit_raw_convertible(split, merge_engine));
    }

    let mut groups: HashMap<(Vec<u8>, i32), Vec<DataSplit>> = HashMap::new();
    for split in data_splits.iter().cloned() {
        groups
            .entry((split.partition().to_serialized_bytes(), split.bucket()))
            .or_default()
            .push(split);
    }
    let mut raw = Vec::new();
    let mut merge = Vec::new();
    for group in groups.into_values() {
        if group
            .iter()
            .all(|split| audit_raw_convertible(split, merge_engine))
        {
            raw.extend(group);
        } else {
            merge.extend(group);
        }
    }
    (raw, merge)
}

struct AuditPhysicalProjection {
    value_kind: Option<usize>,
    sequence: Option<usize>,
    user: Vec<usize>,
}

fn audit_physical_projection(
    schema: &ArrowSchema,
    user_read_type: &[DataField],
    include_rowkind: bool,
    include_sequence: bool,
    has_value_kind: bool,
) -> crate::Result<AuditPhysicalProjection> {
    let by_name: HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name().as_str(), index))
        .collect();
    let index = |name: &str| {
        by_name
            .get(name)
            .copied()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("Audit read missing column '{name}'"),
                source: None,
            })
    };
    Ok(AuditPhysicalProjection {
        value_kind: (include_rowkind && has_value_kind)
            .then(|| index(VALUE_KIND_FIELD_NAME))
            .transpose()?,
        sequence: include_sequence
            .then(|| index(SEQUENCE_NUMBER_FIELD_NAME))
            .transpose()?,
        user: user_read_type
            .iter()
            .map(|field| index(field.name()))
            .collect::<crate::Result<Vec<_>>>()?,
    })
}

fn audit_stream_from_physical(
    raw_stream: ArrowRecordBatchStream,
    audit_schema: Arc<ArrowSchema>,
    user_read_type: Vec<DataField>,
    include_rowkind: bool,
    include_sequence: bool,
    has_value_kind: bool,
) -> ArrowRecordBatchStream {
    Box::pin(async_stream::try_stream! {
        futures::pin_mut!(raw_stream);
        let mut projection = None;
        while let Some(batch) = raw_stream.next().await {
            let batch = batch?;
            if projection.is_none() {
                projection = Some(audit_physical_projection(
                    batch.schema().as_ref(),
                    &user_read_type,
                    include_rowkind,
                    include_sequence,
                    has_value_kind,
                )?);
            }
            let projection = projection.as_ref().unwrap();
            let mut columns = Vec::with_capacity(audit_schema.fields().len());
            if include_rowkind {
                let rowkind_col: ArrayRef = if let Some(index) = projection.value_kind {
                    Arc::new(rowkind_array_from_column(batch.column(index).as_ref())?)
                } else {
                    Arc::new(StringArray::from(vec!["+I"; batch.num_rows()]))
                };
                columns.push(rowkind_col);
            }
            if let Some(index) = projection.sequence {
                columns.push(batch.column(index).clone());
            }
            columns.extend(
                projection
                    .user
                    .iter()
                    .map(|&index| batch.column(index).clone()),
            );
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            yield RecordBatch::try_new_with_options(
                audit_schema.clone(),
                columns,
                &options,
            )
            .map_err(|error| crate::Error::UnexpectedError {
                message: format!("Failed to build audit log batch: {error}"),
                source: Some(Box::new(error)),
            })?;
        }
    })
}

fn project_audit_stream(
    stream: ArrowRecordBatchStream,
    read_type: Option<&[DataField]>,
) -> crate::Result<ArrowRecordBatchStream> {
    let Some(read_type) = read_type else {
        return Ok(stream);
    };
    let schema = build_target_arrow_schema(read_type)?;
    let names = read_type
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    Ok(Box::pin(async_stream::try_stream! {
        futures::pin_mut!(stream);
        let mut indices = None;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let indices = indices.get_or_insert_with(|| {
                names
                    .iter()
                    .map(|name| batch.schema().index_of(name))
                    .collect::<Result<Vec<_>, _>>()
            });
            let indices = indices.as_ref().map_err(|error| crate::Error::DataInvalid {
                message: format!("Audit read projection failed: {error}"),
                source: None,
            })?;
            let columns = indices
                .iter()
                .map(|&index| batch.column(index).clone())
                .collect();
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            yield RecordBatch::try_new_with_options(schema.clone(), columns, &options)
                .map_err(|error| crate::Error::UnexpectedError {
                    message: format!("Failed to project audit log batch: {error}"),
                    source: Some(Box::new(error)),
                })?;
        }
    }))
}

fn audit_field_requested(read_type: &[DataField], field_id: i32) -> bool {
    read_type.iter().any(|field| field.id() == field_id)
}

fn audit_fields_for_read_type(
    read_type: &[DataField],
    include_rowkind: bool,
    include_sequence: bool,
) -> Vec<DataField> {
    let mut fields = Vec::with_capacity(read_type.len() + 2);
    if include_rowkind {
        fields.push(DataField::new(
            ROW_KIND_FIELD_ID,
            ROW_KIND_FIELD_NAME.to_string(),
            DataType::VarChar(crate::spec::VarCharType::string_type()),
        ));
    }
    if include_sequence {
        fields.push(DataField::new(
            SEQUENCE_NUMBER_FIELD_ID,
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::new()),
        ));
    }
    fields.extend(read_type.iter().cloned());
    fields
}

fn audit_schema_for_read_type(
    read_type: &[DataField],
    include_rowkind: bool,
    include_sequence: bool,
) -> crate::Result<Arc<ArrowSchema>> {
    build_target_arrow_schema(&audit_fields_for_read_type(
        read_type,
        include_rowkind,
        include_sequence,
    ))
}

fn audit_sequence_number_enabled(table: &Table) -> bool {
    table
        .schema()
        .core_options()
        .table_read_sequence_number_enabled()
}

fn rowkind_array_from_column(column: &dyn arrow_array::Array) -> crate::Result<StringArray> {
    let values = column
        .as_any()
        .downcast_ref::<arrow_array::Int8Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "AuditLogTable _VALUE_KIND column must be Int8".to_string(),
            source: None,
        })?;
    let mut strings = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if values.is_null(idx) {
            return Err(crate::Error::DataInvalid {
                message: format!("AuditLogTable _VALUE_KIND is null at row {idx}"),
                source: None,
            });
        }
        let rowkind = match values.value(idx) {
            0 => "+I",
            1 => "-U",
            2 => "+U",
            3 => "-D",
            value => {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "AuditLogTable _VALUE_KIND has invalid value {value} at row {idx}"
                    ),
                    source: None,
                });
            }
        };
        strings.push(rowkind);
    }
    Ok(StringArray::from(strings))
}

struct AuditBatchBuilder {
    schema: Arc<ArrowSchema>,
    rowkind: StringBuilder,
    row_indices: Vec<(usize, usize)>,
    pinned_batches: Vec<RecordBatch>,
    pinned_batch_ids: HashMap<(usize, usize), usize>,
    data_col_indices: Vec<usize>,
    len: usize,
}

impl AuditBatchBuilder {
    fn new(schema: Arc<ArrowSchema>) -> Self {
        Self {
            schema,
            rowkind: StringBuilder::new(),
            row_indices: Vec::new(),
            pinned_batches: Vec::new(),
            pinned_batch_ids: HashMap::new(),
            data_col_indices: Vec::new(),
            len: 0,
        }
    }

    fn has_data_columns(&self) -> bool {
        !self.data_col_indices.is_empty()
    }

    fn set_data_col_indices(&mut self, indices: Vec<usize>) {
        self.data_col_indices = indices;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, kind: &str, batch_id: (usize, usize), batch: &RecordBatch, row: usize) {
        self.rowkind.append_value(kind);
        let batch_id = pin_batch(
            &mut self.pinned_batches,
            &mut self.pinned_batch_ids,
            batch_id,
            batch,
        );
        self.row_indices.push((batch_id, row));
        self.len += 1;
    }

    fn flush(&mut self) -> crate::Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = vec![Arc::new(self.rowkind.finish())];
        self.rowkind = StringBuilder::new();
        columns.extend(interleave_columns(
            &self.pinned_batches,
            &self.data_col_indices,
            &self.row_indices,
        )?);
        self.row_indices.clear();
        self.pinned_batches.clear();
        self.pinned_batch_ids.clear();
        self.len = 0;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
            crate::Error::UnexpectedError {
                message: format!("Failed to build audit diff batch: {e}"),
                source: Some(Box::new(e)),
            }
        })
    }
}

fn pin_batch(
    pinned_batches: &mut Vec<RecordBatch>,
    pinned_batch_ids: &mut HashMap<(usize, usize), usize>,
    batch_id: (usize, usize),
    batch: &RecordBatch,
) -> usize {
    if let Some(&pinned_id) = pinned_batch_ids.get(&batch_id) {
        return pinned_id;
    }
    let pinned_id = pinned_batches.len();
    pinned_batches.push(batch.clone());
    pinned_batch_ids.insert(batch_id, pinned_id);
    pinned_id
}

fn interleave_columns(
    batches: &[RecordBatch],
    column_indices: &[usize],
    row_indices: &[(usize, usize)],
) -> crate::Result<Vec<ArrayRef>> {
    column_indices
        .iter()
        .map(|&column_idx| {
            let arrays: Vec<&dyn Array> = batches
                .iter()
                .map(|batch| batch.column(column_idx).as_ref())
                .collect();
            interleave(&arrays, row_indices).map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to interleave diff column: {e}"),
                source: Some(Box::new(e)),
            })
        })
        .collect()
}

fn diff_output_col_indices(
    batch: &RecordBatch,
    read_type: &[DataField],
    include_sequence: bool,
) -> crate::Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(read_type.len() + usize::from(include_sequence));
    if include_sequence {
        indices.push(
            batch
                .schema()
                .index_of(SEQUENCE_NUMBER_FIELD_NAME)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Diff read missing _SEQUENCE_NUMBER: {e}"),
                    source: None,
                })?,
        );
    }
    for field in read_type {
        indices.push(batch.schema().index_of(field.name()).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Diff read missing column '{}': {e}", field.name()),
                source: None,
            }
        })?);
    }
    Ok(indices)
}

#[cfg(test)]
mod tests {
    use super::super::tests::{file, split};
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType as ArrowDataType, Field};
    use futures::TryStreamExt;

    #[tokio::test]
    async fn test_default_audit_projection_bypasses_batch_rebuild() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let input = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let stream: ArrowRecordBatchStream =
            Box::pin(stream::iter(vec![Ok::<_, crate::Error>(input.clone())]));

        let output = project_audit_stream(stream, None)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&schema, &output[0].schema()));

        let stream: ArrowRecordBatchStream =
            Box::pin(stream::iter(vec![Ok::<_, crate::Error>(input)]));
        let output = project_audit_stream(stream, Some(&[]))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(output[0].num_columns(), 0);
        assert_eq!(output[0].num_rows(), 1);
    }

    #[test]
    fn test_rowkind_rejects_null_value_kind() {
        let values = arrow_array::Int8Array::from(vec![Some(0), None]);
        assert!(matches!(
            rowkind_array_from_column(&values),
            Err(crate::Error::DataInvalid { ref message, .. }) if message.contains("null at row 1")
        ));
    }

    #[test]
    fn test_rowkind_rejects_invalid_value_kind() {
        let values = arrow_array::Int8Array::from(vec![4]);
        assert!(matches!(
            rowkind_array_from_column(&values),
            Err(crate::Error::DataInvalid { ref message, .. })
                if message.contains("invalid value 4 at row 0")
        ));
    }

    #[test]
    fn test_audit_batch_builder_pins_each_input_batch_once() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let input_a =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let input_b =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![3, 4]))])
                .unwrap();

        let mut audit = AuditBatchBuilder::new(Arc::new(ArrowSchema::new(vec![
            Field::new(ROW_KIND_FIELD_NAME, ArrowDataType::Utf8, false),
            Field::new("id", ArrowDataType::Int32, false),
        ])));
        audit.set_data_col_indices(vec![0]);
        audit.push("+I", (0, 1), &input_a, 1);
        audit.push("+I", (1, 1), &input_b, 0);
        audit.push("+I", (0, 1), &input_a, 0);
        audit.push("+I", (1, 1), &input_b, 1);
        assert_eq!(audit.pinned_batches.len(), 2);
        let audit_batch = audit.flush().unwrap();
        let audit_ids = audit_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            audit_ids.values(),
            &[2, 3, 1, 4],
            "interleaved batches must preserve row order"
        );
    }

    #[test]
    fn test_audit_split_routing() {
        let raw = split(vec![file("a", 5, Some(0))], true);
        let merge = split(vec![file("a", 5, Some(0))], false);
        let legacy = split(vec![file("a", 5, None)], true);
        assert!(audit_raw_convertible(&raw, MergeEngine::Deduplicate));
        assert!(audit_raw_convertible(&raw, MergeEngine::FirstRow));
        assert!(!audit_raw_convertible(&merge, MergeEngine::Deduplicate));
        assert!(!audit_raw_convertible(&legacy, MergeEngine::Deduplicate));
        let level_zero = split(vec![file("a", 0, Some(0))], true);
        assert!(audit_raw_convertible(&level_zero, MergeEngine::Deduplicate));
        assert!(!audit_raw_convertible(&level_zero, MergeEngine::FirstRow));
        let (raw_only, merge_only) =
            partition_audit_splits(std::slice::from_ref(&raw), MergeEngine::FirstRow);
        assert_eq!((raw_only.len(), merge_only.len()), (1, 0));
        let (raw_group, merge_group) =
            partition_audit_splits(&[raw.clone(), level_zero], MergeEngine::FirstRow);
        assert_eq!((raw_group.len(), merge_group.len()), (0, 2));
    }
}
