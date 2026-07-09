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

//! Scan implementation for Java-compatible `type=format-table` metadata.

use super::{Plan, ScanTrace, Table};
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    extract_datum, BinaryRow, BinaryRowBuilder, CoreOptions, DataField, DataFileMeta, DataType,
    Datum, PartitionComputer, Predicate, PredicateOperator,
};
use crate::table::partition_filter::PartitionFilter;
use crate::table::source::DataSplitBuilder;
use chrono::NaiveDate;

#[derive(Debug, Clone)]
pub(crate) struct FormatTableScan<'a> {
    table: &'a Table,
    partition_filter: Option<PartitionFilter>,
    limit: Option<usize>,
}

impl<'a> FormatTableScan<'a> {
    pub(crate) fn new(
        table: &'a Table,
        partition_filter: Option<PartitionFilter>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            table,
            partition_filter,
            limit,
        }
    }

    pub(crate) async fn plan(&self) -> crate::Result<Plan> {
        self.ensure_query_auth_allowed()?;
        self.plan_inner(None).await
    }

    pub(crate) async fn plan_with_trace(&self) -> crate::Result<(Plan, ScanTrace)> {
        self.ensure_query_auth_allowed()?;
        let mut trace = ScanTrace::default();
        let plan = self.plan_inner(Some(&mut trace)).await?;
        Ok((plan, trace))
    }

    fn ensure_query_auth_allowed(&self) -> crate::Result<()> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()
    }

    async fn plan_inner(&self, trace: Option<&mut ScanTrace>) -> crate::Result<Plan> {
        let core_options = CoreOptions::new(self.table.schema().options());
        let format_extension = supported_format_table_extension(core_options.file_format())?;
        let schema_id = self.table.schema().id();
        let table_path = core_options
            .path()
            .unwrap_or_else(|| self.table.location())
            .trim_end_matches('/')
            .to_string();

        let partition_fields = self.table.schema().partition_fields();
        let mut splits = Vec::new();
        for scan_root in self.scan_roots(&core_options, &table_path)? {
            let statuses = self
                .list_status_recursive_if_exists(&scan_root.path)
                .await?;
            for status in statuses {
                if let Some(split) = self
                    .status_to_split(
                        status,
                        &table_path,
                        format_extension,
                        schema_id,
                        &partition_fields,
                        scan_root.partition.clone(),
                    )
                    .await?
                {
                    splits.push(split);
                }
            }
        }

        splits.sort_by(|left, right| {
            left.bucket_path().cmp(right.bucket_path()).then_with(|| {
                left.data_files()[0]
                    .file_name
                    .cmp(&right.data_files()[0].file_name)
            })
        });
        splits = self.apply_limit_pushdown(splits);

        if let Some(trace) = trace {
            trace.record_final_plan(splits.len(), splits.len(), splits.len());
        }
        Ok(Plan::new(splits))
    }

    fn scan_roots(
        &self,
        core_options: &CoreOptions<'_>,
        table_path: &str,
    ) -> crate::Result<Vec<ScanRoot>> {
        let partition_keys = self.table.schema().partition_keys();
        let partition_fields = self.table.schema().partition_fields();
        if partition_keys.is_empty() {
            return Ok(vec![ScanRoot {
                path: table_path.to_string(),
                partition: BinaryRow::new(0),
            }]);
        }

        let Some(PartitionFilter::PartitionSet { partitions, .. }) = &self.partition_filter else {
            if let Some(PartitionFilter::Predicate(predicate)) = &self.partition_filter {
                if let Some(path) = leading_equality_partition_path(
                    table_path,
                    partition_keys,
                    &partition_fields,
                    predicate,
                    core_options.partition_default_name(),
                    core_options.legacy_partition_name(),
                    core_options.format_table_partition_only_value_in_path(),
                ) {
                    return Ok(vec![ScanRoot {
                        path,
                        partition: BinaryRow::new(0),
                    }]);
                }
            }
            return Ok(vec![ScanRoot {
                path: table_path.to_string(),
                partition: BinaryRow::new(0),
            }]);
        };

        let partition_computer = PartitionComputer::new(
            partition_keys,
            self.table.schema().fields(),
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?;
        let only_value_in_path = core_options.format_table_partition_only_value_in_path();
        let mut roots = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let row = BinaryRow::from_serialized_bytes(partition)?;
            let partition_path = if only_value_in_path {
                partition_path_from_row(
                    &row,
                    &partition_fields,
                    core_options.partition_default_name(),
                    core_options.legacy_partition_name(),
                    true,
                )?
            } else {
                partition_computer.generate_partition_path(&row)?
            };
            roots.push(ScanRoot {
                path: join_path(table_path, &partition_path),
                partition: row,
            });
        }
        roots.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(roots)
    }

    async fn list_status_recursive_if_exists(
        &self,
        path: &str,
    ) -> crate::Result<Vec<crate::io::FileStatus>> {
        match self.table.file_io().list_status_recursive(path).await {
            Ok(statuses) => Ok(statuses),
            Err(err) => {
                if !self.table.file_io().exists(path).await.unwrap_or(true) {
                    Ok(Vec::new())
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn status_to_split(
        &self,
        status: crate::io::FileStatus,
        table_path: &str,
        format_extension: &str,
        schema_id: i64,
        partition_fields: &[DataField],
        known_partition: BinaryRow,
    ) -> crate::Result<Option<crate::DataSplit>> {
        let Some((parent, file_name)) = split_parent_and_file(&status.path) else {
            return Ok(None);
        };
        let parent = parent.to_string();
        let file_name = file_name.to_string();
        if !is_format_table_data_file_name(&file_name) {
            return Ok(None);
        }
        if !file_name.to_ascii_lowercase().ends_with(format_extension) {
            return Ok(None);
        }
        let status = if status.size == 0 {
            self.table.file_io().get_status(&status.path).await?
        } else {
            status
        };
        let file_size = i64::try_from(status.size).map_err(|_| crate::Error::DataInvalid {
            message: format!(
                "Format table file '{}' is too large to fit in i64 metadata",
                status.path
            ),
            source: None,
        })?;
        let data_file = data_file_meta(file_name, file_size, schema_id);
        let partition = if partition_fields.is_empty() {
            BinaryRow::new(0)
        } else if known_partition.arity() == partition_fields.len() as i32
            && !known_partition.is_empty()
        {
            known_partition
        } else {
            let core_options = CoreOptions::new(self.table.schema().options());
            let Some(partition) = partition_row_from_path(
                table_path,
                &parent,
                partition_fields,
                self.table.schema().partition_keys(),
                core_options.partition_default_name(),
                core_options.format_table_partition_only_value_in_path(),
            )?
            else {
                return Ok(None);
            };
            if !self.partition_matches(&partition)? {
                return Ok(None);
            }
            partition
        };

        Ok(Some(
            DataSplitBuilder::new()
                .with_snapshot(0)
                .with_partition(partition)
                .with_bucket(0)
                .with_bucket_path(parent)
                .with_total_buckets(1)
                .with_data_files(vec![data_file])
                .with_raw_convertible(true)
                .build()?,
        ))
    }

    fn partition_matches(&self, partition: &BinaryRow) -> crate::Result<bool> {
        match &self.partition_filter {
            Some(filter) => filter.matches_entry(&partition.to_serialized_bytes()),
            None => Ok(true),
        }
    }

    pub(crate) fn apply_limit_pushdown(
        &self,
        splits: Vec<crate::DataSplit>,
    ) -> Vec<crate::DataSplit> {
        match self.limit {
            Some(0) => Vec::new(),
            Some(limit) if splits.len() > limit => splits.into_iter().take(limit).collect(),
            _ => splits,
        }
    }
}

#[derive(Debug, Clone)]
struct ScanRoot {
    path: String,
    partition: BinaryRow,
}

fn is_format_table_data_file_name(file_name: &str) -> bool {
    !file_name.is_empty() && !file_name.starts_with('.') && !file_name.starts_with('_')
}

fn split_parent_and_file(path: &str) -> Option<(&str, &str)> {
    let trimmed = path.trim_end_matches('/');
    let slash = trimmed.rfind('/')?;
    Some((&trimmed[..slash], &trimmed[slash + 1..]))
}

fn join_path(parent: &str, child: &str) -> String {
    let parent = parent.trim_end_matches('/');
    let child = child.trim_start_matches('/').trim_end_matches('/');
    if child.is_empty() {
        parent.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

fn leading_equality_partition_path(
    table_path: &str,
    partition_keys: &[String],
    partition_fields: &[DataField],
    predicate: &Predicate,
    default_partition_name: &str,
    legacy_partition_name: bool,
    only_value_in_path: bool,
) -> Option<String> {
    let predicates = predicate.clone().split_and();
    let mut values: Vec<Option<&Datum>> = vec![None; partition_keys.len()];
    for predicate in &predicates {
        let Predicate::Leaf {
            index,
            op: PredicateOperator::Eq,
            literals,
            ..
        } = predicate
        else {
            continue;
        };
        if *index < values.len() {
            values[*index] = literals.first();
        }
    }

    let mut segments = Vec::new();
    for (idx, key) in partition_keys.iter().enumerate() {
        let Some(datum) = values[idx] else {
            break;
        };
        let value = partition_value_from_datum(
            datum,
            partition_fields[idx].data_type(),
            default_partition_name,
            legacy_partition_name,
        )?;
        if only_value_in_path {
            segments.push(escape_path_name(&value));
        } else {
            segments.push(format!(
                "{}={}",
                escape_path_name(key),
                escape_path_name(&value)
            ));
        }
    }

    if segments.is_empty() {
        None
    } else {
        Some(join_path(table_path, &segments.join("/")))
    }
}

fn partition_path_from_row(
    row: &BinaryRow,
    partition_fields: &[DataField],
    default_partition_name: &str,
    legacy_partition_name: bool,
    only_value_in_path: bool,
) -> crate::Result<String> {
    let mut segments = Vec::with_capacity(partition_fields.len());
    for (idx, field) in partition_fields.iter().enumerate() {
        let value = match extract_datum(row, idx, field.data_type())? {
            None => default_partition_name.to_string(),
            Some(datum) => partition_value_from_datum(
                &datum,
                field.data_type(),
                default_partition_name,
                legacy_partition_name,
            )
            .ok_or_else(|| crate::Error::Unsupported {
                message: format!(
                    "Format table partition path generation does not support type '{:?}'",
                    field.data_type()
                ),
            })?,
        };
        if only_value_in_path {
            segments.push(escape_path_name(&value));
        } else {
            segments.push(format!(
                "{}={}",
                escape_path_name(field.name()),
                escape_path_name(&value)
            ));
        }
    }
    Ok(segments.join("/"))
}

fn partition_value_from_datum(
    datum: &Datum,
    data_type: &DataType,
    default_partition_name: &str,
    legacy_partition_name: bool,
) -> Option<String> {
    match (datum, data_type) {
        (Datum::Bool(value), DataType::Boolean(_)) => Some(value.to_string()),
        (Datum::TinyInt(value), DataType::TinyInt(_)) => Some(value.to_string()),
        (Datum::SmallInt(value), DataType::SmallInt(_)) => Some(value.to_string()),
        (Datum::Int(value), DataType::Int(_)) => Some(value.to_string()),
        (Datum::Long(value), DataType::BigInt(_)) => Some(value.to_string()),
        (Datum::String(value), DataType::Char(_) | DataType::VarChar(_)) => {
            if value.trim().is_empty() {
                Some(default_partition_name.to_string())
            } else {
                Some(value.clone())
            }
        }
        (Datum::Date(value), DataType::Date(_)) => {
            if legacy_partition_name {
                Some(value.to_string())
            } else {
                Some(format_partition_date(*value))
            }
        }
        (Datum::Time(value), DataType::Time(_)) => Some(value.to_string()),
        _ => None,
    }
}

fn format_partition_date(epoch_days: i32) -> String {
    let date = NaiveDate::from_num_days_from_ce_opt(epoch_days + 719_163)
        .unwrap_or(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    date.format("%Y-%m-%d").to_string()
}

fn escape_path_name(path: &str) -> String {
    let mut result = String::with_capacity(path.len());
    for byte in path.bytes() {
        if should_escape(byte) {
            result.push('%');
            result.push_str(&format!("{byte:02X}"));
        } else {
            result.push(byte as char);
        }
    }
    result
}

fn should_escape(byte: u8) -> bool {
    byte <= 0x1F
        || byte >= 0x7F
        || matches!(
            byte,
            b'"' | b'#'
                | b'%'
                | b'\''
                | b'*'
                | b'/'
                | b':'
                | b'='
                | b'?'
                | b'\\'
                | b'\x7F'
                | b'{'
                | b'['
                | b']'
                | b'^'
        )
}

fn partition_row_from_path(
    table_path: &str,
    file_parent: &str,
    partition_fields: &[DataField],
    partition_keys: &[String],
    default_partition_name: &str,
    only_value_in_path: bool,
) -> crate::Result<Option<BinaryRow>> {
    let relative = match file_parent
        .trim_end_matches('/')
        .strip_prefix(table_path.trim_end_matches('/'))
    {
        Some(path) => path.trim_start_matches('/'),
        None => return Ok(None),
    };
    if relative.is_empty() {
        return Ok(None);
    }

    let mut values = Vec::with_capacity(partition_keys.len());
    if only_value_in_path {
        for segment in relative
            .split('/')
            .filter(|segment| !segment.is_empty())
            .take(partition_keys.len())
        {
            let Some(value) = unescape_path_name(segment) else {
                return Ok(None);
            };
            values.push(value);
        }
        if values.len() != partition_keys.len() {
            return Ok(None);
        }
    } else {
        for key in partition_keys {
            let Some(value) = relative
                .split('/')
                .find_map(|segment| partition_segment_value(segment, key))
            else {
                return Ok(None);
            };
            values.push(value);
        }
    }

    let mut builder = BinaryRowBuilder::new(partition_fields.len() as i32);
    for (idx, value) in values.iter().enumerate() {
        if value == default_partition_name {
            builder.set_null_at(idx);
            continue;
        }
        let Some(datum) = parse_partition_datum(value, partition_fields[idx].data_type()) else {
            return Ok(None);
        };
        builder.write_datum(idx, &datum, partition_fields[idx].data_type());
    }
    Ok(Some(builder.build()))
}

fn partition_segment_value(segment: &str, key: &str) -> Option<String> {
    let (segment_key, segment_value) = segment.split_once('=')?;
    if unescape_path_name(segment_key)? == key {
        unescape_path_name(segment_value)
    } else {
        None
    }
}

fn unescape_path_name(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_value(bytes[i + 1])?;
            let lo = hex_value(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_partition_datum(value: &str, data_type: &DataType) -> Option<Datum> {
    match data_type {
        DataType::Boolean(_) => value.parse::<bool>().ok().map(Datum::Bool),
        DataType::TinyInt(_) => value.parse::<i8>().ok().map(Datum::TinyInt),
        DataType::SmallInt(_) => value.parse::<i16>().ok().map(Datum::SmallInt),
        DataType::Int(_) => value.parse::<i32>().ok().map(Datum::Int),
        DataType::BigInt(_) => value.parse::<i64>().ok().map(Datum::Long),
        DataType::Char(_) | DataType::VarChar(_) => Some(Datum::String(value.to_string())),
        DataType::Date(_) => parse_partition_date(value).map(Datum::Date),
        DataType::Time(_) => value.parse::<i32>().ok().map(Datum::Time),
        _ => None,
    }
}

fn parse_partition_date(value: &str) -> Option<i32> {
    if let Ok(epoch_days) = value.parse::<i32>() {
        return Some(epoch_days);
    }
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;
    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days()
        .try_into()
        .ok()
}

fn supported_format_table_extension(format: &str) -> crate::Result<&'static str> {
    match format.to_ascii_lowercase().as_str() {
        "parquet" => Ok(".parquet"),
        "orc" => Ok(".orc"),
        "avro" => Ok(".avro"),
        "row" => Ok(".row"),
        "mosaic" => Ok(".mosaic"),
        #[cfg(feature = "vortex")]
        "vortex" => Ok(".vortex"),
        other => Err(crate::Error::Unsupported {
            message: format!(
                "Format table file.format '{other}' is not supported by the Rust reader yet"
            ),
        }),
    }
}

fn data_file_meta(file_name: String, file_size: i64, schema_id: i64) -> DataFileMeta {
    DataFileMeta {
        file_name,
        file_size,
        row_count: 0,
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
        delete_row_count: Some(0),
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
    }
}
