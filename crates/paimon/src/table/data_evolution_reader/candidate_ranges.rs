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

use super::{DataEvolutionReader, FieldSource, SourcePlan};
use crate::arrow::filtering::build_field_mapping;
use crate::arrow::format::parquet::ParquetFormatReader;
use crate::predicate_stats::stats_evolution_is_safe;
use crate::spec::{CoreOptions, DataFileMeta, DataType, Predicate};
use crate::table::data_file_reader::{open_file_reader, to_local_row_ranges};
use crate::table::row_id_predicate::intersect_sorted_ranges;
use crate::table::row_sidecar;
use crate::table::RowRange;
use crate::{DataSplit, Error};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};
use std::collections::HashSet;

#[cfg(test)]
#[path = "candidate_ranges_tests.rs"]
mod tests;

impl DataEvolutionReader {
    /// Logical row IDs that may match in the winning providers. An empty list
    /// skips the group and `None` leaves its selection unchanged.
    pub(super) async fn candidate_ranges(
        &self,
        split: &DataSplit,
        plan: &mut SourcePlan,
        selected_ranges: Option<&[RowRange]>,
        sidecar_ranges: Option<&[RowRange]>,
    ) -> Option<Vec<RowRange>> {
        let FieldSource::DataFile { file, .. } = plan.sources.first()? else {
            return None;
        };
        let (first, last) = file.row_id_range()?;
        self.compute_candidate_ranges(split, plan, first, selected_ranges, sidecar_ranges)
            .await
            .unwrap_or_else(|error| {
                log::warn!(
                    "Parquet statistics pruning failed for row IDs {first}..={last} in {}. Using the original row selection ({error})",
                    split.bucket_path()
                );
                None
            })
    }

    async fn compute_candidate_ranges(
        &self,
        split: &DataSplit,
        plan: &mut SourcePlan,
        first_row_id: i64,
        selected_ranges: Option<&[RowRange]>,
        sidecar_ranges: Option<&[RowRange]>,
    ) -> crate::Result<Option<Vec<RowRange>>> {
        for source in &plan.sources {
            if !self.can_use_source_stats(source, sidecar_ranges)? {
                return Ok(None);
            }
        }
        let reader = ParquetFormatReader::with_options(&self.table_options, None)?;
        let mut candidates: Option<Vec<RowRange>> = None;
        let mut unfiltered_sources = Vec::new();
        for source in &mut plan.sources {
            let FieldSource::DataFile {
                file,
                data_fields,
                read_fields,
                parquet_metadata,
            } = source
            else {
                return Ok(None);
            };
            if read_fields.is_empty() {
                continue;
            }
            let file_fields = data_fields.as_deref().unwrap_or(&self.table_fields);
            let winning_ids: HashSet<_> = read_fields.iter().map(|field| field.id()).collect();
            let mut mapping = build_field_mapping(&self.table_fields, file_fields);
            for (index, field) in mapping.iter_mut().zip(&self.table_fields) {
                *index = index.filter(|&i| {
                    winning_ids.contains(&field.id())
                        && supports_stats(field.data_type())
                        && !self.partition_fields.iter().any(|p| p.id() == field.id())
                        && stats_evolution_is_safe(file_fields[i].data_type(), field.data_type())
                });
            }
            // Remap a whole conjunct so an OR or NOT spanning providers stays residual.
            let predicates: Vec<_> = self
                .predicates
                .iter()
                .filter_map(|p| p.remap_field_index(&mapping))
                .filter(|p| !matches!(p, Predicate::AlwaysTrue))
                .collect();
            if predicates.is_empty() {
                unfiltered_sources.push((file, parquet_metadata));
                continue;
            }
            let metadata = self
                .cache_source_metadata(split, file, &reader, parquet_metadata)
                .await?;
            let selection = reader.candidate_row_selection(metadata, predicates, file_fields);
            if let Some(selection) = selection {
                let ranges = absolute_row_ranges(first_row_id, &selection);
                let live = match candidates.as_deref().or(selected_ranges) {
                    Some(previous) => intersect_sorted_ranges(previous, &ranges),
                    None => ranges,
                };
                if live.is_empty() {
                    return Ok(Some(live));
                }
                candidates = Some(live);
            }
        }
        let Some(candidates) = candidates else {
            return Ok(None);
        };
        if selected_ranges == Some(candidates.as_slice()) {
            return Ok(None);
        }
        // A newly narrowed selection could hide a short file from an unfiltered source.
        for (file, metadata) in unfiltered_sources {
            self.cache_source_metadata(split, file, &reader, metadata)
                .await?;
        }
        Ok(Some(candidates))
    }

    async fn cache_source_metadata<'a>(
        &self,
        split: &DataSplit,
        file: &DataFileMeta,
        reader: &ParquetFormatReader,
        cached: &'a mut Option<ArrowReaderMetadata>,
    ) -> crate::Result<&'a ArrowReaderMetadata> {
        let path = split.data_file_path(file);
        let size = u64::try_from(file.file_size).map_err(|_| Error::DataInvalid {
            message: format!("Negative file size {} for {path}", file.file_size),
            source: None,
        })?;
        let input = open_file_reader(&self.file_io, &path, size, self.read_timing.as_ref()).await?;
        let metadata = reader
            .load_merged_read_metadata(input, size)
            .await
            .map_err(|error| {
                Error::from_parquet_with_context(
                    error,
                    &format!("Failed to read pruning metadata for {path}"),
                )
            })?;
        // The fallback reader can reuse this footer even if the manifest row count differs.
        let metadata = cached.insert(metadata);
        let parquet = metadata.metadata();
        let footer_rows = parquet.file_metadata().num_rows();
        let group_rows = parquet.row_groups().iter().try_fold(0usize, |sum, group| {
            sum.checked_add(usize::try_from(group.num_rows()).ok()?)
        });
        if footer_rows != file.row_count
            || group_rows.is_none()
            || group_rows != usize::try_from(file.row_count).ok()
        {
            let group_rows = group_rows.map_or_else(|| "invalid".into(), |rows| rows.to_string());
            return Err(Error::DataInvalid {
                message: format!(
                    "Inconsistent row counts for {path} (manifest {}, footer {footer_rows}, row groups {group_rows})",
                    file.row_count
                ),
                source: None,
            });
        }
        Ok(metadata)
    }

    fn can_use_source_stats(
        &self,
        source: &FieldSource,
        sidecar_ranges: Option<&[RowRange]>,
    ) -> crate::Result<bool> {
        let FieldSource::DataFile {
            file,
            data_fields,
            read_fields,
            ..
        } = source
        else {
            return Ok(false);
        };
        let path = file.external_path.as_deref().unwrap_or(&file.file_name);
        let fields = data_fields.as_deref().unwrap_or(&self.table_fields);
        let nested_write = file
            .write_cols
            .iter()
            .flatten()
            .any(|col| col.contains('.') && !fields.iter().any(|f| f.name() == col));
        if nested_write
            || !path
                .rsplit_once('.')
                .is_some_and(|(_, ext)| ext.eq_ignore_ascii_case("parquet"))
        {
            return Ok(false);
        }

        // A sparse read can use a ROW sidecar even when the primary is missing.
        let Some(ranges) = sidecar_ranges else {
            return Ok(true);
        };
        let options = CoreOptions::new(&self.table_options);
        if read_fields.is_empty()
            || !file.extra_files.iter().any(|name| name.ends_with(".row"))
            || !row_sidecar::supports_read_type(
                read_fields,
                self.blob_as_descriptor || options.blob_as_descriptor(),
            )
        {
            return Ok(true);
        }
        let Some((first, _)) = file.row_id_range() else {
            return Ok(false);
        };
        let local = to_local_row_ranges(ranges, first, file.row_count);
        Ok(row_sidecar::selected_sidecar(file, Some(&local), &options)?.is_none())
    }
}

fn absolute_row_ranges(first: i64, selection: &RowSelection) -> Vec<RowRange> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    for selector in selection.iter() {
        let end = offset + selector.row_count as i64;
        if !selector.skip {
            ranges.push(RowRange::new(first + offset, first + (end - 1)));
        }
        offset = end;
    }
    ranges
}

fn supports_stats(ty: &DataType) -> bool {
    match ty {
        // Parquet has no seconds timestamp annotation. An Arrow TIMESTAMP(0)
        // may store seconds as INT64, while short timestamp bounds use millis.
        DataType::Timestamp(ts) => ts.precision() > 0,
        DataType::LocalZonedTimestamp(ts) => ts.precision() > 0,
        DataType::Boolean(_)
        | DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Date(_)
        | DataType::Char(_)
        | DataType::VarChar(_)
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Decimal(_)
        | DataType::Time(_) => true,
        // Float bounds can omit NaNs, and complex types need field-specific statistics.
        _ => false,
    }
}
