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

//! Filesystem partition metadata, matching Java PartitionsTable's manifest path.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow_array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::catalog::{list_partition_entries, FileSystemPartitionEntry};
use crate::spec::{
    BinaryRow, CoreOptions, DataField, Datum, FileKind, IndexManifest, IndexManifestEntry,
};
use crate::table::Table;
use crate::Result;

use super::row_string_cast::format_field;

const DELETED_RECORD_COUNT: usize = 11;

pub(super) fn partitions_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("partition", DataType::Utf8, true),
                Field::new("record_count", DataType::Int64, false),
                Field::new("file_size_in_bytes", DataType::Int64, false),
                Field::new("file_count", DataType::Int64, false),
                Field::new(
                    "last_update_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("created_by", DataType::Utf8, true),
                Field::new("updated_by", DataType::Utf8, true),
                Field::new("options", DataType::Utf8, true),
                Field::new("total_buckets", DataType::Int32, false),
                Field::new("done", DataType::Boolean, false),
                Field::new("deleted_record_count", DataType::Int64, true),
            ]))
        })
        .clone()
}

pub(super) async fn read(table: &Table, projection: Option<&[usize]>) -> Result<RecordBatch> {
    // Resolve once: files and deletion-vector counts must describe the same commit.
    let snapshot = table.resolve_read_snapshot().await?;
    let pinned = table.copy_with_pinned_snapshot(snapshot.as_ref());
    let entries = list_partition_entries(&pinned).await?;
    let needs_deletions = projection.is_none_or(|columns| columns.contains(&DELETED_RECORD_COUNT));
    let deletions = if needs_deletions
        && CoreOptions::new(table.schema().options()).deletion_vectors_enabled()
    {
        deleted_record_counts(&pinned).await?
    } else {
        HashMap::new()
    };
    let fields = table.schema().partition_fields();
    let default_name = CoreOptions::new(table.schema().options())
        .partition_default_name()
        .to_string();
    partition_batch(entries, &fields, &default_name, &deletions)
}

async fn deleted_record_counts(table: &Table) -> Result<HashMap<Vec<u8>, Option<i64>>> {
    let Some(snapshot) = table.resolve_read_snapshot().await? else {
        return Ok(HashMap::new());
    };
    let Some(name) = snapshot.index_manifest() else {
        return Ok(HashMap::new());
    };
    let path = table.snapshot_manager().manifest_path(name);
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    Ok(accumulate_deletions(entries))
}

fn accumulate_deletions(entries: Vec<IndexManifestEntry>) -> HashMap<Vec<u8>, Option<i64>> {
    let mut counts = HashMap::new();
    for entry in entries {
        if entry.kind != FileKind::Add || entry.index_file.index_type != "DELETION_VECTORS" {
            continue;
        }
        let Some(ranges) = entry.index_file.deletion_vectors_ranges else {
            continue;
        };
        if ranges.is_empty() {
            continue;
        }
        let count = counts.entry(entry.partition).or_insert(Some(0_i64));
        for meta in ranges.into_values() {
            *count = count
                .zip(meta.cardinality)
                .map(|(sum, value)| sum.wrapping_add(value));
        }
    }
    counts
}

fn partition_string(row: &BinaryRow, fields: &[DataField], default_name: &str) -> Result<String> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = if row.is_null_at(index) {
                default_name.to_string()
            } else {
                format_field(row, index, field.data_type())?
            };
            let value = if crate::spec::is_java_whitespace_only(&value) {
                default_name
            } else {
                &value
            };
            Ok(format!("{}={value}", field.name()))
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("/"))
}

// Java converts catalog partition specs back to typed rows before looking up
// deletion metadata. A default spec becomes NULL, and binary specs round-trip
// through Java's UTF-8 decoder rather than retaining malformed source bytes.
fn deletion_partition_key(
    row: &BinaryRow,
    fields: &[DataField],
    default_name: &str,
) -> Result<Vec<u8>> {
    let values = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = row.get_datum(index, field.data_type())?;
            let Some(value) = value else {
                return Ok(None);
            };
            let text = format_field(row, index, field.data_type())?;
            if text == default_name || crate::spec::is_java_whitespace_only(&text) {
                return Ok(None);
            }
            Ok(Some(match value {
                Datum::Bytes(_) => Datum::Bytes(text.into_bytes()),
                Datum::Time(value) => {
                    let crate::spec::DataType::Time(typ) = field.data_type() else {
                        unreachable!()
                    };
                    let quantum = 10_i32.pow(3 - typ.precision().min(3));
                    Datum::Time(value / quantum * quantum)
                }
                value => value,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let refs: Vec<_> = values
        .iter()
        .zip(fields)
        .map(|(value, field)| (value.as_ref(), field.data_type()))
        .collect();
    Ok(BinaryRow::from_datums(&refs).to_serialized_bytes())
}

fn partition_batch(
    entries: Vec<FileSystemPartitionEntry>,
    fields: &[DataField],
    default_name: &str,
    deletions: &HashMap<Vec<u8>, Option<i64>>,
) -> Result<RecordBatch> {
    let mut rows = entries
        .into_iter()
        .map(|entry| {
            Ok((
                partition_string(&entry.partition, fields, default_name)?,
                entry,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let n = rows.len();
    let mut names = Vec::with_capacity(n);
    let mut records = Vec::with_capacity(n);
    let mut sizes = Vec::with_capacity(n);
    let mut files = Vec::with_capacity(n);
    let mut times = Vec::with_capacity(n);
    let mut buckets = Vec::with_capacity(n);
    let mut deleted = Vec::with_capacity(n);
    for (name, entry) in rows {
        names.push(name);
        records.push(entry.record_count);
        sizes.push(entry.file_size_in_bytes);
        files.push(entry.file_count);
        times.push(entry.last_file_creation_time);
        buckets.push(entry.total_buckets);
        deleted.push(if deletions.is_empty() {
            Some(0)
        } else {
            deletions
                .get(&deletion_partition_key(
                    &entry.partition,
                    fields,
                    default_name,
                )?)
                .copied()
                .unwrap_or(Some(0))
        });
    }
    super::record_batch(
        partitions_schema(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(records)),
            Arc::new(Int64Array::from(sizes)),
            Arc::new(Int64Array::from(files)),
            super::local_timestamp_column(times)?,
            Arc::new(TimestampMillisecondArray::from(vec![None::<i64>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(Int32Array::from(buckets)),
            Arc::new(BooleanArray::from(vec![false; n])) as ArrayRef,
            Arc::new(Int64Array::from(deleted)),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        DataType as PaimonType, Datum, DeletionVectorMeta, IndexFileMeta, VarCharType,
    };

    fn index(partition: &str, cardinalities: Option<&[Option<i64>]>) -> IndexManifestEntry {
        let typ = PaimonType::VarChar(VarCharType::string_type());
        let value = Datum::String(partition.into());
        let row = BinaryRow::from_datums(&[(Some(&value), &typ)]);
        IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: row.to_serialized_bytes(),
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "DELETION_VECTORS".into(),
                file_name: "index".into(),
                file_size: 1,
                row_count: 1,
                deletion_vectors_ranges: cardinalities.map(|values| {
                    values
                        .iter()
                        .enumerate()
                        .map(|(i, count)| {
                            (
                                format!("file-{i}"),
                                DeletionVectorMeta {
                                    offset: 0,
                                    length: 1,
                                    cardinality: *count,
                                },
                            )
                        })
                        .collect()
                }),
                external_path: None,
                global_index_meta: None,
            },
        }
    }

    #[test]
    fn deletion_counts_sum_buckets_and_keep_unknown_cardinality() {
        let known = index("known", Some(&[Some(2), Some(3)]));
        let mut other_bucket = index("known", Some(&[Some(4)]));
        other_bucket.bucket = 1;
        let unknown = index("unknown", Some(&[Some(2), None]));
        let later = index("unknown", Some(&[Some(7)]));
        let absent = index("absent", None);
        let empty = index("empty", Some(&[]));
        let mut deleted = index("known", Some(&[Some(100)]));
        deleted.kind = FileKind::Delete;
        let mut unrelated = index("known", Some(&[Some(100)]));
        unrelated.index_file.index_type = "HASH".into();
        let counts = accumulate_deletions(vec![
            known.clone(),
            other_bucket,
            unknown.clone(),
            later,
            absent.clone(),
            empty,
            deleted,
            unrelated,
        ]);
        assert_eq!(counts[&known.partition], Some(9));
        assert_eq!(counts[&unknown.partition], None);
        assert!(!counts.contains_key(&absent.partition));
        assert_eq!(counts.len(), 2);
    }

    #[test]
    fn deletion_lookup_uses_typed_partition_specs() {
        let typ = PaimonType::VarChar(VarCharType::string_type());
        let fields = [DataField::new(0, "p".into(), typ.clone())];
        let null = BinaryRow::from_datums(&[(None, &typ)]).to_serialized_bytes();
        for value in ["DEFAULT", "\u{1c}"] {
            let datum = Datum::String(value.into());
            let row = BinaryRow::from_datums(&[(Some(&datum), &typ)]);
            assert_eq!(
                deletion_partition_key(&row, &fields, "DEFAULT").unwrap(),
                null
            );
        }
        let typ = PaimonType::VarBinary(crate::spec::VarBinaryType::new(3).unwrap());
        let fields = [DataField::new(0, "p".into(), typ.clone())];
        let original = Datum::Bytes(vec![0xed, 0xa0, 0x80]);
        let canonical = Datum::Bytes("�".as_bytes().to_vec());
        let row = BinaryRow::from_datums(&[(Some(&original), &typ)]);
        assert_eq!(
            deletion_partition_key(&row, &fields, "DEFAULT").unwrap(),
            BinaryRow::from_datums(&[(Some(&canonical), &typ)]).to_serialized_bytes()
        );
    }

    #[test]
    fn partition_defaults_use_java_whitespace() {
        let typ = PaimonType::VarChar(VarCharType::string_type());
        let fields = [DataField::new(0, "p".into(), typ.clone())];
        for (input, expected) in [
            ("\u{a0}", "\u{a0}"),
            ("\u{85}", "\u{85}"),
            ("\u{2007}", "\u{2007}"),
            ("\u{202f}", "\u{202f}"),
            ("\u{1c}", "DEFAULT"),
            ("", "DEFAULT"),
        ] {
            let datum = Datum::String(input.into());
            let row = BinaryRow::from_datums(&[(Some(&datum), &typ)]);
            assert_eq!(
                partition_string(&row, &fields, "DEFAULT").unwrap(),
                format!("p={expected}")
            );
        }
    }

    #[test]
    fn partition_names_use_casts_default_name_and_unescaped_values() {
        use crate::spec::{BooleanType, DecimalType, TimestampType};
        let types = [
            PaimonType::Boolean(BooleanType::new()),
            PaimonType::Decimal(DecimalType::new(10, 2).unwrap()),
            PaimonType::Timestamp(TimestampType::new(9).unwrap()),
            PaimonType::VarChar(VarCharType::string_type()),
            PaimonType::VarChar(VarCharType::string_type()),
        ];
        let fields: Vec<_> = types
            .iter()
            .enumerate()
            .map(|(i, t)| DataField::new(i as i32, format!("p{i}"), t.clone()))
            .collect();
        let values = [
            Some(Datum::Bool(true)),
            Some(Datum::Decimal {
                unscaled: 120,
                precision: 10,
                scale: 2,
            }),
            Some(Datum::Timestamp {
                millis: 16_725_225_600_123,
                nanos: 456_789,
            }),
            Some(Datum::String("a/b=c".into())),
            None,
        ];
        let refs: Vec<_> = values
            .iter()
            .zip(types.iter())
            .map(|(v, t)| (v.as_ref(), t))
            .collect();
        let row = BinaryRow::from_datums(&refs);
        assert_eq!(
            partition_string(&row, &fields, "DEFAULT").unwrap(),
            "p0=true/p1=1.20/p2=2500-01-01 00:00:00.123456789/p3=a/b=c/p4=DEFAULT"
        );
        assert_eq!(
            partition_string(&BinaryRow::from_datums(&[]), &[], "DEFAULT").unwrap(),
            ""
        );
    }
}
