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
use jiff::tz::TimeZone;

use crate::catalog::{list_partition_entries, FileSystemPartitionEntry};
use crate::spec::{
    BinaryRow, CoreOptions, DataField, Datum, FileKind, IndexManifest, IndexManifestEntry,
};
use crate::table::Table;
use crate::Result;

use super::row_string_cast::{format_field_in_zone, format_row_field_as_java_object_string};

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
    let options = CoreOptions::new(table.schema().options());
    partition_batch(
        entries,
        &fields,
        options.partition_default_name(),
        options.legacy_partition_name(),
        &deletions,
    )
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

fn partition_string(
    row: &BinaryRow,
    fields: &[DataField],
    default_name: &str,
    zone: &TimeZone,
) -> Result<String> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = if row.is_null_at(index) {
                default_name.to_string()
            } else {
                format_field_in_zone(row, index, field.data_type(), zone)?
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

// Java generates a spec using partition.legacy-name, then reconstructs a typed
// row for BOTH display and deletion lookup. Keep this conversion independent of
// whether deletion metadata was requested (notably for LTZ and DST overlaps).
fn normalize_partition(
    row: &BinaryRow,
    fields: &[DataField],
    default_name: &str,
    legacy: bool,
    zone: &TimeZone,
) -> Result<BinaryRow> {
    let values = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = row.get_datum(index, field.data_type())?;
            let Some(value) = value else {
                return Ok(None);
            };
            let text = if legacy {
                format_row_field_as_java_object_string(row, index, field.data_type())?
                    .expect("non-null partition field")
            } else {
                format_field_in_zone(row, index, field.data_type(), zone)?
            };
            if text == default_name || crate::spec::is_java_whitespace_only(&text) {
                return Ok(None);
            }
            Ok(Some(match value {
                Datum::Bytes(_) => Datum::Bytes(text.into_bytes()),
                Datum::Time(_) => Datum::Time(parse_time_spec(&text)?),
                Datum::Timestamp { .. } => {
                    let crate::spec::DataType::Timestamp(typ) = field.data_type() else {
                        unreachable!()
                    };
                    let (millis, nanos) =
                        crate::arrow::parse_java_timestamp(&text, typ.precision(), None)?;
                    Datum::Timestamp {
                        millis,
                        nanos: nanos as i32,
                    }
                }
                Datum::LocalZonedTimestamp { .. } => {
                    let crate::spec::DataType::LocalZonedTimestamp(typ) = field.data_type() else {
                        unreachable!()
                    };
                    let (millis, nanos) =
                        crate::arrow::parse_java_timestamp(&text, typ.precision(), Some(zone))?;
                    Datum::LocalZonedTimestamp {
                        millis,
                        nanos: nanos as i32,
                    }
                }
                // The remaining scalar formatters round-trip the typed value
                // unchanged; reuse it without narrowing through Arrow storage.
                value => value,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let refs: Vec<_> = values
        .iter()
        .zip(fields)
        .map(|(value, field)| (value.as_ref(), field.data_type()))
        .collect();
    Ok(BinaryRow::from_datums(&refs))
}

fn parse_time_spec(text: &str) -> Result<i32> {
    // BinaryStringUtils.toTime preserves the integer legacy spec. Otherwise
    // parse the formatter's HH:mm:ss[.fraction] without chrono's hour limit.
    if let Ok(value) = text.parse::<i32>() {
        return Ok(value);
    }
    let (time, fraction) = text.split_once('.').unwrap_or((text, ""));
    let parts = time
        .split(':')
        .map(|part| {
            part.parse::<i32>()
                .map_err(|error| super::invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let [hours, minutes, seconds] = parts.as_slice() else {
        return Err(super::invalid(format!(
            "Invalid TIME partition spec '{text}'"
        )));
    };
    let fraction = if fraction.is_empty() {
        0
    } else {
        let digits = &fraction[..fraction.len().min(3)];
        digits
            .parse::<i32>()
            .map_err(|error| super::invalid(error.to_string()))?
            * 10_i32.pow(3 - digits.len() as u32)
    };
    Ok((hours * 3600 + minutes * 60 + seconds) * 1000 + fraction)
}

fn partition_batch(
    entries: Vec<FileSystemPartitionEntry>,
    fields: &[DataField],
    default_name: &str,
    legacy: bool,
    deletions: &HashMap<Vec<u8>, Option<i64>>,
) -> Result<RecordBatch> {
    let zone = TimeZone::system();
    let mut rows = entries
        .into_iter()
        .map(|mut entry| {
            entry.partition =
                normalize_partition(&entry.partition, fields, default_name, legacy, &zone)?;
            Ok((
                partition_string(&entry.partition, fields, default_name, &zone)?,
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
                .get(&entry.partition.to_serialized_bytes())
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
        let zone = TimeZone::UTC;
        let typ = PaimonType::VarChar(VarCharType::string_type());
        let fields = [DataField::new(0, "p".into(), typ.clone())];
        let null = BinaryRow::from_datums(&[(None, &typ)]).to_serialized_bytes();
        for value in ["DEFAULT", "\u{1c}"] {
            let datum = Datum::String(value.into());
            let row = BinaryRow::from_datums(&[(Some(&datum), &typ)]);
            assert_eq!(
                normalize_partition(&row, &fields, "DEFAULT", false, &zone)
                    .unwrap()
                    .to_serialized_bytes(),
                null
            );
        }
        let typ = PaimonType::VarBinary(crate::spec::VarBinaryType::new(3).unwrap());
        let fields = [DataField::new(0, "p".into(), typ.clone())];
        let original = Datum::Bytes(vec![0xed, 0xa0, 0x80]);
        let canonical = Datum::Bytes("�".as_bytes().to_vec());
        let row = BinaryRow::from_datums(&[(Some(&original), &typ)]);
        assert_eq!(
            normalize_partition(&row, &fields, "DEFAULT", false, &zone)
                .unwrap()
                .to_serialized_bytes(),
            BinaryRow::from_datums(&[(Some(&canonical), &typ)]).to_serialized_bytes()
        );
    }

    #[test]
    fn time_spec_parser_preserves_java_integer_and_clock_forms() {
        for (spec, millis) in [
            ("0", 0),
            ("00:00:00", 0),
            ("45296120", 45_296_120),
            ("12:34:56.12", 45_296_120),
            ("23:59:59.999", 86_399_999),
            ("24:00:00", 86_400_000),
        ] {
            assert_eq!(parse_time_spec(spec).unwrap(), millis);
        }
    }

    #[test]
    fn time_spec_round_trip_respects_legacy_names() {
        use crate::spec::TimeType;
        for precision in 0..=3 {
            let typ = PaimonType::Time(TimeType::new(precision).unwrap());
            let fields = [DataField::new(0, "p".into(), typ.clone())];
            let datum = Datum::Time(45_296_123);
            let row = BinaryRow::from_datums(&[(Some(&datum), &typ)]);
            for legacy in [true, false] {
                let normalized =
                    normalize_partition(&row, &fields, "DEFAULT", legacy, &TimeZone::UTC).unwrap();
                let quantum = if legacy { 1 } else { 10_i32.pow(3 - precision) };
                assert_eq!(
                    normalized.get_int(0).unwrap(),
                    45_296_123 / quantum * quantum
                );
                let counts = HashMap::from([(row.to_serialized_bytes(), Some(1))]);
                let batch = partition_batch(
                    vec![FileSystemPartitionEntry {
                        partition: row.clone(),
                        ..Default::default()
                    }],
                    &fields,
                    "DEFAULT",
                    legacy,
                    &counts,
                )
                .unwrap();
                let count = batch
                    .column(DELETED_RECORD_COUNT)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                assert_eq!(count, i64::from(legacy || precision == 3));
            }
        }
    }

    #[test]
    fn ltz_spec_round_trip_matches_java_legacy_and_dst_rules() {
        use crate::spec::LocalZonedTimestampType;
        let zone = TimeZone::get("America/New_York").unwrap();
        let typ = PaimonType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap());
        let fields = [DataField::new(0, "p".into(), typ.clone())];
        // Expected keys and names were obtained with Java's partition computer,
        // convertSpecToInternalRow and CastExecutors.resolveToString.
        for (instant, legacy, key, display) in [
            (
                1_704_067_200_000,
                true,
                1_704_085_200_000,
                "2024-01-01 00:00:00.000",
            ),
            (
                1_704_067_200_000,
                false,
                1_704_067_200_000,
                "2023-12-31 19:00:00.000",
            ),
            (
                1_730_615_400_000,
                true,
                1_730_633_400_000,
                "2024-11-03 06:30:00.000",
            ),
            // Parsing 01:30 during the overlap chooses the earlier EDT offset.
            (
                1_730_615_400_000,
                false,
                1_730_611_800_000,
                "2024-11-03 01:30:00.000",
            ),
            // A legacy wall time inside the spring gap shifts forward an hour.
            (
                1_710_038_700_000,
                true,
                1_710_056_700_000,
                "2024-03-10 03:45:00.000",
            ),
        ] {
            let datum = Datum::LocalZonedTimestamp {
                millis: instant,
                nanos: 0,
            };
            let row = BinaryRow::from_datums(&[(Some(&datum), &typ)]);
            let normalized = normalize_partition(&row, &fields, "DEFAULT", legacy, &zone).unwrap();
            assert_eq!(normalized.get_timestamp_raw(0, 3).unwrap(), (key, 0));
            assert_eq!(
                partition_string(&normalized, &fields, "DEFAULT", &zone).unwrap(),
                format!("p={display}")
            );
            let counts = HashMap::from([(row.to_serialized_bytes(), Some(1))]);
            assert_eq!(
                counts
                    .get(&normalized.to_serialized_bytes())
                    .copied()
                    .unwrap_or(Some(0)),
                Some(i64::from(key == instant))
            );
        }
    }

    #[test]
    fn timestamp_spec_round_trip_uses_declared_precision_without_arrow_overflow() {
        use crate::spec::TimestampType;
        for (precision, millis, nanos) in [
            (1, 16_725_225_600_100, 0),
            (3, 16_725_225_600_123, 0),
            (6, 16_725_225_600_123, 456_000),
            (7, 16_725_225_600_123, 456_700),
            (9, 16_725_225_600_123, 456_789),
        ] {
            let typ = PaimonType::Timestamp(TimestampType::new(precision).unwrap());
            let fields = [DataField::new(0, "p".into(), typ.clone())];
            let datum = Datum::Timestamp {
                millis: 16_725_225_600_123,
                nanos: 456_789,
            };
            let row = BinaryRow::from_datums(&[(Some(&datum), &typ)]);
            for legacy in [true, false] {
                let normalized =
                    normalize_partition(&row, &fields, "DEFAULT", legacy, &TimeZone::UTC).unwrap();
                assert_eq!(
                    normalized.get_timestamp_raw(0, precision).unwrap(),
                    (millis, nanos)
                );
            }
        }
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
                partition_string(&row, &fields, "DEFAULT", &TimeZone::UTC).unwrap(),
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
            partition_string(&row, &fields, "DEFAULT", &TimeZone::UTC).unwrap(),
            "p0=true/p1=1.20/p2=2500-01-01 00:00:00.123456789/p3=a/b=c/p4=DEFAULT"
        );
        assert_eq!(
            partition_string(&BinaryRow::from_datums(&[]), &[], "DEFAULT", &TimeZone::UTC).unwrap(),
            ""
        );
    }
}
