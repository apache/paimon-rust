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

//! Record-level expiration for primary-key compaction.
//!
//! Mirrors Java `org.apache.paimon.io.RecordLevelExpire`: versions whose
//! configured time field is older than `now - expire` are dropped **before**
//! merge during compaction. Query reads are unaffected.

use crate::spec::{
    extract_datum_from_array, parse_duration_millis, DataField, DataFileMeta, DataType, Datum,
    TableSchema,
};
use crate::table::sort_merge::{
    BufferedBatch, LookupProvider, MergeFunction, MergeResult, MergeRow,
};
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const RECORD_LEVEL_EXPIRE_TIME: &str = "record-level.expire-time";
const RECORD_LEVEL_TIME_FIELD: &str = "record-level.time-field";
/// Java treats BIGINT values at or above this threshold as milliseconds.
const BIGINT_MILLIS_THRESHOLD: i64 = 1_000_000_000_000;

/// Compaction-time filter for expired primary-key versions.
#[derive(Debug, Clone)]
pub(crate) struct RecordLevelExpire {
    expire_secs: i64,
    time_field: DataField,
}

impl RecordLevelExpire {
    /// Build from table options, or `Ok(None)` when expire-time is unset.
    pub(crate) fn create(schema: &TableSchema) -> Result<Option<Self>> {
        let options = schema.options();
        let Some(raw) = options.get(RECORD_LEVEL_EXPIRE_TIME) else {
            return Ok(None);
        };
        if options
            .get("pk-clustering-override")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return Err(Error::Unsupported {
                message: "Cannot support record-level.expire-time with pk-clustering-override"
                    .into(),
            });
        }
        let expire_millis = parse_duration_millis(raw).ok_or_else(|| Error::ConfigInvalid {
            message: format!("Invalid record-level.expire-time '{raw}'"),
        })?;
        let expire_secs =
            i64::try_from(expire_millis / 1000).map_err(|_| Error::ConfigInvalid {
                message: format!("record-level.expire-time '{raw}' is out of range"),
            })?;
        let time_field_name =
            options
                .get(RECORD_LEVEL_TIME_FIELD)
                .ok_or_else(|| Error::ConfigInvalid {
                    message: "You should set time field for record-level expire.".into(),
                })?;
        let time_field = schema
            .fields()
            .iter()
            .find(|field| field.name() == time_field_name)
            .cloned()
            .ok_or_else(|| Error::ConfigInvalid {
                message: format!(
                    "Can not find time field {time_field_name} for record level expire."
                ),
            })?;
        validate_time_field_type(time_field.data_type())?;
        Ok(Some(Self {
            expire_secs,
            time_field,
        }))
    }

    /// Convert a datum to epoch seconds, matching Java
    /// `createFieldGetterAndConvertToSecond`. `None` means NULL (keep).
    pub(crate) fn datum_to_seconds(&self, datum: &Datum) -> Result<Option<i64>> {
        datum_to_seconds(self.time_field.data_type(), datum)
    }

    /// Keep when `now_sec <= time_sec + expire_secs`, or when time is NULL.
    pub(crate) fn is_live(&self, time_secs: Option<i64>, now_secs: i64) -> bool {
        match time_secs {
            None => true,
            Some(time) => now_secs <= time.saturating_add(self.expire_secs),
        }
    }

    /// Whole-file expire only when min(time) is known and already past the
    /// expire watermark. Unknown stats return `false` (never force rewrite).
    pub(crate) fn is_expire_file(&self, file: &DataFileMeta, schema: &TableSchema) -> bool {
        let Some(stats) =
            file.value_stats_for_field(schema.id(), schema.fields(), &self.time_field)
        else {
            return false;
        };
        let Some(min) = stats.min_value.as_ref() else {
            return false;
        };
        let Ok(Some(min_secs)) = self.datum_to_seconds(min) else {
            return false;
        };
        let now_secs = current_epoch_secs();
        now_secs.saturating_sub(self.expire_secs) > min_secs
    }

    /// Whether a sort-merge row should be kept (NULL time or within expire window).
    pub(crate) fn keeps_merge_row(
        &self,
        batches: &[BufferedBatch],
        row: &MergeRow,
        columns: &[usize],
        schema: &SchemaRef,
        now_secs: i64,
    ) -> Result<bool> {
        let time = self.row_time_secs(batches, row, columns, schema)?;
        Ok(self.is_live(time, now_secs))
    }

    /// Whether a single-row KV batch (e.g. LevelLookup hit) should be kept.
    pub(crate) fn keeps_record_batch(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        now_secs: i64,
    ) -> Result<bool> {
        let output_idx = batch
            .schema()
            .index_of(self.time_field.name())
            .map_err(|_| Error::DataInvalid {
                message: format!(
                    "Compaction batch is missing record-level time field '{}'",
                    self.time_field.name()
                ),
                source: None,
            })?;
        let datum = extract_datum_from_array(
            batch.column(output_idx),
            row_idx,
            output_idx,
            self.time_field.data_type(),
        )?;
        let time = match datum {
            None => None,
            Some(value) => self.datum_to_seconds(&value)?,
        };
        Ok(self.is_live(time, now_secs))
    }

    fn row_time_secs(
        &self,
        batches: &[BufferedBatch],
        row: &MergeRow,
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<Option<i64>> {
        let output_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == self.time_field.name())
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Compaction batch is missing record-level time field '{}'",
                    self.time_field.name()
                ),
                source: None,
            })?;
        let array = batches[row.batch_idx].column_ref_for_output(output_idx, columns);
        let datum =
            extract_datum_from_array(array, row.row_idx, output_idx, self.time_field.data_type())?;
        match datum {
            None => Ok(None),
            Some(value) => self.datum_to_seconds(&value),
        }
    }
}

/// Drops expired LevelLookup hits before sort-merge injects them as before-state.
pub(crate) struct ExpiringLookup {
    inner: Arc<dyn LookupProvider>,
    expire: RecordLevelExpire,
    now_secs: i64,
}

impl ExpiringLookup {
    pub(crate) fn wrap(
        inner: Arc<dyn LookupProvider>,
        expire: RecordLevelExpire,
        now_secs: i64,
    ) -> Arc<dyn LookupProvider> {
        Arc::new(Self {
            inner,
            expire,
            now_secs,
        })
    }
}

#[async_trait]
impl LookupProvider for ExpiringLookup {
    async fn lookup(&self, key: &[u8]) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.inner.lookup(key).await? else {
            return Ok(None);
        };
        if batch.num_rows() != 1 {
            return Err(Error::DataInvalid {
                message: format!("Lookup must return one KV row, got {}", batch.num_rows()),
                source: None,
            });
        }
        if self.expire.keeps_record_batch(&batch, 0, self.now_secs)? {
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}

fn validate_time_field_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_) => Ok(()),
        other => Err(Error::ConfigInvalid {
            message: format!(
                "The record level time field type should be one of INT, BIGINT, or TIMESTAMP, but field type is {other}."
            ),
        }),
    }
}

fn datum_to_seconds(data_type: &DataType, datum: &Datum) -> Result<Option<i64>> {
    match (data_type, datum) {
        (DataType::Int(_), Datum::Int(value)) => Ok(Some(i64::from(*value))),
        (DataType::BigInt(_), Datum::Long(value)) => {
            Ok(Some(if *value >= BIGINT_MILLIS_THRESHOLD {
                value / 1000
            } else {
                *value
            }))
        }
        (DataType::Timestamp(_), Datum::Timestamp { millis, .. })
        | (DataType::LocalZonedTimestamp(_), Datum::LocalZonedTimestamp { millis, .. })
        | (DataType::LocalZonedTimestamp(_), Datum::Timestamp { millis, .. })
        | (DataType::Timestamp(_), Datum::LocalZonedTimestamp { millis, .. }) => {
            Ok(Some(millis / 1000))
        }
        _ => Err(Error::DataInvalid {
            message: format!(
                "Unexpected datum {datum:?} for record-level time field type {data_type}"
            ),
            source: None,
        }),
    }
}

pub(crate) fn current_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

/// Drops expired versions before the inner merge function runs.
pub(crate) struct ExpireBeforeMerge {
    expire: RecordLevelExpire,
    now_secs: i64,
    inner: Box<dyn MergeFunction>,
}

impl ExpireBeforeMerge {
    fn live_rows(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<Vec<MergeRow>> {
        let mut live = Vec::with_capacity(rows.len());
        for row in rows {
            if self
                .expire
                .keeps_merge_row(batches, row, columns, schema, self.now_secs)?
            {
                live.push(row.clone());
            }
        }
        Ok(live)
    }
}

impl MergeFunction for ExpireBeforeMerge {
    fn merge(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<MergeResult> {
        let live = self.live_rows(rows, batches, columns, schema)?;
        self.inner.merge(&live, batches, columns, schema)
    }

    fn merge_with_changelog(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<(MergeResult, Option<RecordBatch>)> {
        let live = self.live_rows(rows, batches, columns, schema)?;
        self.inner
            .merge_with_changelog(&live, batches, columns, schema)
    }
}

/// Wrap a non-Lookup compaction merge function with a frozen task-level `now`.
pub(crate) fn wrap_expire_with_now(
    inner: Box<dyn MergeFunction>,
    expire: RecordLevelExpire,
    now_secs: i64,
) -> Box<dyn MergeFunction> {
    Box::new(ExpireBeforeMerge {
        now_secs,
        expire,
        inner,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BigIntType, BinaryRowBuilder, IntType, LocalZonedTimestampType, Schema, TimestampType,
        VarCharType,
    };

    fn schema_with(options: &[(&str, &str)], time_type: DataType) -> TableSchema {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("ts", time_type)
            .primary_key(["id"])
            .option("bucket", "1");
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        TableSchema::new(0, &builder.build().unwrap())
    }

    #[test]
    fn create_requires_time_field_and_rejects_clustering() {
        let missing = schema_with(
            &[("record-level.expire-time", "1 s")],
            DataType::Int(IntType::new()),
        );
        let err = RecordLevelExpire::create(&missing).unwrap_err();
        assert!(err.to_string().contains("time field"));

        let clustering = schema_with(
            &[
                ("record-level.expire-time", "1 s"),
                ("record-level.time-field", "ts"),
                ("pk-clustering-override", "true"),
                ("clustering.columns", "ts"),
            ],
            DataType::Int(IntType::new()),
        );
        let err = RecordLevelExpire::create(&clustering).unwrap_err();
        assert!(err.to_string().contains("pk-clustering-override"));
    }

    #[test]
    fn bigint_millis_threshold_and_null_keep() {
        let schema = schema_with(
            &[
                ("record-level.expire-time", "1 s"),
                ("record-level.time-field", "ts"),
            ],
            DataType::BigInt(BigIntType::new()),
        );
        let expire = RecordLevelExpire::create(&schema).unwrap().unwrap();
        assert_eq!(
            expire
                .datum_to_seconds(&Datum::Long(1_000_000_000_000))
                .unwrap(),
            Some(1_000_000_000)
        );
        assert_eq!(
            expire
                .datum_to_seconds(&Datum::Long(1_000_000_000_001))
                .unwrap(),
            Some(1_000_000_000)
        );
        assert_eq!(
            expire
                .datum_to_seconds(&Datum::Long(2_147_483_649))
                .unwrap(),
            Some(2_147_483_649)
        );
        assert!(expire.is_live(None, 100));
        assert!(!expire.is_live(Some(0), 100));
        assert!(expire.is_live(Some(100), 100));
        // Negative timestamps use the same keep rule: now <= time + expire.
        assert!(expire.is_live(Some(-10), -10));
        assert!(!expire.is_live(Some(-10), 0));
    }

    #[test]
    fn timestamp_types_use_epoch_millis() {
        let schema = schema_with(
            &[
                ("record-level.expire-time", "1 s"),
                ("record-level.time-field", "ts"),
            ],
            DataType::Timestamp(TimestampType::new(6).unwrap()),
        );
        let expire = RecordLevelExpire::create(&schema).unwrap().unwrap();
        assert_eq!(
            expire
                .datum_to_seconds(&Datum::Timestamp {
                    millis: 2_147_483_649,
                    nanos: 0
                })
                .unwrap(),
            Some(2_147_483)
        );

        let ltz = schema_with(
            &[
                ("record-level.expire-time", "1 s"),
                ("record-level.time-field", "ts"),
            ],
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
        );
        let expire = RecordLevelExpire::create(&ltz).unwrap().unwrap();
        assert_eq!(
            expire
                .datum_to_seconds(&Datum::LocalZonedTimestamp {
                    millis: 5_000,
                    nanos: 0
                })
                .unwrap(),
            Some(5)
        );
    }

    #[test]
    fn is_expire_file_requires_known_expired_min() {
        let schema = schema_with(
            &[
                ("record-level.expire-time", "1 s"),
                ("record-level.time-field", "ts"),
            ],
            DataType::Int(IntType::new()),
        );
        let expire = RecordLevelExpire::create(&schema).unwrap().unwrap();
        let mut file = DataFileMeta {
            file_name: "f.parquet".into(),
            file_size: 1,
            row_count: 2,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 1,
            schema_id: 0,
            level: 5,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: Some(vec!["id".into(), "ts".into()]),
            external_path: None,
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        };
        assert!(!expire.is_expire_file(&file, &schema));

        let mut min = BinaryRowBuilder::new(2);
        min.write_int(0, 1);
        min.write_int(1, 1);
        let mut max = BinaryRowBuilder::new(2);
        max.write_int(0, 1);
        max.write_int(1, 1);
        file.value_stats = BinaryTableStats::new(
            min.build().to_serialized_bytes(),
            max.build().to_serialized_bytes(),
            vec![Some(0), Some(0)],
        );
        assert!(expire.is_expire_file(&file, &schema));

        file.schema_id = 1;
        assert!(!expire.is_expire_file(&file, &schema));

        file.schema_id = 0;
        file.value_stats_cols = Some(vec!["id".into()]);
        assert!(!expire.is_expire_file(&file, &schema));
    }

    #[test]
    fn rejects_unsupported_time_field_types() {
        let schema = schema_with(
            &[
                ("record-level.expire-time", "1 s"),
                ("record-level.time-field", "ts"),
            ],
            DataType::VarChar(VarCharType::string_type()),
        );
        let err = RecordLevelExpire::create(&schema).unwrap_err();
        assert!(err.to_string().contains("INT, BIGINT, or TIMESTAMP"));
    }

    #[test]
    fn duration_uses_whole_seconds_like_java_get_seconds() {
        let schema = schema_with(
            &[
                ("record-level.expire-time", "1500 ms"),
                ("record-level.time-field", "ts"),
            ],
            DataType::Int(IntType::new()),
        );
        let expire = RecordLevelExpire::create(&schema).unwrap().unwrap();
        // 1500ms → Java Duration.getSeconds() == 1
        assert_eq!(expire.expire_secs, 1);
    }
}
