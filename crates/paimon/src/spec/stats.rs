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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};

use super::{
    extract_datum_from_arrow, serialize_binary_array_long, BinaryRowBuilder, DataType, Datum,
    EMPTY_SERIALIZED_ROW,
};
use arrow_array::RecordBatch;

/// Deserialize `_NULL_COUNTS` which in Avro is `["null", {"type":"array","items":["null","long"]}]`.
/// Preserves null items as `None` (meaning "unknown") rather than collapsing to 0.
fn deserialize_null_counts<'de, D>(deserializer: D) -> Result<Vec<Option<i64>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<Vec<Option<i64>>> = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

fn serialize_null_counts<S>(value: &[Option<i64>], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Serialize as Option<Vec<Option<i64>>> to match the Avro union schema.
    let wrapped: Option<&[Option<i64>]> = Some(value);
    wrapped.serialize(serializer)
}

/// The statistics for columns, supports the following stats.
///
/// All statistics are stored in the form of a Binary, which can significantly reduce its memory consumption, but the cost is that the column type needs to be known when getting.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/stats/FieldStatsArraySerializer.java#L111>
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BinaryTableStats {
    /// the minimum values of the columns
    #[serde(rename = "_MIN_VALUES", with = "serde_bytes")]
    min_values: Vec<u8>,

    /// the maximum values of the columns
    #[serde(rename = "_MAX_VALUES", with = "serde_bytes")]
    max_values: Vec<u8>,

    /// the number of nulls of the columns
    #[serde(
        rename = "_NULL_COUNTS",
        deserialize_with = "deserialize_null_counts",
        serialize_with = "serialize_null_counts"
    )]
    null_counts: Vec<Option<i64>>,
}

impl BinaryTableStats {
    /// Get the minimum values of the columns
    #[inline]
    pub fn min_values(&self) -> &[u8] {
        &self.min_values
    }

    /// Get the maximum values of the columns
    #[inline]
    pub fn max_values(&self) -> &[u8] {
        &self.max_values
    }

    /// Get the number of nulls of the columns
    #[inline]
    pub fn null_counts(&self) -> &Vec<Option<i64>> {
        &self.null_counts
    }

    pub fn new(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
    ) -> BinaryTableStats {
        Self {
            min_values,
            max_values,
            null_counts,
        }
    }

    /// Stats with empty (arity=0) BinaryRow bytes for min/max and no null counts.
    ///
    /// Use this whenever there are no columns to collect stats for (e.g. a non-partitioned
    /// table's `partition_stats`, or a writer producing no key/value stats columns). Writing
    /// `Vec::new()` here breaks the Java reader: `SerializationUtils.deserializeBinaryRow`
    /// requires at least the 4-byte BE arity prefix and throws `BufferUnderflowException` on
    /// zero-length input.
    pub fn empty() -> BinaryTableStats {
        Self {
            min_values: EMPTY_SERIALIZED_ROW.clone(),
            max_values: EMPTY_SERIALIZED_ROW.clone(),
            null_counts: Vec::new(),
        }
    }

    /// Serialize as a `SimpleStats.SCHEMA` BinaryRow (raw data, no arity prefix), matching
    /// Java `SimpleStats#toRow`: `[_MIN_VALUES bytes] [_MAX_VALUES bytes] [_NULL_COUNTS array<bigint>]`.
    /// `min_values`/`max_values` are already serialized `BinaryRow`s, written as-is.
    pub fn to_simple_stats_row_data(&self) -> Vec<u8> {
        let mut b = BinaryRowBuilder::new(3);
        b.write_bytes(0, &self.min_values);
        b.write_bytes(1, &self.max_values);
        b.write_bytes(2, &serialize_binary_array_long(&self.null_counts));
        b.build_row_data()
    }

    /// Reverse of [`BinaryTableStats::to_simple_stats_row_data`]: a 3-field
    /// `SimpleStats` BinaryRow body (min_values bytes, max_values bytes,
    /// null_counts array<bigint>).
    pub fn from_simple_stats_row_data(data: &[u8]) -> crate::Result<BinaryTableStats> {
        let row = crate::spec::BinaryRow::from_bytes(3, data.to_vec());
        let min_values = row.get_binary(0)?.to_vec();
        let max_values = row.get_binary(1)?.to_vec();
        let null_counts = crate::spec::deserialize_binary_array_long(row.get_binary(2)?)?;
        Ok(BinaryTableStats::new(min_values, max_values, null_counts))
    }
}

impl Display for BinaryTableStats {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// Compute per-column independent min/max/null_count for the specified columns
/// in a RecordBatch. Each entry in `col_indices` is the column index in the batch,
/// and the corresponding entry in `col_types` is its Paimon DataType.
pub fn compute_column_stats(
    batch: &RecordBatch,
    col_indices: &[usize],
    col_types: &[DataType],
) -> crate::Result<BinaryTableStats> {
    let num_cols = col_indices.len();
    let num_rows = batch.num_rows();
    let mut min_datums: Vec<Option<Datum>> = vec![None; num_cols];
    let mut max_datums: Vec<Option<Datum>> = vec![None; num_cols];
    let mut null_counts: Vec<Option<i64>> = vec![Some(0); num_cols];

    for row_idx in 0..num_rows {
        for (pos, (&col_idx, data_type)) in col_indices.iter().zip(col_types.iter()).enumerate() {
            let datum = extract_datum_from_arrow(batch, row_idx, col_idx, data_type)?;
            match datum {
                Some(d) => {
                    if min_datums[pos].as_ref().is_none_or(|m| d < *m) {
                        min_datums[pos] = Some(d.clone());
                    }
                    if max_datums[pos].as_ref().is_none_or(|m| d > *m) {
                        max_datums[pos] = Some(d);
                    }
                }
                None => {
                    *null_counts[pos].as_mut().unwrap() += 1;
                }
            }
        }
    }

    let mut min_builder = BinaryRowBuilder::new(num_cols as i32);
    let mut max_builder = BinaryRowBuilder::new(num_cols as i32);
    for (pos, data_type) in col_types.iter().enumerate() {
        match &min_datums[pos] {
            Some(d) => min_builder.write_datum(pos, d, data_type),
            None => min_builder.set_null_at(pos),
        }
        match &max_datums[pos] {
            Some(d) => max_builder.write_datum(pos, d, data_type),
            None => max_builder.set_null_at(pos),
        }
    }

    Ok(BinaryTableStats::new(
        min_builder.build_serialized(),
        max_builder.build_serialized(),
        null_counts,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::BinaryRow;

    /// Empty stats must produce min/max bytes that the Java side's
    /// `SerializationUtils.deserializeBinaryRow` accepts: at minimum a 4-byte BE
    /// arity prefix. A bare `Vec::new()` would trigger `BufferUnderflowException`
    /// when Spark/Flink read manifests written for a non-partitioned table.
    #[test]
    fn empty_stats_carries_arity_prefix_parseable_by_reader() {
        let stats = BinaryTableStats::empty();
        assert!(
            stats.min_values().len() >= 4,
            "min_values must contain at least the 4-byte arity prefix"
        );
        assert!(
            stats.max_values().len() >= 4,
            "max_values must contain at least the 4-byte arity prefix"
        );
        assert!(
            stats.null_counts().is_empty(),
            "null_counts stays empty so the Java reader short-circuits to EMPTY_STATS"
        );

        // Round-trip through the same parser the Java reader uses (4-byte BE arity).
        let min_row = BinaryRow::from_serialized_bytes(stats.min_values())
            .expect("min_values must decode as a BinaryRow");
        let max_row = BinaryRow::from_serialized_bytes(stats.max_values())
            .expect("max_values must decode as a BinaryRow");
        assert_eq!(min_row.arity(), 0);
        assert_eq!(max_row.arity(), 0);
    }

    #[test]
    fn simple_stats_row_data_round_trips() {
        let stats = BinaryTableStats::empty();
        let bytes = stats.to_simple_stats_row_data();
        let back = BinaryTableStats::from_simple_stats_row_data(&bytes).unwrap();
        assert_eq!(back.to_simple_stats_row_data(), bytes);
    }
}
