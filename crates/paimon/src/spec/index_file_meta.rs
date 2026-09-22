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

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use crate::spec::{
    deserialize_binary_array_int, deserialize_binary_array_rows, BinaryRow, BinaryRowBuilder,
};
use indexmap::IndexMap;

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn round_to_word(size: usize) -> usize {
    (size + 7) & !7
}

fn array_header(size: usize) -> usize {
    4 + size.div_ceil(32) * 4
}

fn serialize_int_array(values: &[i32]) -> Vec<u8> {
    let header = array_header(values.len());
    let mut data = vec![0; round_to_word(header + values.len() * 4)];
    data[..4].copy_from_slice(&(values.len() as i32).to_le_bytes());
    for (i, value) in values.iter().enumerate() {
        data[header + i * 4..header + i * 4 + 4].copy_from_slice(&value.to_le_bytes());
    }
    data
}

fn serialize_row_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let header = array_header(rows.len());
    let mut data = vec![0; round_to_word(header + rows.len() * 8)];
    data[..4].copy_from_slice(&(rows.len() as i32).to_le_bytes());
    for (i, row) in rows.iter().enumerate() {
        let offset = data.len();
        data.extend_from_slice(row);
        data.resize(data.len() + round_to_word(row.len()) - row.len(), 0);
        let slot = ((offset as u64) << 32) | row.len() as u64;
        data[header + i * 8..header + i * 8 + 8].copy_from_slice(&slot.to_le_bytes());
    }
    data
}

fn row_from_data(data: &[u8], arity: i32) -> crate::Result<BinaryRow> {
    if data.len() < BinaryRow::cal_fix_part_size_in_bytes(arity) as usize {
        return Err(invalid(format!(
            "IndexFileMeta nested row is too short for arity {arity}"
        )));
    }
    Ok(BinaryRow::from_bytes(arity, data.to_vec()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionVectorMeta {
    pub offset: i32,
    pub length: i32,
    pub cardinality: Option<i64>,
}

/// Metadata for a global index entry within an index file.
///
/// Reference: [org.apache.paimon.index.GlobalIndexMeta](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/index/GlobalIndexMeta.java)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalIndexMeta {
    #[serde(rename = "_ROW_RANGE_START")]
    pub row_range_start: i64,

    #[serde(rename = "_ROW_RANGE_END")]
    pub row_range_end: i64,

    #[serde(rename = "_INDEX_FIELD_ID")]
    pub index_field_id: i32,

    #[serde(default, rename = "_EXTRA_FIELD_IDS")]
    pub extra_field_ids: Option<Vec<i32>>,

    #[serde(default, rename = "_INDEX_META", with = "serde_bytes")]
    pub index_meta: Option<Vec<u8>>,

    #[serde(default, rename = "_SOURCE_META", with = "serde_bytes")]
    pub source_meta: Option<Vec<u8>>,
}

/// Metadata of index file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/index/IndexFileMeta.java>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFileMeta {
    #[serde(rename = "_INDEX_TYPE")]
    pub index_type: String,

    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,

    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,

    #[serde(rename = "_ROW_COUNT")]
    pub row_count: i64,

    // use Indexmap to ensure the order of deletion_vectors_ranges is consistent.
    #[serde(
        default,
        with = "map_serde",
        rename = "_DELETIONS_VECTORS_RANGES",
        alias = "_DELETION_VECTORS_RANGES"
    )]
    pub deletion_vectors_ranges: Option<IndexMap<String, DeletionVectorMeta>>,

    /// Absolute path of an externally-stored index file. `None` when the file
    /// lives under the table's index directory (or bucket data-file directory).
    #[serde(
        default,
        rename = "_EXTERNAL_PATH",
        skip_serializing_if = "Option::is_none"
    )]
    pub external_path: Option<String>,

    #[serde(
        default,
        rename = "_GLOBAL_INDEX",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_index_meta: Option<GlobalIndexMeta>,
}

impl IndexFileMeta {
    /// Java `IndexFileMetaSerializer` row body, without its i32 length prefix.
    pub fn to_serialized_row_data(&self) -> crate::Result<Vec<u8>> {
        let mut row = BinaryRowBuilder::new(7);
        row.write_bytes(0, self.index_type.as_bytes());
        row.write_bytes(1, self.file_name.as_bytes());
        row.write_long(2, self.file_size);
        row.write_long(3, self.row_count);
        if let Some(ranges) = &self.deletion_vectors_ranges {
            let rows = ranges
                .iter()
                .map(|(name, range)| {
                    let mut dv = BinaryRowBuilder::new(4);
                    dv.write_bytes(0, name.as_bytes());
                    dv.write_int(1, range.offset);
                    dv.write_int(2, range.length);
                    match range.cardinality {
                        Some(value) => dv.write_long(3, value),
                        None => dv.set_null_at(3),
                    }
                    dv.build_row_data()
                })
                .collect::<Vec<_>>();
            row.write_bytes(4, &serialize_row_array(&rows));
        } else {
            row.set_null_at(4);
        }
        match &self.external_path {
            Some(value) => row.write_bytes(5, value.as_bytes()),
            None => row.set_null_at(5),
        }
        if let Some(meta) = &self.global_index_meta {
            let mut global = BinaryRowBuilder::new(6);
            global.write_long(0, meta.row_range_start);
            global.write_long(1, meta.row_range_end);
            global.write_int(2, meta.index_field_id);
            match &meta.extra_field_ids {
                Some(values) => global.write_bytes(3, &serialize_int_array(values)),
                None => global.set_null_at(3),
            }
            match &meta.index_meta {
                Some(value) => global.write_bytes(4, value),
                None => global.set_null_at(4),
            }
            match &meta.source_meta {
                Some(value) => global.write_bytes(5, value),
                None => global.set_null_at(5),
            }
            row.write_bytes(6, &global.build_row_data());
        } else {
            row.set_null_at(6);
        }
        Ok(row.build_row_data())
    }

    /// Reverse of `to_serialized_row_data` for the current Java row schema.
    pub fn from_serialized_row_data(data: &[u8]) -> crate::Result<Self> {
        let row = row_from_data(data, 7)?;
        let name = |pos| -> crate::Result<String> {
            String::from_utf8(row.get_binary(pos)?.to_vec())
                .map_err(|_| invalid(format!("IndexFileMeta field {pos} is not UTF-8")))
        };
        let ranges = if row.is_null_at(4) {
            None
        } else {
            let mut ranges = IndexMap::new();
            for raw in deserialize_binary_array_rows(row.get_binary(4)?)? {
                let dv = row_from_data(raw, 4)?;
                let name = String::from_utf8(dv.get_binary(0)?.to_vec())
                    .map_err(|_| invalid("deletion vector file name is not UTF-8"))?;
                ranges.insert(
                    name,
                    DeletionVectorMeta {
                        offset: dv.get_int(1)?,
                        length: dv.get_int(2)?,
                        cardinality: if dv.is_null_at(3) {
                            None
                        } else {
                            Some(dv.get_long(3)?)
                        },
                    },
                );
            }
            Some(ranges)
        };
        let external_path = if row.is_null_at(5) {
            None
        } else {
            Some(name(5)?)
        };
        let global_index_meta = if row.is_null_at(6) {
            None
        } else {
            let global = row_from_data(row.get_binary(6)?, 6)?;
            Some(GlobalIndexMeta {
                row_range_start: global.get_long(0)?,
                row_range_end: global.get_long(1)?,
                index_field_id: global.get_int(2)?,
                extra_field_ids: if global.is_null_at(3) {
                    None
                } else {
                    Some(deserialize_binary_array_int(global.get_binary(3)?)?)
                },
                index_meta: if global.is_null_at(4) {
                    None
                } else {
                    Some(global.get_binary(4)?.to_vec())
                },
                source_meta: if global.is_null_at(5) {
                    None
                } else {
                    Some(global.get_binary(5)?.to_vec())
                },
            })
        };
        Ok(Self {
            index_type: name(0)?,
            file_name: name(1)?,
            file_size: row.get_long(2)?,
            row_count: row.get_long(3)?,
            deletion_vectors_ranges: ranges,
            external_path,
            global_index_meta,
        })
    }
}

impl Display for IndexFileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexFileMeta{{index_type={}, fileName={}, fileSize={}, rowCount={}, deletion_vectors_ranges={:?}, global_index_meta={:?}}}",
            self.index_type,
            self.file_name,
            self.file_size,
            self.row_count,
            self.deletion_vectors_ranges,
            self.global_index_meta,
        )
    }
}

mod map_serde {
    use indexmap::IndexMap;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::DeletionVectorMeta;

    #[derive(Deserialize, Serialize)]
    struct Temp {
        f0: String,
        f1: i32,
        f2: i32,
        #[serde(default, rename = "_CARDINALITY")]
        cardinality: Option<i64>,
    }

    pub fn serialize<S>(
        data: &Option<IndexMap<String, DeletionVectorMeta>>,
        s: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match data {
            None => s.serialize_none(),
            Some(d) => s.collect_seq(d.iter().map(|(path, meta)| Temp {
                f0: path.clone(),
                f1: meta.offset,
                f2: meta.length,
                cardinality: meta.cardinality,
            })),
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<IndexMap<String, DeletionVectorMeta>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::deserialize(deserializer)? {
            None => Ok(None),
            Some::<Vec<Temp>>(s) => Ok(Some(
                s.into_iter()
                    .map(|t| {
                        (
                            t.f0,
                            DeletionVectorMeta {
                                offset: t.f1,
                                length: t.f2,
                                cardinality: t.cardinality,
                            },
                        )
                    })
                    .collect::<IndexMap<_, _>>(),
            )),
        }
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn current_java_row_round_trip_with_nested_metadata() {
        let mut ranges = IndexMap::new();
        ranges.insert(
            "data.parquet".into(),
            DeletionVectorMeta {
                offset: 3,
                length: 12,
                cardinality: Some(2),
            },
        );
        let original = IndexFileMeta {
            index_type: "DV".into(),
            file_name: "index".into(),
            file_size: 9,
            row_count: 2,
            deletion_vectors_ranges: Some(ranges),
            external_path: Some("file:/index".into()),
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 10,
                row_range_end: 20,
                index_field_id: 3,
                extra_field_ids: Some(vec![1, 4]),
                index_meta: Some(vec![1, 2]),
                source_meta: Some(vec![3]),
            }),
        };
        let bytes = original.to_serialized_row_data().unwrap();
        assert_eq!(
            IndexFileMeta::from_serialized_row_data(&bytes).unwrap(),
            original
        );
    }
}
