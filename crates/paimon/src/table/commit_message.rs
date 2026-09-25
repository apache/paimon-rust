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

use super::source::{read_i32, read_i64, take};
use crate::spec::{BinaryRow, DataFileMeta, DataFileMetaRowLayout, IndexFileMeta};
use std::collections::HashMap;

/// Current Java `CommitMessageSerializer` body version. The version is carried
/// by an enclosing serializer, not embedded in the body.
pub const COMMIT_MESSAGE_SERIALIZER_VERSION: i32 = 14;

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn write_rows<T>(
    out: &mut Vec<u8>,
    values: &[T],
    encode: impl Fn(&T) -> crate::Result<Vec<u8>>,
) -> crate::Result<()> {
    out.extend_from_slice(&(values.len() as i32).to_be_bytes());
    for value in values {
        let row = encode(value)?;
        out.extend_from_slice(&(row.len() as i32).to_be_bytes());
        out.extend_from_slice(&row);
    }
    Ok(())
}

fn read_rows<T>(
    cur: &mut &[u8],
    decode: impl Fn(&[u8]) -> crate::Result<T>,
) -> crate::Result<Vec<T>> {
    let count = read_i32(cur)?;
    if count < 0 || count as usize > cur.len() / 4 {
        return Err(invalid(format!(
            "invalid CommitMessage list count: {count}"
        )));
    }
    let mut values = Vec::new();
    for _ in 0..count {
        let length = read_i32(cur)?;
        if length < 0 {
            return Err(invalid(format!(
                "negative CommitMessage row length: {length}"
            )));
        }
        values.push(decode(take(cur, length as usize)?)?);
    }
    Ok(values)
}

/// A commit message representing new files to be committed for a specific partition and bucket.
///
/// Reference: [org.apache.paimon.table.sink.CommitMessage](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageImpl.java)
#[derive(Debug, Clone)]
pub struct CommitMessage {
    /// A staged Format Table file. Format Tables have no manifest or snapshot;
    /// this message is published by the Format Table committer instead.
    pub(crate) format_file: Option<FormatFileCommit>,
    /// Binary row bytes for the partition.
    pub partition: Vec<u8>,
    /// Bucket id.
    pub bucket: i32,
    /// Per-partition bucket count for fixed-bucket postpone writes.
    pub total_buckets: Option<i32>,
    /// New data files to be added.
    pub new_files: Vec<DataFileMeta>,
    /// Snapshot id from which state-dependent write conflicts should be checked.
    pub check_from_snapshot: Option<i64>,
    /// New changelog files to be added.
    pub new_changelog_files: Vec<DataFileMeta>,
    /// New index files to be added (used by dynamic bucket mode).
    pub new_index_files: Vec<IndexFileMeta>,
    /// Index files to be removed from the current index manifest.
    pub deleted_index_files: Vec<IndexFileMeta>,
    /// Files to be deleted (copy-on-write rewrite: old files replaced by new_files).
    pub deleted_files: Vec<DataFileMeta>,
    /// Files removed by compaction (Java's separate compact increment).
    pub compact_before: Vec<DataFileMeta>,
    /// Files produced by compaction.
    pub compact_after: Vec<DataFileMeta>,
    pub compact_changelog_files: Vec<DataFileMeta>,
    pub compact_new_index_files: Vec<IndexFileMeta>,
    pub compact_deleted_index_files: Vec<IndexFileMeta>,
    fixed_bucket_overwrite: bool,
}

impl CommitMessage {
    pub fn new(partition: Vec<u8>, bucket: i32, new_files: Vec<DataFileMeta>) -> Self {
        Self {
            format_file: None,
            partition,
            bucket,
            total_buckets: None,
            new_files,
            check_from_snapshot: None,
            new_changelog_files: Vec::new(),
            new_index_files: Vec::new(),
            deleted_index_files: Vec::new(),
            deleted_files: Vec::new(),
            compact_before: Vec::new(),
            compact_after: Vec::new(),
            compact_changelog_files: Vec::new(),
            compact_new_index_files: Vec::new(),
            compact_deleted_index_files: Vec::new(),
            fixed_bucket_overwrite: false,
        }
    }

    /// Supply the overwrite operation when restoring a message from Java's wire
    /// format, which does not encode this flag. Use the target committer's mode.
    pub fn mark_fixed_bucket_overwrite(&mut self) {
        self.fixed_bucket_overwrite = true;
    }

    pub(crate) fn is_fixed_bucket_overwrite(&self) -> bool {
        self.fixed_bucket_overwrite
    }

    /// Write the unframed Java v14 `CommitMessageSerializer.serialize` body.
    pub fn serialize(&self) -> crate::Result<Vec<u8>> {
        if self.format_file.is_some() {
            return Err(crate::Error::Unsupported {
                message: "Format Table two-phase file messages use a different Java serializer"
                    .into(),
            });
        }
        let mut out = Vec::new();
        // The partition normally is SerializationUtils.serializeBinaryRow:
        // i32 arity followed by a raw BinaryRow. Internal unpartitioned writers
        // may still use an empty Vec, which represents BinaryRow.EMPTY_ROW.
        let partition = if self.partition.is_empty() {
            BinaryRow::new(0).to_serialized_bytes()
        } else {
            BinaryRow::from_serialized_bytes(&self.partition)?;
            self.partition.clone()
        };
        out.extend_from_slice(&(partition.len() as i32).to_be_bytes());
        out.extend_from_slice(&partition);
        out.extend_from_slice(&self.bucket.to_be_bytes());
        out.push(u8::from(self.total_buckets.is_some()));
        if let Some(value) = self.total_buckets {
            out.extend_from_slice(&value.to_be_bytes());
        }
        for files in [
            &self.new_files,
            &self.deleted_files,
            &self.new_changelog_files,
        ] {
            write_rows(&mut out, files, DataFileMeta::to_serialized_row_data)?;
        }
        for files in [&self.new_index_files, &self.deleted_index_files] {
            write_rows(&mut out, files, IndexFileMeta::to_serialized_row_data)?;
        }
        for files in [
            &self.compact_before,
            &self.compact_after,
            &self.compact_changelog_files,
        ] {
            write_rows(&mut out, files, DataFileMeta::to_serialized_row_data)?;
        }
        for files in [
            &self.compact_new_index_files,
            &self.compact_deleted_index_files,
        ] {
            write_rows(&mut out, files, IndexFileMeta::to_serialized_row_data)?;
        }
        out.push(u8::from(self.check_from_snapshot.is_some()));
        if let Some(value) = self.check_from_snapshot {
            out.extend_from_slice(&value.to_be_bytes());
        }
        Ok(out)
    }

    /// Decode an unframed Java `CommitMessageSerializer` v14 body.
    pub fn deserialize(version: i32, bytes: &[u8]) -> crate::Result<Self> {
        if version != COMMIT_MESSAGE_SERIALIZER_VERSION {
            return Err(crate::Error::Unsupported {
                message: format!("CommitMessage serializer version {version} is not supported"),
            });
        }
        let mut cur = bytes;
        let partition_len = read_i32(&mut cur)?;
        if partition_len < 0 {
            return Err(invalid("negative CommitMessage partition length"));
        }
        let partition = take(&mut cur, partition_len as usize)?.to_vec();
        BinaryRow::from_serialized_bytes(&partition)?;
        let bucket = read_i32(&mut cur)?;
        let total_buckets = match take(&mut cur, 1)?[0] {
            0 => None,
            1 => Some(read_i32(&mut cur)?),
            value => return Err(invalid(format!("invalid totalBuckets flag: {value}"))),
        };
        let data_file = |raw: &[u8]| {
            DataFileMeta::from_serialized_row_data(raw, DataFileMetaRowLayout::CURRENT)
        };
        let new_files = read_rows(&mut cur, data_file)?;
        let deleted_files = read_rows(&mut cur, data_file)?;
        let new_changelog_files = read_rows(&mut cur, data_file)?;
        let new_index_files = read_rows(&mut cur, IndexFileMeta::from_serialized_row_data)?;
        let deleted_index_files = read_rows(&mut cur, IndexFileMeta::from_serialized_row_data)?;
        let compact_before = read_rows(&mut cur, data_file)?;
        let compact_after = read_rows(&mut cur, data_file)?;
        let compact_changelog_files = read_rows(&mut cur, data_file)?;
        let compact_new_index_files = read_rows(&mut cur, IndexFileMeta::from_serialized_row_data)?;
        let compact_deleted_index_files =
            read_rows(&mut cur, IndexFileMeta::from_serialized_row_data)?;
        let check_from_snapshot = match take(&mut cur, 1)?[0] {
            0 => None,
            1 => Some(read_i64(&mut cur)?),
            value => return Err(invalid(format!("invalid checkFromSnapshot flag: {value}"))),
        };
        if !cur.is_empty() {
            return Err(invalid(format!(
                "{} trailing bytes after CommitMessage",
                cur.len()
            )));
        }
        Ok(Self {
            format_file: None,
            partition,
            bucket,
            total_buckets,
            new_files,
            deleted_files,
            check_from_snapshot,
            new_changelog_files,
            new_index_files,
            deleted_index_files,
            compact_before,
            compact_after,
            compact_changelog_files,
            compact_new_index_files,
            compact_deleted_index_files,
            fixed_bucket_overwrite: false,
        })
    }

    /// Import a message for the fixed-bucket overwrite commit path. Java's
    /// wire format does not encode the commit operation, so the caller must
    /// supply that context when restoring a prepared overwrite message.
    pub fn deserialize_for_fixed_bucket_overwrite(
        version: i32,
        bytes: &[u8],
    ) -> crate::Result<Self> {
        let mut message = Self::deserialize(version, bytes)?;
        message.mark_fixed_bucket_overwrite();
        Ok(message)
    }
}

/// A file prepared below `_temporary`, awaiting publish into its partition.
#[derive(Debug, Clone)]
pub(crate) struct FormatFileCommit {
    pub staged_path: String,
    pub target_path: String,
    pub partition: HashMap<String, String>,
    pub record_count: i64,
    pub file_size: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    // Produced by Java CommitMessageSerializer v14 with BinaryRow.EMPTY_ROW,
    // bucket 3, checkFromSnapshot 7. The second message also has totalBuckets
    // 5 and one data-increment IndexFileMeta("I", "index", 9, 2).
    const EMPTY: &str = "AAAADAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABw==";
    const INDEXED: &str = "AAAADAAAAAAAAAAAAAAAAAAAAAMBAAAABQAAAAAAAAAAAAAAAAAAAAEAAABAAHAAAAAAAABJAAAAAAAAgWluZGV4AACFCQAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABw==";
    const RICH_INDEXED: &str = "AAAADAAAAAAAAAAAAAAAAAAAAAMBAAAABQAAAAAAAAAAAAAAAAAAAAEAAADgAAAAAAAAAABEVgAAAAAAgmluZGV4AACFCQAAAAAAAAACAAAAAAAAAEgAAABAAAAACwAAAIgAAABIAAAAmAAAAAEAAAAAAAAAOAAAABAAAAAAAAAAAAAAAAwAAAAoAAAAAwAAAAAAAAAMAAAAAAAAAAIAAAAAAAAAZGF0YS5wYXJxdWV0AAAAAGZpbGU6L2luZGV4AAAAAAAAAAAAAAAAAAoAAAAAAAAAFAAAAAAAAAADAAAAAAAAABAAAAA4AAAAAQIAAAAAAIIDAAAAAAAAgQIAAAAAAAAAAQAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAc=";

    #[test]
    fn java_v14_golden_round_trip() {
        for golden in [EMPTY, INDEXED, RICH_INDEXED] {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(golden)
                .unwrap();
            let message =
                CommitMessage::deserialize(COMMIT_MESSAGE_SERIALIZER_VERSION, &bytes).unwrap();
            assert_eq!(message.bucket, 3);
            assert_eq!(message.check_from_snapshot, Some(7));
            assert_eq!(message.serialize().unwrap(), bytes);
        }
        let indexed = base64::engine::general_purpose::STANDARD
            .decode(INDEXED)
            .unwrap();
        let message = CommitMessage::deserialize(14, &indexed).unwrap();
        assert_eq!(message.total_buckets, Some(5));
        assert_eq!(message.new_index_files[0].file_name, "index");
        let overwrite =
            CommitMessage::deserialize_for_fixed_bucket_overwrite(14, &indexed).unwrap();
        assert!(overwrite.is_fixed_bucket_overwrite());
        let mut empty_partition = CommitMessage::new(Vec::new(), 3, Vec::new());
        empty_partition.check_from_snapshot = Some(7);
        let empty = base64::engine::general_purpose::STANDARD
            .decode(EMPTY)
            .unwrap();
        assert_eq!(empty_partition.serialize().unwrap(), empty);
        let rich = base64::engine::general_purpose::STANDARD
            .decode(RICH_INDEXED)
            .unwrap();
        let message = CommitMessage::deserialize(14, &rich).unwrap();
        let index = &message.new_index_files[0];
        assert_eq!(index.external_path.as_deref(), Some("file:/index"));
        assert_eq!(index.deletion_vectors_ranges.as_ref().unwrap().len(), 1);
        assert_eq!(
            index.global_index_meta.as_ref().unwrap().extra_field_ids,
            Some(vec![1, 4])
        );
    }

    #[test]
    fn rejects_truncated_or_unsupported_message() {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(EMPTY)
            .unwrap();
        assert!(CommitMessage::deserialize(13, &bytes).is_err());
        assert!(CommitMessage::deserialize(14, &bytes[..bytes.len() - 1]).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(CommitMessage::deserialize(14, &trailing).is_err());
    }
}
