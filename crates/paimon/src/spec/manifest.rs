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

use crate::io::FileIO;
use crate::spec::avro::SchemaCache;
use crate::spec::manifest_entry::ManifestEntry;
use crate::spec::manifest_entry::MANIFEST_ENTRY_SCHEMA;
use crate::spec::FileKind;

use crate::Result;

/// Manifest file reader and writer.
///
/// A manifest file contains a list of ManifestEntry records in Avro format.
/// Each entry represents an addition or deletion of a data file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFile.java>
pub struct Manifest;

impl Manifest {
    /// Read manifest entries from a file.
    pub async fn read(file_io: &FileIO, path: &str) -> Result<Vec<ManifestEntry>> {
        let input_file = file_io.new_input(path)?;
        let content = input_file.read().await?;
        Self::read_from_bytes(&content)
    }

    /// Read manifest entries from bytes.
    fn read_from_bytes(bytes: &[u8]) -> Result<Vec<ManifestEntry>> {
        crate::spec::avro::from_avro_bytes_fast(bytes)
    }

    /// Read manifest entries with a lightweight filter on (kind, partition, bucket, total_buckets).
    /// Entries that fail the filter skip DataFileMeta decoding entirely.
    pub async fn read_filtered<F>(
        file_io: &FileIO,
        path: &str,
        cache: &mut SchemaCache,
        filter: &mut F,
    ) -> Result<Vec<ManifestEntry>>
    where
        F: FnMut(FileKind, &[u8], i32, i32) -> bool,
    {
        let input_file = file_io.new_input(path)?;
        let content = input_file.read().await?;
        crate::spec::avro::from_manifest_bytes_filtered(&content, cache, filter)
    }

    /// Write manifest entries to a file.
    pub async fn write(file_io: &FileIO, path: &str, entries: &[ManifestEntry]) -> Result<()> {
        Self::write_with_compression(
            file_io,
            path,
            entries,
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .await
    }

    /// Write manifest entries to a file with the configured Avro compression.
    pub async fn write_with_compression(
        file_io: &FileIO,
        path: &str,
        entries: &[ManifestEntry],
        compression: &str,
    ) -> Result<()> {
        let bytes = crate::spec::to_avro_bytes_with_compression(
            MANIFEST_ENTRY_SCHEMA,
            entries,
            compression,
        )?;
        let output = file_io.new_output(path)?;
        output.write(bytes::Bytes::from(bytes)).await
    }
}

/// Resolve a complete manifest set to its active ADD entries.
/// Return order is unspecified.
pub(crate) fn merge_active_entries(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    use std::collections::HashMap;

    use crate::spec::manifest_entry::Identifier;
    let mut map: HashMap<Identifier, ManifestEntry> = HashMap::new();
    for entry in entries {
        match entry.kind() {
            FileKind::Add => {
                map.insert(entry.identifier(), entry);
            }
            FileKind::Delete => {
                map.remove(&entry.identifier());
            }
        }
    }
    map.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;
    use crate::spec::manifest_common::FileKind;
    use std::env::current_dir;

    #[tokio::test]
    async fn test_read_manifest_from_file() {
        let workdir = current_dir().unwrap();
        let path =
            workdir.join("tests/fixtures/manifest/manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0");

        let file_io = FileIO::from_url("file://").unwrap().build().unwrap();
        let entries = Manifest::read(&file_io, path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        // verify manifest entry
        let t1 = &entries[0];
        assert_eq!(t1.kind(), &FileKind::Delete);
        assert_eq!(t1.bucket(), 1);

        let t2 = &entries[1];
        assert_eq!(t2.kind(), &FileKind::Add);
        assert_eq!(t2.bucket(), 2);
    }

    #[tokio::test]
    async fn test_fully_populated_entry_survives_write_and_read() {
        use crate::io::FileIOBuilder;
        use crate::spec::data_file::DataFileMeta;
        use crate::spec::stats::BinaryTableStats;

        let file = DataFileMeta {
            file_name: "data-0.parquet".into(),
            file_size: 1024,
            row_count: 10,
            min_key: vec![1, 2],
            max_key: vec![3, 4],
            key_stats: BinaryTableStats::new(vec![5], vec![6], vec![Some(0), None]),
            value_stats: BinaryTableStats::new(vec![7], vec![8], vec![Some(1)]),
            min_sequence_number: 1,
            max_sequence_number: 9,
            schema_id: 2,
            level: 3,
            extra_files: vec!["data-0.index".into()],
            creation_time: chrono::DateTime::from_timestamp_millis(1_700_000_000_123),
            delete_row_count: Some(4),
            embedded_index: Some(vec![9, 9]),
            file_source: Some(1),
            value_stats_cols: Some(vec!["id".into()]),
            external_path: Some("oss://bucket/data-0.parquet".into()),
            first_row_id: Some(100),
            write_cols: Some(vec!["id".into(), "name".into()]),
            column_max_sequence_numbers: Some(vec![7, 8]),
        };
        let mut partition = crate::spec::BinaryRowBuilder::new(1);
        partition.write_int(0, 7);
        let entry = ManifestEntry::new(FileKind::Add, partition.build_serialized(), 5, 8, file, 2);
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/manifest-round-trip/manifest-0";
        Manifest::write(&file_io, path, std::slice::from_ref(&entry))
            .await
            .unwrap();
        assert_eq!(Manifest::read(&file_io, path).await.unwrap(), [entry]);
    }

    #[test]
    fn test_merge_active_entries_cancels_add_then_delete() {
        use crate::spec::data_file::DataFileMeta;
        use crate::spec::stats::BinaryTableStats;
        use crate::spec::ManifestEntry;

        fn entry(kind: FileKind, file_name: &str, level: i32) -> ManifestEntry {
            let stats = BinaryTableStats::empty();
            let file = DataFileMeta {
                file_name: file_name.to_string(),
                file_size: 100,
                row_count: 10,
                min_key: vec![],
                max_key: vec![],
                key_stats: stats.clone(),
                value_stats: stats,
                min_sequence_number: 0,
                max_sequence_number: 0,
                schema_id: 0,
                level,
                extra_files: vec![],
                creation_time: None,
                delete_row_count: None,
                embedded_index: None,
                file_source: None,
                value_stats_cols: None,
                external_path: None,
                first_row_id: None,
                write_cols: None,
                column_max_sequence_numbers: None,
            };
            ManifestEntry::new(kind, vec![], 0, 1, file, 2)
        }

        let cancelled = merge_active_entries(vec![
            entry(FileKind::Add, "f.parquet", 0),
            entry(FileKind::Delete, "f.parquet", 0),
        ]);
        assert!(cancelled.is_empty());

        let two_levels = merge_active_entries(vec![
            entry(FileKind::Add, "f.parquet", 0),
            entry(FileKind::Add, "f.parquet", 1),
        ]);
        assert_eq!(two_levels.len(), 2);

        let compacted = merge_active_entries(vec![
            entry(FileKind::Add, "f.parquet", 0),
            entry(FileKind::Delete, "f.parquet", 0),
            entry(FileKind::Add, "f.parquet", 1),
        ]);
        assert_eq!(compacted.len(), 1);
        assert_eq!(compacted[0].file().level, 1);
    }
}
