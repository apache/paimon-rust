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

//! Resolves the on-disk path of an index file recorded in an index manifest.
//!
//! An index file's location depends on how it was written:
//!   * an externally-stored file records its absolute path in the manifest and
//!     is read from exactly that path, wherever it lives;
//!   * a global index (data-evolution, row-id space) always lives under the
//!     table's `index/` directory;
//!   * a source-backed primary-key index lives beside its bucket's data files
//!     when the table stores index files in the data-file directory, and under
//!     the table `index/` directory otherwise.
//!
//! The bucket-local fallback resolves against the bucket directory captured on
//! the data split, never a path rebuilt from the table root, so custom data
//! directories and postpone-bucket layouts are honored.
//!
//! A reader knows which of these it is asking for, because the mode follows the
//! index kind it reads. Cleanup after a failed commit does not — see
//! [`committed_index_file_path`].

use crate::spec::IndexFileMeta;

const INDEX_DIR: &str = "index";

/// How to resolve an index file that carries no explicit external path.
pub(crate) enum IndexFileLocation<'a> {
    /// Global index files (data-evolution row-id space) always live under the
    /// table's `index/` directory.
    Global { table_path: &'a str },
    /// Source-backed primary-key index files live beside their bucket's data
    /// files when the table keeps index files in the data-file directory, and
    /// under the table `index/` directory otherwise.
    BucketLocal {
        table_path: &'a str,
        /// The bucket directory captured from the data split (e.g.
        /// `warehouse/db/tbl/bucket-3`). Used directly, not rebuilt.
        bucket_path: &'a str,
        /// Whether the table stores index files in the data-file (bucket)
        /// directory (`index-file-in-data-file-dir`).
        index_file_in_data_file_dir: bool,
    },
}

impl IndexFileLocation<'_> {
    /// The directory a file with no explicit external path resolves into. A
    /// writer needs it to create the directory it is about to write into, so it
    /// must come from here rather than be re-derived from the resolved path.
    pub(crate) fn directory(&self) -> String {
        match self {
            IndexFileLocation::Global { table_path } => format!("{table_path}/{INDEX_DIR}"),
            IndexFileLocation::BucketLocal {
                table_path,
                bucket_path,
                index_file_in_data_file_dir,
            } => {
                if *index_file_in_data_file_dir {
                    (*bucket_path).to_string()
                } else {
                    format!("{table_path}/{INDEX_DIR}")
                }
            }
        }
    }

    /// Resolve the full path of `file_name`, honoring an explicit
    /// `external_path` when present.
    pub(crate) fn resolve(&self, file_name: &str, external_path: Option<&str>) -> String {
        match external_path {
            Some(external) => external.to_string(),
            None => format!("{}/{file_name}", self.directory()),
        }
    }
}

/// `"DEIX"` as a big-endian int, Java `DataEvolutionIndexSourceMeta`'s marker.
/// `PrimaryKeyIndexSourceMeta` starts with its own version instead, so the marker
/// tells the two apart — which is what Java added it for.
const DATA_EVOLUTION_SOURCE_META_MAGIC: &[u8; 4] = b"DEIX";

/// Whether an index file carried by a commit message is a global index file.
///
/// A `_GLOBAL_INDEX` on its own does not say — a source-backed primary-key index
/// carries one too. The source metadata does: Java marks its own with
/// `DataEvolutionIndexSourceMeta`'s magic, added for exactly this question, while
/// `PrimaryKeyIndexSourceMeta` starts with its own version. A deletion vector or
/// the dynamic-bucket hash index carries no `_GLOBAL_INDEX` at all.
///
/// Absent source metadata is global because the index builders in this crate
/// write it that way, and they are the only producers of the global index files
/// it commits. Java records that predate `_SOURCE_META` cannot reach here: a
/// commit message is built by this crate's writers, never decoded from Java.
fn is_global_index_file(file: &IndexFileMeta) -> bool {
    file.global_index_meta
        .as_ref()
        .is_some_and(|meta| match meta.source_meta.as_deref() {
            None => true,
            Some(source_meta) => source_meta.starts_with(DATA_EVOLUTION_SOURCE_META_MAGIC),
        })
}

/// Where an index file carried by a commit message was written.
///
/// Cleanup after a failed commit resolves index files this crate just wrote, and
/// they are not all one layout: a data-evolution index build is global, while a
/// deletion vector or the dynamic-bucket hash index is bucket-local. Each file
/// is therefore classified on its own rather than the layout being taken from
/// the message. Anything not recognizable as a global index file is bucket-local,
/// which is what Java `FileStoreCommitImpl.abort` assumes for every index file.
pub(crate) fn committed_index_file_path(
    table_path: &str,
    bucket_path: &str,
    index_file_in_data_file_dir: bool,
    file: &IndexFileMeta,
) -> String {
    let location = if is_global_index_file(file) {
        IndexFileLocation::Global { table_path }
    } else {
        IndexFileLocation::BucketLocal {
            table_path,
            bucket_path,
            index_file_in_data_file_dir,
        }
    };
    location.resolve(&file.file_name, file.external_path.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{GlobalIndexMeta, PrimaryKeyIndexSourceMeta};

    #[test]
    fn external_path_wins_over_every_mode() {
        let external = "s3://other-bucket/abs/idx-0";
        let global = IndexFileLocation::Global {
            table_path: "warehouse/db/tbl",
        };
        assert_eq!(global.resolve("idx-0", Some(external)), external);

        let bucket_local = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "warehouse/db/tbl/bucket-3",
            index_file_in_data_file_dir: true,
        };
        assert_eq!(bucket_local.resolve("idx-0", Some(external)), external);
    }

    #[test]
    fn global_uses_table_index_directory() {
        let loc = IndexFileLocation::Global {
            table_path: "warehouse/db/tbl",
        };
        assert_eq!(loc.resolve("idx-0", None), "warehouse/db/tbl/index/idx-0");
    }

    #[test]
    fn bucket_local_uses_bucket_directory_when_enabled() {
        let loc = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "warehouse/db/tbl/bucket-3",
            index_file_in_data_file_dir: true,
        };
        assert_eq!(
            loc.resolve("idx-0", None),
            "warehouse/db/tbl/bucket-3/idx-0"
        );
    }

    #[test]
    fn bucket_local_falls_back_to_table_index_when_disabled() {
        let loc = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "warehouse/db/tbl/bucket-3",
            index_file_in_data_file_dir: false,
        };
        assert_eq!(loc.resolve("idx-0", None), "warehouse/db/tbl/index/idx-0");
    }

    #[test]
    fn bucket_local_uses_captured_custom_bucket_path() {
        // A custom data directory / postpone-bucket layout must be honored via
        // the captured bucket path, not a path rebuilt from the table root.
        let loc = IndexFileLocation::BucketLocal {
            table_path: "warehouse/db/tbl",
            bucket_path: "s3://data-warehouse/custom/tbl/bucket-postpone",
            index_file_in_data_file_dir: true,
        };
        assert_eq!(
            loc.resolve("idx-0", None),
            "s3://data-warehouse/custom/tbl/bucket-postpone/idx-0"
        );
    }

    #[test]
    fn directory_is_the_parent_resolve_writes_into() {
        // A writer creates `directory()` and then writes `resolve()`; the two must
        // agree, or it creates one directory and writes into another.
        let locations = [
            IndexFileLocation::Global {
                table_path: "warehouse/db/tbl",
            },
            IndexFileLocation::BucketLocal {
                table_path: "warehouse/db/tbl",
                bucket_path: "warehouse/db/tbl/pt=1/bucket-3",
                index_file_in_data_file_dir: false,
            },
            IndexFileLocation::BucketLocal {
                table_path: "warehouse/db/tbl",
                bucket_path: "warehouse/db/tbl/pt=1/bucket-3",
                index_file_in_data_file_dir: true,
            },
        ];
        for loc in &locations {
            assert_eq!(
                loc.resolve("idx-0", None),
                format!("{}/idx-0", loc.directory())
            );
        }
    }

    /// A committed index file with the given `_GLOBAL_INDEX` and `_SOURCE_META`.
    fn committed_file(global_index_meta: Option<Option<Vec<u8>>>) -> IndexFileMeta {
        IndexFileMeta {
            index_type: "btree".to_string(),
            file_name: "idx-0".to_string(),
            file_size: 128,
            row_count: 1,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: global_index_meta.map(|source_meta| GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: 0,
                index_field_id: 0,
                extra_field_ids: None,
                index_meta: None,
                source_meta,
            }),
        }
    }

    #[test]
    fn missing_bucket_index_does_not_use_table_directory() {
        let dir = tempfile::tempdir().unwrap();
        let table_path = dir.path().to_str().unwrap();
        let bucket_path = format!("{table_path}/pt=1/bucket-7");
        let table_index = format!("{table_path}/index/idx-0");
        std::fs::create_dir_all(format!("{table_path}/index")).unwrap();
        std::fs::write(&table_index, b"wrong directory").unwrap();
        for index_type in ["DELETION_VECTORS", "HASH"] {
            let mut file = committed_file(None);
            file.index_type = index_type.to_string();
            let path = committed_index_file_path(table_path, &bucket_path, true, &file);
            assert_eq!(path, format!("{bucket_path}/idx-0"));
            assert_eq!(
                std::fs::read(path).unwrap_err().kind(),
                std::io::ErrorKind::NotFound
            );
            file.external_path = Some(table_index.clone());
            let path = committed_index_file_path(table_path, &bucket_path, true, &file);
            assert_eq!(std::fs::read(path).unwrap(), b"wrong directory");
        }
    }

    fn committed_path(file: &IndexFileMeta) -> String {
        committed_index_file_path(
            "warehouse/db/tbl",
            "warehouse/db/tbl/pt=1/bucket-3",
            true,
            file,
        )
    }

    /// Java `DataEvolutionIndexSourceMeta.serialize`: magic, version, scan
    /// snapshot id.
    fn data_evolution_source_meta(scan_snapshot_id: i64) -> Vec<u8> {
        let mut bytes = b"DEIX".to_vec();
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&scan_snapshot_id.to_be_bytes());
        bytes
    }

    /// Java `PrimaryKeyIndexSourceMeta.serialize`: version, data level, source
    /// count, then each source's `writeUTF` name and row count.
    fn primary_key_source_meta(data_level: i32, source_name: &str, row_count: i64) -> Vec<u8> {
        let mut bytes = 1i32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&data_level.to_be_bytes());
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&(source_name.len() as u16).to_be_bytes());
        bytes.extend_from_slice(source_name.as_bytes());
        bytes.extend_from_slice(&row_count.to_be_bytes());
        // A frame the classifier rejects has to be one a reader would accept,
        // otherwise the test only proves that garbage is not global.
        PrimaryKeyIndexSourceMeta::deserialize(&bytes).expect("a valid primary-key source frame");
        bytes
    }

    #[test]
    fn a_global_index_file_is_committed_under_the_table_index_directory() {
        // This crate's index builders leave `_SOURCE_META` empty, and Java marks
        // its own with `DataEvolutionIndexSourceMeta`'s `DEIX`. Both are global
        // even when the table keeps index files in the data-file directory.
        for source_meta in [None, Some(data_evolution_source_meta(7))] {
            let file = committed_file(Some(source_meta));
            assert_eq!(committed_path(&file), "warehouse/db/tbl/index/idx-0");
        }
    }

    #[test]
    fn a_source_backed_primary_key_index_file_is_committed_bucket_local() {
        // Primary-key source metadata starts with its version, never `DEIX`.
        let file = committed_file(Some(Some(primary_key_source_meta(1, "data-0.parquet", 3))));
        assert_eq!(
            committed_path(&file),
            "warehouse/db/tbl/pt=1/bucket-3/idx-0"
        );
    }

    #[test]
    fn only_a_whole_data_evolution_marker_makes_source_metadata_global() {
        // The marker is the whole four bytes, as in Java's length-checked
        // big-endian comparison: a shorter or partial prefix is not it. Java's own
        // marker test likewise looks no further than those four bytes, so a
        // truncated frame that still carries them stays global.
        for not_global in [vec![], b"D".to_vec(), b"DEI".to_vec(), b"XIED".to_vec()] {
            let file = committed_file(Some(Some(not_global.clone())));
            assert_eq!(
                committed_path(&file),
                "warehouse/db/tbl/pt=1/bucket-3/idx-0",
                "{not_global:?} does not carry the marker"
            );
        }
        let file = committed_file(Some(Some(b"DEIX".to_vec())));
        assert_eq!(committed_path(&file), "warehouse/db/tbl/index/idx-0");
    }

    #[test]
    fn an_index_file_without_global_index_meta_is_committed_bucket_local() {
        // Deletion vectors and the dynamic-bucket hash index carry no
        // `_GLOBAL_INDEX` at all.
        let file = committed_file(None);
        assert_eq!(
            committed_path(&file),
            "warehouse/db/tbl/pt=1/bucket-3/idx-0"
        );
    }

    #[test]
    fn a_committed_index_file_keeps_its_external_path_in_either_layout() {
        let external = "s3://other-bucket/abs/idx-0";
        for global_index_meta in [None, Some(None)] {
            let mut file = committed_file(global_index_meta);
            file.external_path = Some(external.to_string());
            assert_eq!(committed_path(&file), external);
        }
    }

    #[test]
    fn committed_index_files_share_one_directory_without_the_bucket_dir_option() {
        // Without the option every layout resolves under the table `index/`
        // directory, so classification cannot change the outcome.
        for global_index_meta in [
            None,
            Some(None),
            Some(Some(primary_key_source_meta(1, "data-0.parquet", 3))),
        ] {
            let file = committed_file(global_index_meta);
            assert_eq!(
                committed_index_file_path(
                    "warehouse/db/tbl",
                    "warehouse/db/tbl/pt=1/bucket-3",
                    false,
                    &file,
                ),
                "warehouse/db/tbl/index/idx-0"
            );
        }
    }
}
