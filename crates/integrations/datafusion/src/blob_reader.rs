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

use std::sync::{Arc, RwLock};

use paimon::io::FileIO;

#[derive(Clone, Debug)]
struct BlobFileIO {
    prefix: String,
    file_io: FileIO,
}

/// Session-scoped registry of Paimon [`FileIO`] instances for blob and external file reads.
/// Blob resolution uses table routes; only external scans include catalog fallbacks.
#[derive(Clone, Debug, Default)]
pub struct BlobReaderRegistry {
    readers: Arc<RwLock<Vec<BlobFileIO>>>,
    // Catalog roots are available only to explicit external-file reads.
    catalog_readers: Arc<RwLock<Vec<BlobFileIO>>>,
}

fn allows_backslash_boundary(location: &str) -> bool {
    let bytes = location.as_bytes();
    location.starts_with("file:/")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/'))
}

fn matches_location(uri: &str, location: &str) -> bool {
    uri == location
        || uri.strip_prefix(location).is_some_and(|suffix| {
            location.ends_with('/')
                || suffix.starts_with('/')
                || (allows_backslash_boundary(location)
                    && (location.ends_with('\\') || suffix.starts_with('\\')))
        })
}

impl BlobReaderRegistry {
    pub fn register(&self, prefix: impl Into<String>, file_io: FileIO) {
        let prefix = prefix.into();
        let mut readers = self.readers.write().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = readers.iter_mut().find(|reader| reader.prefix == prefix) {
            existing.file_io = file_io;
            return;
        }
        readers.push(BlobFileIO { prefix, file_io });
    }

    pub fn register_if_absent(&self, prefix: impl Into<String>, file_io: FileIO) {
        let prefix = prefix.into();
        let mut readers = self.readers.write().unwrap_or_else(|e| e.into_inner());
        if readers.iter().any(|reader| reader.prefix == prefix) {
            return;
        }
        readers.push(BlobFileIO { prefix, file_io });
    }

    fn register_location(readers: &RwLock<Vec<BlobFileIO>>, prefix: &str, file_io: FileIO) {
        let mut prefixes = vec![prefix.to_string()];
        if let Ok(url) = datafusion::datasource::listing::ListingTableUrl::parse(prefix) {
            let url: &url::Url = url.as_ref();
            if let Ok(path) = url
                .to_file_path()
                .and_then(|path| std::fs::canonicalize(path).map_err(|_| ()))
            {
                prefixes.push(path.to_string_lossy().to_string());
            }
        }
        let mut readers = readers.write().unwrap_or_else(|e| e.into_inner());
        for prefix in prefixes {
            if !readers.iter().any(|reader| reader.prefix == prefix) {
                readers.push(BlobFileIO {
                    prefix,
                    file_io: file_io.clone(),
                });
            }
        }
    }

    pub(crate) fn register_location_if_absent(&self, prefix: &str, file_io: FileIO) {
        Self::register_location(&self.readers, prefix, file_io);
    }

    pub(crate) fn register_catalog_location(&self, prefix: &str, file_io: FileIO) {
        Self::register_location(&self.catalog_readers, prefix, file_io);
    }

    /// Compare equivalent URI/path spellings before choosing the closest route.
    pub(crate) fn resolve_paths(&self, paths: &[String]) -> Option<FileIO> {
        let readers = self.readers.read().unwrap_or_else(|e| e.into_inner());
        let catalogs = self
            .catalog_readers
            .read()
            .unwrap_or_else(|e| e.into_inner());
        readers
            .iter()
            .map(|reader| (0, reader))
            .chain(catalogs.iter().map(|reader| (1, reader)))
            .filter_map(|(kind, reader)| {
                let remaining = paths
                    .iter()
                    .filter(|path| matches_location(path, &reader.prefix))
                    .map(|path| {
                        let suffix = &path[reader.prefix.len()..];
                        suffix
                            .trim_start_matches(|c| {
                                c == '/' || (c == '\\' && allows_backslash_boundary(&reader.prefix))
                            })
                            .len()
                    })
                    .min()?;
                Some((remaining, kind, reader))
            })
            .min_by_key(|(remaining, kind, reader)| {
                (*remaining, *kind, std::cmp::Reverse(reader.prefix.len()))
            })
            .map(|(_, _, reader)| reader.file_io.clone())
    }

    pub fn resolve(&self, uri: &str) -> Option<FileIO> {
        let readers = self.readers.read().unwrap_or_else(|e| e.into_inner());
        readers
            .iter()
            .filter(|reader| matches_location(uri, &reader.prefix))
            .max_by_key(|reader| reader.prefix.len())
            .map(|reader| reader.file_io.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use paimon::io::{FileIOBuilder, FileRead};
    use paimon::spec::BlobDescriptor;

    use super::*;

    fn memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    #[tokio::test]
    async fn catalog_file_io_is_an_external_read_fallback_only() {
        let uri = "memory:/warehouse/data";
        let catalog_io = memory_file_io();
        let table_io = memory_file_io();
        write_file(&catalog_io, uri, b"catalog").await;
        write_file(&table_io, uri, b"table").await;
        for catalog_prefix in ["memory:/warehouse", "memory:/warehouse/"] {
            for table_prefix in ["memory:/warehouse", "memory:/warehouse/"] {
                let registry = BlobReaderRegistry::default();
                registry.register_catalog_location(catalog_prefix, catalog_io.clone());
                assert!(registry.resolve(uri).is_none());
                let paths = vec![uri.to_string()];
                let selected = registry.resolve_paths(&paths).unwrap();
                assert_eq!(
                    selected
                        .new_input(uri)
                        .unwrap()
                        .read()
                        .await
                        .unwrap()
                        .as_ref(),
                    b"catalog"
                );
                registry.register_location_if_absent(table_prefix, table_io.clone());
                assert_resolves_to(&registry, uri, b"table").await;
                let selected = registry.resolve_paths(&paths).unwrap();
                assert_eq!(
                    selected
                        .new_input(uri)
                        .unwrap()
                        .read()
                        .await
                        .unwrap()
                        .as_ref(),
                    b"table"
                );
            }
        }
    }

    async fn write_file(file_io: &FileIO, uri: &str, contents: &[u8]) {
        file_io
            .new_output(uri)
            .unwrap()
            .write(contents.to_vec().into())
            .await
            .unwrap();
    }

    async fn assert_resolves_to(registry: &BlobReaderRegistry, uri: &str, expected: &[u8]) {
        let file_io = registry
            .resolve(uri)
            .unwrap_or_else(|| panic!("expected registered FileIO for {uri}"));
        let bytes = file_io.new_input(uri).unwrap().read().await.unwrap();
        assert_eq!(&bytes[..], expected);
    }

    #[test]
    fn resolves_only_same_location_or_descendants() {
        let cases = [
            ("memory:/foo", "memory:/foo", true),
            ("memory:/foo", "memory:/foo/blob.bin", true),
            ("memory:/foo", "memory:/foo/nested/blob.bin", true),
            ("memory:/foo", "memory:/foobar/private.bin", false),
            ("s3://bucket", "s3://bucket-other/blob.bin", false),
            ("s3://bucket/table", "gs://bucket/table/blob.bin", false),
            ("memory:/foo/", "memory:/foo/", true),
            ("memory:/foo/", "memory:/foo/blob.bin", true),
            ("memory:/foo/", "memory:/foo", false),
            (r"C:\warehouse\table", r"C:\warehouse\table\blob.bin", true),
            (
                r"C:\warehouse\table",
                r"C:\warehouse\table-other\blob.bin",
                false,
            ),
            (
                "C:\\warehouse\\table\\",
                r"C:\warehouse\table\blob.bin",
                true,
            ),
            ("C:\\warehouse\\table\\", r"C:\warehouse\table", false),
            (
                r"file:/C:\warehouse\table",
                r"file:/C:\warehouse\table\blob.bin",
                true,
            ),
            (
                r"file:/C:\warehouse\table",
                r"file:/C:\warehouse\table-other\blob.bin",
                false,
            ),
            ("s3://bucket/table", r"s3://bucket/table\blob.bin", false),
        ];

        for (location, uri, expected) in cases {
            let registry = BlobReaderRegistry::default();
            registry.register(location, memory_file_io());
            assert_eq!(
                registry.resolve(uri).is_some(),
                expected,
                "location={location}, uri={uri}"
            );
        }
    }

    #[tokio::test]
    async fn resolves_with_longest_valid_location() {
        let parent_file_io = memory_file_io();
        let child_file_io = memory_file_io();
        let registry = BlobReaderRegistry::default();
        registry.register("memory:/warehouse/db", parent_file_io.clone());
        registry.register("memory:/warehouse/db/foo", child_file_io.clone());

        let child_uri = "memory:/warehouse/db/foo/blob.bin";
        write_file(&parent_file_io, child_uri, b"parent").await;
        write_file(&child_file_io, child_uri, b"child").await;
        assert_resolves_to(&registry, child_uri, b"child").await;

        let sibling_uri = "memory:/warehouse/db/foobar/private.bin";
        write_file(&parent_file_io, sibling_uri, b"parent").await;
        write_file(&child_file_io, sibling_uri, b"child").await;
        assert_resolves_to(&registry, sibling_uri, b"parent").await;
    }

    #[tokio::test]
    async fn preserves_registration_semantics() {
        let uri = "memory:/warehouse/db/foo";
        let initial_file_io = memory_file_io();
        let replacement_file_io = memory_file_io();
        write_file(&initial_file_io, uri, b"initial").await;
        write_file(&replacement_file_io, uri, b"replacement").await;

        let registry = BlobReaderRegistry::default();
        registry.register(uri, initial_file_io.clone());
        registry.register(uri, replacement_file_io.clone());
        assert_resolves_to(&registry, uri, b"replacement").await;

        let registry = BlobReaderRegistry::default();
        registry.register(uri, initial_file_io);
        registry.register_if_absent(uri, replacement_file_io);
        assert_resolves_to(&registry, uri, b"initial").await;
    }

    #[tokio::test]
    async fn resolves_file_blob_descriptor_with_file_io() {
        let directory = tempfile::tempdir().unwrap();
        let blob_path = directory.path().join("blob.bin");
        fs::write(&blob_path, b"prefixpayloadsuffix").unwrap();

        let descriptor = BlobDescriptor::new(
            blob_path.to_string_lossy().to_string(),
            6,
            "payload".len() as i64,
        );
        let descriptor = BlobDescriptor::deserialize(&descriptor.serialize()).unwrap();

        let registry = BlobReaderRegistry::default();
        let file_io = FileIOBuilder::new("file").build().unwrap();
        registry.register(directory.path().to_string_lossy().to_string(), file_io);

        let resolved_file_io = registry
            .resolve(descriptor.uri())
            .expect("file blob descriptor should resolve to registered FileIO");
        let input = resolved_file_io.new_input(descriptor.uri()).unwrap();
        let reader = input.reader().await.unwrap();
        let start = descriptor.offset() as u64;
        let end = start + descriptor.length() as u64;
        let bytes = reader.read(start..end).await.unwrap();

        assert_eq!(&bytes[..], b"payload");
    }
}
