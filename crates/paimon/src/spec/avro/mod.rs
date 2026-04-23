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

pub mod cursor;
pub mod decode;
pub(crate) mod decode_helpers;
mod index_manifest_entry_decode;
pub(crate) mod manifest_entry_decode;
mod manifest_file_meta_decode;
pub mod ocf;
pub mod schema;

use cursor::AvroCursor;
use decode::AvroRecordDecode;
use ocf::parse_ocf_streaming;
use schema::WriterSchema;
use std::sync::{Arc, RwLock};

/// Cache for parsed WriterSchemas, keyed by schema JSON string.
/// Same manifest type always produces the same schema JSON, so parsing
/// once and reusing across files within a scan saves repeated work.
/// Uses Vec instead of HashMap since Paimon tables typically have 1-2 distinct schemas.
pub struct SchemaCache {
    cache: Vec<(String, Arc<WriterSchema>)>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self { cache: Vec::new() }
    }

    pub fn get_or_parse(&mut self, schema_json: &str) -> crate::Result<Arc<WriterSchema>> {
        if let Some(cached) = self.cache.iter().find(|(k, _)| k == schema_json) {
            return Ok(Arc::clone(&cached.1));
        }
        let ws = Arc::new(WriterSchema::parse(schema_json)?);
        self.cache.push((schema_json.to_string(), Arc::clone(&ws)));
        Ok(ws)
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe schema cache for sharing across concurrent async tasks.
/// Wraps `SchemaCache` in `Arc<Mutex<_>>` so multiple tasks can reuse
/// the same parsed `WriterSchema` without re-parsing.
#[derive(Clone)]
pub struct SharedSchemaCache {
    inner: Arc<RwLock<SchemaCache>>,
}

impl SharedSchemaCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SchemaCache::new())),
        }
    }

    pub fn get_or_parse(&self, schema_json: &str) -> crate::Result<Arc<WriterSchema>> {
        // Fast path: read lock for cache hit
        {
            let cache = self.inner.read().unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = cache.cache.iter().find(|(k, _)| k == schema_json) {
                return Ok(Arc::clone(&cached.1));
            }
        }
        // Slow path: write lock for cache miss
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_parse(schema_json)
    }
}

impl Default for SharedSchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Read an Avro OCF file and decode records directly into `T`, bypassing
/// the intermediate `apache_avro::Value` representation.
pub fn from_avro_bytes_fast<T: AvroRecordDecode>(bytes: &[u8]) -> crate::Result<Vec<T>> {
    let mut cache = SchemaCache::new();
    from_avro_bytes_with_cache(bytes, &mut cache)
}

/// Same as `from_avro_bytes_fast` but reuses a `SchemaCache` across calls.
pub fn from_avro_bytes_with_cache<T: AvroRecordDecode>(
    bytes: &[u8],
    cache: &mut SchemaCache,
) -> crate::Result<Vec<T>> {
    let (header, mut block_iter) = parse_ocf_streaming(bytes)?;
    let writer_schema = cache.get_or_parse(&header.schema_json)?;

    let mut results = Vec::new();
    while let Some(block) = block_iter.next_block()? {
        results.reserve(block.object_count);
        let mut cursor = AvroCursor::new(&block.data);
        for _ in 0..block.object_count {
            let record = decode_top_level_record::<T>(&mut cursor, &writer_schema)?;
            results.push(record);
        }
    }

    Ok(results)
}

/// Decode ManifestEntry records from Avro OCF bytes with a lightweight filter.
///
/// The filter receives `(kind, partition_bytes, bucket, total_buckets)` and
/// returns true to keep the entry. Entries that fail the filter skip the
/// expensive `DataFileMeta` decoding entirely.
pub fn from_manifest_bytes_filtered<F>(
    bytes: &[u8],
    cache: &mut SchemaCache,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming(bytes)?;
    let writer_schema = cache.get_or_parse(&header.schema_json)?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, filter)
}

/// Decode ManifestEntry records from Avro OCF bytes using a pre-resolved shared schema.
///
/// Use this when the `WriterSchema` is shared across concurrent tasks via
/// `SharedSchemaCache`. Falls back to parsing if the OCF schema differs.
pub fn from_manifest_bytes_filtered_shared<F>(
    bytes: &[u8],
    shared_cache: &SharedSchemaCache,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming(bytes)?;
    let writer_schema = shared_cache.get_or_parse(&header.schema_json)?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, filter)
}

fn decode_manifest_streaming<F>(
    block_iter: &mut ocf::OcfBlockIter<'_>,
    writer_schema: &WriterSchema,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let mut results = Vec::new();
    while let Some(block) = block_iter.next_block()? {
        results.reserve(block.object_count);
        let mut cursor = AvroCursor::new(&block.data);
        for _ in 0..block.object_count {
            if let Some(entry) = manifest_entry_decode::decode_manifest_entries_filtered(
                &mut cursor,
                writer_schema,
                writer_schema.is_union_wrapped,
                filter,
            )? {
                results.push(entry);
            }
        }
    }
    Ok(results)
}

/// Decode a single record from the cursor, handling the top-level union wrapper
/// that Paimon uses (`["null", record]`).
fn decode_top_level_record<T: AvroRecordDecode>(
    cursor: &mut AvroCursor,
    writer_schema: &WriterSchema,
) -> crate::Result<T> {
    if writer_schema.is_union_wrapped {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Err(crate::Error::UnexpectedError {
                message: "avro decode: unexpected null in top-level union".into(),
                source: None,
            });
        }
    }
    T::decode(cursor, writer_schema)
}
