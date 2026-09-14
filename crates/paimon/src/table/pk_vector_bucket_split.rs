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

//! Reader for the byte form of Java's `BucketVectorSearchSplit`.
//!
//! A primary-key vector search is planned per bucket, because deciding which ANN
//! segments are current needs the bucket's complete active-file set rather than
//! the arbitrary file subset an ordinary table-scan split carries. Java plans
//! those splits and serializes them in this form; this module decodes them so
//! the search can then run outside the JVM.

use std::collections::HashMap;

use indexmap::IndexMap;

use crate::spec::{
    deserialize_binary_array_int, deserialize_binary_array_rows, BinaryRow, DeletionVectorMeta,
    GlobalIndexMeta,
};
use crate::table::source::{read_i32, read_i64, read_java_utf};
use crate::table::{DataSplit, RowRange};

/// `"PKVSPLIT"` in ASCII.
const MAGIC: i64 = 0x504B_5653_504C_4954;
const VERSION: i32 = 1;

/// Field counts of the rows nested in a payload. They come from the writer's
/// schema rather than the bytes, so both sides have to agree on them; a change
/// to either schema is what `VERSION` exists to signal.
const PAYLOAD_ARITY: i32 = 7;
const GLOBAL_INDEX_ARITY: i32 = 6;
const DELETION_VECTOR_ARITY: i32 = 4;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// All active data files and primary-key vector index payloads for one snapshot
/// bucket.
///
/// Reference: [org.apache.paimon.table.source.BucketVectorSearchSplit](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/BucketVectorSearchSplit.java)
#[derive(Debug, Clone, PartialEq)]
pub struct BucketVectorSearchSplit {
    data_split: DataSplit,
    payload_files: Vec<BucketVectorPayload>,
    /// Rows to keep per data file, as inclusive positions local to that file.
    /// Kept in the order the message carries them -- the writer sorts by file
    /// name -- so iterating a decoded split is reproducible; lookups are still
    /// by name.
    row_ranges_by_file: IndexMap<String, Vec<RowRange>>,
}

/// One primary-key vector index file carried by a [`BucketVectorSearchSplit`].
///
/// A distinct type from [`crate::spec::IndexFileMeta`], which models the
/// manifest form: that one has no external path and narrows the row count to
/// `i32`, while the schema behind these bytes has both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketVectorPayload {
    index_type: String,
    file_name: String,
    file_size: i64,
    row_count: i64,
    deletion_vectors_ranges: Option<IndexMap<String, DeletionVectorMeta>>,
    external_path: Option<String>,
    global_index_meta: GlobalIndexMeta,
}

impl BucketVectorPayload {
    pub fn index_type(&self) -> &str {
        &self.index_type
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn file_size(&self) -> i64 {
        self.file_size
    }

    pub fn row_count(&self) -> i64 {
        self.row_count
    }

    /// Deletion vector ranges. Always absent on a vector payload, but the schema
    /// can express them, so they are carried rather than dropped.
    pub fn deletion_vectors_ranges(&self) -> Option<&IndexMap<String, DeletionVectorMeta>> {
        self.deletion_vectors_ranges.as_ref()
    }

    pub fn external_path(&self) -> Option<&str> {
        self.external_path.as_deref()
    }

    /// Global index metadata. The schema makes the row nullable, but a
    /// `BucketVectorSearchSplit` cannot hold a payload without one, so a message
    /// that omits it is rejected rather than decoded into an absence callers
    /// would have to handle.
    pub fn global_index_meta(&self) -> &GlobalIndexMeta {
        &self.global_index_meta
    }

    /// The `_SOURCE_META` blob, which maps ANN ordinals back to rows. Always
    /// present, for the same reason.
    pub fn source_meta(&self) -> &[u8] {
        self.global_index_meta
            .source_meta
            .as_deref()
            .expect("a decoded payload always carries source metadata")
    }

    /// Consume the payload into the pieces a planner needs, so its decoded metadata
    /// moves out of the payload rather than being cloned out of it.
    ///
    /// Two decoded fields are deliberately left behind. `row_count` is the payload's
    /// own row count, which the read path derives from the source metadata instead.
    /// `deletion_vectors_ranges` belongs to deletion-vector index files -- Java
    /// builds a vector payload through the overload that leaves it null, and a read
    /// takes its deletion vectors from the bucket's data split -- so a value here
    /// describes something this payload is not, and is ignored rather than applied.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn into_parts(self) -> BucketVectorPayloadParts {
        BucketVectorPayloadParts {
            index_type: self.index_type,
            file_name: self.file_name,
            file_size: self.file_size,
            external_path: self.external_path,
            global_index_meta: self.global_index_meta,
        }
    }
}

/// The owned pieces of a [`BucketVectorPayload`], produced by
/// [`BucketVectorPayload::into_parts`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct BucketVectorPayloadParts {
    pub(crate) index_type: String,
    pub(crate) file_name: String,
    /// As decoded: Java writes a signed length, so a negative value is possible on
    /// the wire and is rejected where it is converted, not here.
    pub(crate) file_size: i64,
    pub(crate) external_path: Option<String>,
    pub(crate) global_index_meta: GlobalIndexMeta,
}

impl BucketVectorSearchSplit {
    /// The bucket's data files. Its own `row_ranges` are always absent here --
    /// this form carries them per file in [`Self::row_ranges_by_file`] -- so
    /// re-serializing it on its own would drop them.
    pub fn data_split(&self) -> &DataSplit {
        &self.data_split
    }

    pub fn payload_files(&self) -> &[BucketVectorPayload] {
        &self.payload_files
    }

    pub fn row_ranges_by_file(&self) -> &IndexMap<String, Vec<RowRange>> {
        &self.row_ranges_by_file
    }

    /// Consume the split into its three parts, so a planner can take ownership of
    /// the data split, the payloads and the row ranges without cloning them.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn into_parts(
        self,
    ) -> (
        DataSplit,
        Vec<BucketVectorPayload>,
        IndexMap<String, Vec<RowRange>>,
    ) {
        (self.data_split, self.payload_files, self.row_ranges_by_file)
    }

    /// Parse a Java `BucketVectorSearchSplit#serialize` message.
    ///
    /// Integers are big-endian and file names are Java modified UTF-8, following
    /// the split formats this one nests. The layout is:
    ///
    /// ```text
    /// i64 magic = "PKVSPLIT"; i32 version = 1
    /// DataSplit                                  // DataSplit.serialize, inline
    /// i32 payloadCount; (i32 rowLength, IndexFileMeta row)*
    /// i32 rangeFileCount; (utf fileName, i32 rangeCount, (i64 from, i64 to)*)*
    /// ```
    ///
    /// Each payload is one `BinaryRow` of Java's `IndexFileMeta.SCHEMA`, which
    /// carries no version of its own. `VERSION` therefore pins the layout of
    /// what is nested as well as the envelope, and has to move when either
    /// schema does -- unlike the `DataSplit`, which carries its own version and
    /// so may change underneath a message still labelled version 1.
    ///
    /// Consumes the entire buffer; trailing bytes are an error.
    pub fn deserialize(bytes: &[u8]) -> crate::Result<Self> {
        let mut input = bytes;
        let cur = &mut input;

        let magic = read_i64(cur)?;
        if magic != MAGIC {
            return Err(data_invalid(format!(
                "invalid BucketVectorSearchSplit magic: {magic:#018x}"
            )));
        }
        let version = read_i32(cur)?;
        if version != VERSION {
            return Err(crate::Error::Unsupported {
                message: format!("BucketVectorSearchSplit version {version} is not supported"),
            });
        }

        let data_split = DataSplit::read_body(cur).map_err(nested_error)?;

        let payload_count = read_count(cur, "payload file")?;
        let mut payload_files = Vec::new();
        for _ in 0..payload_count {
            payload_files.push(read_payload(cur)?);
        }

        // Row ranges are only meaningful against the data file they name, so index
        // the bucket's files before reading them. Two files sharing a name would
        // leave no way to say which of them a range belongs to; a bucket cannot
        // hold such a pair and the planner rejects it, but these bytes are
        // untrusted.
        //
        // The row count is checked here for EVERY file, not only for the ones the
        // message goes on to list ranges for -- the range reader below only sees the
        // files it lists. `bucket_search` re-checks the ACTIVE files it is about to
        // read, so this is the fail-fast copy plus the only check a file that never
        // becomes active gets.
        let mut row_counts: HashMap<&str, i64> = HashMap::new();
        for file in data_split.data_files() {
            if file.row_count < 0 {
                return Err(data_invalid(format!(
                    "data file {} has a negative row count: {}",
                    file.file_name, file.row_count
                )));
            }
            if row_counts
                .insert(file.file_name.as_str(), file.row_count)
                .is_some()
            {
                return Err(data_invalid(format!(
                    "duplicate data file in the bucket split: {}",
                    file.file_name
                )));
            }
        }

        let range_file_count = read_count(cur, "row-range file")?;
        let mut row_ranges_by_file = IndexMap::new();
        for _ in 0..range_file_count {
            let file_name = read_java_utf(cur)?;
            let ranges = read_row_ranges(cur, &file_name, &row_counts)?;
            if row_ranges_by_file
                .insert(file_name.clone(), ranges)
                .is_some()
            {
                return Err(data_invalid(format!(
                    "duplicate row-range file entry: {file_name}"
                )));
            }
        }

        if !cur.is_empty() {
            return Err(data_invalid(format!(
                "{} trailing bytes after BucketVectorSearchSplit",
                cur.len()
            )));
        }

        Ok(Self {
            data_split,
            payload_files,
            row_ranges_by_file,
        })
    }
}

/// Report a malformed nested structure as invalid data. The decoders reached
/// from here signal a short buffer as an unexpected error, which reads as an
/// internal fault; these bytes are untrusted, so the caller has to be able to
/// tell a bad message from a bug. A version that cannot be read stays
/// unsupported.
fn nested_error(error: crate::Error) -> crate::Error {
    match error {
        crate::Error::Unsupported { .. } | crate::Error::DataInvalid { .. } => error,
        other => crate::Error::DataInvalid {
            message: "invalid nested structure in BucketVectorSearchSplit".to_string(),
            source: Some(Box::new(other)),
        },
    }
}

/// Read the rows to keep in one data file, as inclusive positions local to that
/// file. Java writes ranges its planner produced and re-checks nothing, so the
/// checks here are what a reader of untrusted bytes needs rather than a mirror
/// of the writer: `RowRange` cannot represent a descending pair at all, and a
/// range outside its file would read rows that are not there. The bound is only as
/// good as the count it is taken against: every file's count was checked
/// non-negative when the bucket was indexed above, but a forged LARGER positive
/// count does lift this bound, so nothing downstream may size an allocation from
/// what it admits.
fn read_row_ranges(
    cur: &mut &[u8],
    file_name: &str,
    row_counts: &HashMap<&str, i64>,
) -> crate::Result<Vec<RowRange>> {
    let row_count = *row_counts.get(file_name).ok_or_else(|| {
        data_invalid(format!(
            "row ranges reference data file not present in the bucket split: {file_name}"
        ))
    })?;
    let count = read_count(cur, "row range")?;
    let mut ranges: Vec<RowRange> = Vec::new();
    for _ in 0..count {
        let from = read_i64(cur)?;
        let to = read_i64(cur)?;
        if from > to {
            return Err(data_invalid(format!(
                "invalid row range [{from}, {to}] for file {file_name}"
            )));
        }
        if from < 0 {
            return Err(data_invalid(format!(
                "negative row range [{from}, {to}] for file {file_name}"
            )));
        }
        if to >= row_count {
            return Err(data_invalid(format!(
                "row range [{from}, {to}] for file {file_name} is outside [0, {row_count})"
            )));
        }
        ranges.push(RowRange::new(from, to));
    }
    Ok(ranges)
}

/// Read one payload: an `i32` byte length followed by a `BinaryRow` of Java's
/// `IndexFileMeta.SCHEMA`, the framing `ObjectSerializer` writes a record with.
fn read_payload(cur: &mut &[u8]) -> crate::Result<BucketVectorPayload> {
    let row = read_nested_row(cur, PAYLOAD_ARITY, "payload")?;
    // Fields the schema declares NOT NULL. A null slot is zeroed rather than
    // absent, so without this an empty name or a zero size would come back as a
    // value the writer never wrote.
    require_present(&row, &[0, 1, 2, 3], "payload")?;

    let deletion_vectors_ranges = if row.is_null_at(4) {
        None
    } else {
        Some(read_deletion_vector_ranges(
            row.get_binary(4).map_err(nested_error)?,
        )?)
    };
    let file_name = row.get_string(1).map_err(nested_error)?.to_string();
    if row.is_null_at(6) {
        return Err(data_invalid(format!(
            "PK-vector payload {file_name} has no global index metadata"
        )));
    }
    let global_index_meta = read_global_index_meta(row.get_binary(6).map_err(nested_error)?)?;
    if global_index_meta.source_meta.is_none() {
        return Err(data_invalid(format!(
            "PK-vector payload {file_name} has no source metadata"
        )));
    }

    Ok(BucketVectorPayload {
        index_type: row.get_string(0).map_err(nested_error)?.to_string(),
        file_name,
        file_size: row.get_long(2).map_err(nested_error)?,
        row_count: row.get_long(3).map_err(nested_error)?,
        deletion_vectors_ranges,
        external_path: if row.is_null_at(5) {
            None
        } else {
            Some(row.get_string(5).map_err(nested_error)?.to_string())
        },
        global_index_meta,
    })
}

fn read_global_index_meta(data: &[u8]) -> crate::Result<GlobalIndexMeta> {
    let row = nested_row(data, GLOBAL_INDEX_ARITY, "global index metadata")?;
    require_present(&row, &[0, 1, 2], "global index metadata")?;
    Ok(GlobalIndexMeta {
        row_range_start: row.get_long(0).map_err(nested_error)?,
        row_range_end: row.get_long(1).map_err(nested_error)?,
        index_field_id: row.get_int(2).map_err(nested_error)?,
        extra_field_ids: if row.is_null_at(3) {
            None
        } else {
            Some(deserialize_binary_array_int(
                row.get_binary(3).map_err(nested_error)?,
            )?)
        },
        index_meta: if row.is_null_at(4) {
            None
        } else {
            Some(row.get_binary(4).map_err(nested_error)?.to_vec())
        },
        source_meta: if row.is_null_at(5) {
            None
        } else {
            Some(row.get_binary(5).map_err(nested_error)?.to_vec())
        },
    })
}

fn read_deletion_vector_ranges(data: &[u8]) -> crate::Result<IndexMap<String, DeletionVectorMeta>> {
    let mut ranges = IndexMap::new();
    for element in deserialize_binary_array_rows(data)? {
        let row = nested_row(element, DELETION_VECTOR_ARITY, "deletion vector metadata")?;
        require_present(&row, &[0, 1, 2], "deletion vector metadata")?;
        // The data file name is field 0 of the row and also the map key, the way
        // Java rebuilds this map.
        let file_name = row.get_string(0).map_err(nested_error)?.to_string();
        let meta = DeletionVectorMeta {
            offset: row.get_int(1).map_err(nested_error)?,
            length: row.get_int(2).map_err(nested_error)?,
            cardinality: if row.is_null_at(3) {
                None
            } else {
                Some(row.get_long(3).map_err(nested_error)?)
            },
        };
        ranges.insert(file_name, meta);
    }
    Ok(ranges)
}

/// Read an `i32`-framed row body off the cursor.
fn read_nested_row(cur: &mut &[u8], arity: i32, what: &str) -> crate::Result<BinaryRow> {
    let length = read_i32(cur)?;
    if length < 0 {
        return Err(data_invalid(format!(
            "negative {what} row length: {length}"
        )));
    }
    let length = length as usize;
    if length > cur.len() {
        return Err(data_invalid(format!(
            "{what} row length {length} exceeds {} remaining bytes",
            cur.len()
        )));
    }
    nested_row(crate::table::source::take(cur, length)?, arity, what)
}

/// Reject a null in a field the schema declares NOT NULL.
fn require_present(row: &BinaryRow, fields: &[usize], what: &str) -> crate::Result<()> {
    for &field in fields {
        if row.is_null_at(field) {
            return Err(data_invalid(format!(
                "{what} field {field} must not be null"
            )));
        }
    }
    Ok(())
}

/// View bytes that are already delimited as a row of the given arity.
fn nested_row(data: &[u8], arity: i32, what: &str) -> crate::Result<BinaryRow> {
    let fixed_part = BinaryRow::cal_fix_part_size_in_bytes(arity) as usize;
    if data.len() < fixed_part {
        return Err(data_invalid(format!(
            "{what} row of {} bytes is shorter than its {fixed_part}-byte fixed part",
            data.len()
        )));
    }
    Ok(BinaryRow::from_bytes(arity, data.to_vec()))
}

/// Read an element count, bounded by the bytes that can still follow it. Every
/// element of every repetition here costs at least four bytes, so this keeps a
/// forged count from driving work the message could not contain.
fn read_count(cur: &mut &[u8], element: &str) -> crate::Result<usize> {
    let count = read_i32(cur)?;
    if count < 0 {
        return Err(data_invalid(format!("negative {element} count: {count}")));
    }
    let count = count as usize;
    if count > cur.len() / 4 {
        return Err(data_invalid(format!(
            "{element} count {count} exceeds the maximum allowed by {} remaining bytes",
            cur.len()
        )));
    }
    Ok(count)
}

#[cfg(test)]
impl BucketVectorSearchSplit {
    /// Assemble a split directly, for tests that need shapes the decoder will not
    /// produce -- a nested split that wrongly carries row ranges, two splits for one
    /// bucket, a negative payload size. Production splits always come from
    /// [`Self::deserialize`].
    pub(crate) fn new_for_test(
        data_split: DataSplit,
        payload_files: Vec<BucketVectorPayload>,
        row_ranges_by_file: IndexMap<String, Vec<RowRange>>,
    ) -> Self {
        Self {
            data_split,
            payload_files,
            row_ranges_by_file,
        }
    }
}

#[cfg(test)]
impl BucketVectorPayload {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_test(
        index_type: &str,
        file_name: &str,
        file_size: i64,
        row_count: i64,
        deletion_vectors_ranges: Option<IndexMap<String, DeletionVectorMeta>>,
        external_path: Option<String>,
        global_index_meta: GlobalIndexMeta,
    ) -> Self {
        Self {
            index_type: index_type.to_string(),
            file_name: file_name.to_string(),
            file_size,
            row_count,
            deletion_vectors_ranges,
            external_path,
            global_index_meta,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PrimaryKeyIndexSourceMeta;

    /// Byte-for-byte copy of what Java's `BucketVectorSearchSplit#serialize`
    /// produces, so a change on either side that is not mirrored shows up as a
    /// decode failure here. Dumped from `BucketVectorSearchSplitTest.split()` in
    /// apache/paimon at `088d4880ff` (#9386); the Java side keeps no golden bytes
    /// of its own, so a change to that builder has to be dumped here again by
    /// hand -- this test is the only thing that notices.
    const GOLDEN: &[u8] = include_bytes!("goldens/bucket_vector_search_split_v1.bin");

    fn golden() -> Vec<u8> {
        GOLDEN.to_vec()
    }

    /// Offsets into the fixture. The nested `DataSplit` is written inline and is
    /// variable-length, so what follows it cannot be located by a formula over
    /// the header; these come from the fixture's own shape, and
    /// `fixture_offsets_are_current` fails if it changes.
    const DATA_SPLIT_VERSION_OFFSET: usize = 8 + 4 + 8;
    const PAYLOAD_COUNT_OFFSET: usize = 643;

    /// The `DataSplit`'s data-file count, which follows its deprecated
    /// before-files count and before-deletion-files flag.
    const DATA_FILE_COUNT_OFFSET: usize = 80;

    /// `_ROW_COUNT` inside the only data file's row: field 2 of a 21-field
    /// `BinaryRow`, so past its 8-byte null region and two 8-byte slots. Written
    /// little-endian, unlike the big-endian envelope around it.
    const DATA_FILE_ROW_COUNT_OFFSET: usize = DATA_FILE_COUNT_OFFSET + 4 + 4 + 8 + 2 * 8;

    /// The row-range section closes the message: one modified-UTF-8 file name
    /// (`u16` length + 10 bytes), its range count, and two `[from, to]` pairs.
    const RANGE_SECTION_BYTES: usize = 2 + 10 + 4 + 4 * 8;

    fn payload_row_length_offset() -> usize {
        PAYLOAD_COUNT_OFFSET + 4
    }

    fn range_file_name_offset() -> usize {
        GOLDEN.len() - RANGE_SECTION_BYTES + 2
    }

    fn range_bound_offset(index: usize) -> usize {
        GOLDEN.len() - 4 * 8 + index * 8
    }

    fn read_i32_at(bytes: &[u8], offset: usize) -> i32 {
        i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap())
    }

    /// The offsets above are hand-derived, so check them against the fixture
    /// rather than letting a stale one make another test pass for the wrong
    /// reason.
    #[test]
    fn fixture_offsets_are_current() {
        assert_eq!(read_i32_at(GOLDEN, DATA_SPLIT_VERSION_OFFSET), 9);
        assert_eq!(
            i64::from_le_bytes(
                GOLDEN[DATA_FILE_ROW_COUNT_OFFSET..DATA_FILE_ROW_COUNT_OFFSET + 8]
                    .try_into()
                    .unwrap()
            ),
            6,
            "expected the data file's row count"
        );
        assert_eq!(read_i32_at(GOLDEN, PAYLOAD_COUNT_OFFSET), 1);
        assert_eq!(read_i32_at(GOLDEN, DATA_FILE_COUNT_OFFSET), 1);
        assert_eq!(
            read_i32_at(GOLDEN, GOLDEN.len() - RANGE_SECTION_BYTES - 4),
            1,
            "expected one row-range file entry"
        );
        assert_eq!(
            &GOLDEN[range_file_name_offset()..range_file_name_offset() + 10],
            b"data-1.orc"
        );
    }

    fn put_i32(bytes: &mut [u8], offset: usize, value: i32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    fn put_i64(bytes: &mut [u8], offset: usize, value: i64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
    }

    fn single_string_row(value: &str) -> Vec<u8> {
        let mut builder = crate::spec::BinaryRowBuilder::new(1);
        builder.write_bytes(0, value.as_bytes());
        builder.build_serialized()
    }

    fn single_int_row(value: i32) -> Vec<u8> {
        let mut builder = crate::spec::BinaryRowBuilder::new(1);
        builder.write_int(0, value);
        builder.build_serialized()
    }

    #[test]
    fn deserialize_matches_java_golden() {
        let split = BucketVectorSearchSplit::deserialize(GOLDEN).unwrap();

        let data_split = split.data_split();
        assert_eq!(data_split.snapshot_id(), 11);
        assert_eq!(data_split.bucket(), 2);
        assert_eq!(data_split.bucket_path(), "bucket-2");
        assert_eq!(data_split.total_buckets(), 8);
        assert!(data_split.data_deletion_files().is_none());
        assert!(!data_split.raw_convertible());
        // The nested rows are the part of the message most easily misread, so the
        // fixture carries real ones rather than empty rows -- and they are read
        // back as values, not just as bytes.
        assert_eq!(
            data_split.partition().to_serialized_bytes(),
            single_int_row(20_250_826)
        );
        assert_eq!(data_split.partition().get_int(0).unwrap(), 20_250_826);

        let files = data_split.data_files();
        assert_eq!(files.len(), 1);
        let file = &files[0];
        assert_eq!(file.file_name, "data-1.orc");
        assert_eq!(file.file_size, 1_234);
        assert_eq!(file.row_count, 6);
        assert_eq!(file.min_key, single_string_row("min_key"));
        assert_eq!(file.max_key, single_string_row("max_key"));
        for stats in [&file.key_stats, &file.value_stats] {
            assert_eq!(stats.min_values(), &single_string_row("min_value"));
            assert_eq!(stats.max_values(), &single_string_row("max_value"));
            assert_eq!(stats.null_counts(), &[Some(0)]);
        }
        assert_eq!(file.min_sequence_number, 3);
        assert_eq!(file.max_sequence_number, 9);
        assert_eq!(file.schema_id, 7);
        assert_eq!(file.level, 1);
        assert!(file.extra_files.is_empty());
        assert_eq!(
            file.creation_time.unwrap().timestamp_millis(),
            1_700_000_000_000
        );
        assert_eq!(file.delete_row_count, Some(0));
        assert_eq!(file.embedded_index, None);
        // FileSource.COMPACT
        assert_eq!(file.file_source, Some(1));
        assert_eq!(file.value_stats_cols, None);
        assert_eq!(file.external_path, None);
        assert_eq!(file.first_row_id, Some(40));
        assert_eq!(
            file.write_cols.as_deref(),
            Some(["k".to_string(), "v".to_string()].as_slice())
        );
        assert_eq!(
            file.column_max_sequence_numbers.as_deref(),
            Some([3i64, 9].as_slice())
        );

        assert_eq!(split.payload_files().len(), 1);
        let payload = &split.payload_files()[0];
        assert_eq!(payload.index_type(), "ivf-pq");
        assert_eq!(payload.file_name(), "ann-0.idx");
        assert_eq!(payload.file_size(), 5_000_000_000);
        assert_eq!(payload.row_count(), 6);
        assert_eq!(payload.deletion_vectors_ranges(), None);
        assert_eq!(
            payload.external_path(),
            Some("s3://vector-bucket/ann-0.idx")
        );
        let global = payload.global_index_meta();
        assert_eq!(global.row_range_start, 40);
        assert_eq!(global.row_range_end, 45);
        assert_eq!(global.index_field_id, 7);
        assert_eq!(
            global.extra_field_ids.as_deref(),
            Some([3i32, 5].as_slice())
        );
        assert_eq!(global.index_meta.as_deref(), Some([1u8, 2, 3].as_slice()));

        let source_meta = PrimaryKeyIndexSourceMeta::from_global_index_meta(global).unwrap();
        assert_eq!(source_meta.data_level(), 1);
        assert_eq!(source_meta.source_files().len(), 1);
        assert_eq!(source_meta.source_files()[0].file_name(), "data-1.orc");
        assert_eq!(source_meta.source_files()[0].row_count(), 6);

        let mut expected_ranges = IndexMap::new();
        expected_ranges.insert(
            "data-1.orc".to_string(),
            vec![RowRange::new(0, 1), RowRange::new(4, 5)],
        );
        assert_eq!(split.row_ranges_by_file(), &expected_ranges);
    }

    /// A second fixture, for what the first cannot reach: deletion vector ranges
    /// (the only array-of-rows in this format), more than one payload, and a
    /// payload with every optional the schema allows to be absent left absent.
    ///
    /// No committed Java builder produces this one, so its shape is recorded here
    /// to keep it reproducible. It is `BucketVectorSearchSplitTest.split()`'s
    /// `dataSplit` and `rowRangesByFile` with two payloads in place of one:
    ///
    /// ```java
    /// LinkedHashMap<String, DeletionVectorMeta> dv = new LinkedHashMap<>();
    /// dv.put("data-1.orc", new DeletionVectorMeta("data-1.orc", 0, 8, 2L));
    /// dv.put("data-2.orc", new DeletionVectorMeta("data-2.orc", 8, 16, null));
    /// new IndexFileMeta("ivf-pq", "ann-0.idx", 5_000_000_000L, 6, dv,
    ///         "s3://vector-bucket/ann-0.idx",
    ///         new GlobalIndexMeta(40, 45, 7, new int[] {3, 5, 9},
    ///                 new byte[] {1, 2, 3}, sourceMeta));
    /// new IndexFileMeta("flat", "ann-1.idx", 0, 0, null, null,
    ///         new GlobalIndexMeta(0, 0, 1, null, null, sourceMeta));
    /// ```
    ///
    /// where `sourceMeta` is the same blob both payloads carry, the one
    /// `split()` builds: `PrimaryKeyIndexSourceMeta(1, [("data-1.orc", 6)])`.
    const GOLDEN_DELETION_VECTORS: &[u8] =
        include_bytes!("goldens/bucket_vector_search_split_v1_deletion_vectors.bin");

    #[test]
    fn deserialize_matches_java_golden_with_deletion_vectors() {
        let split = BucketVectorSearchSplit::deserialize(GOLDEN_DELETION_VECTORS).unwrap();
        assert_eq!(split.payload_files().len(), 2);

        // The nested DataSplit and the row ranges are the same as the other
        // fixture's, so a change that only breaks one of the two shows up here.
        assert_eq!(split.data_split().snapshot_id(), 11);
        assert_eq!(split.data_split().data_files()[0].file_name, "data-1.orc");
        assert_eq!(
            split.row_ranges_by_file()["data-1.orc"],
            vec![RowRange::new(0, 1), RowRange::new(4, 5)]
        );

        let full = &split.payload_files()[0];
        assert_eq!(full.index_type(), "ivf-pq");
        assert_eq!(full.file_name(), "ann-0.idx");
        assert_eq!(full.file_size(), 5_000_000_000);
        assert_eq!(full.row_count(), 6);
        assert_eq!(full.external_path(), Some("s3://vector-bucket/ann-0.idx"));
        let dv_ranges = full.deletion_vectors_ranges().unwrap();
        assert_eq!(
            dv_ranges.keys().collect::<Vec<_>>(),
            ["data-1.orc", "data-2.orc"]
        );
        assert_eq!(
            dv_ranges["data-1.orc"],
            DeletionVectorMeta {
                offset: 0,
                length: 8,
                cardinality: Some(2),
            }
        );
        // Java writes a null cardinality; it must not come back as a value.
        assert_eq!(
            dv_ranges["data-2.orc"],
            DeletionVectorMeta {
                offset: 8,
                length: 16,
                cardinality: None,
            }
        );
        let global = full.global_index_meta();
        assert_eq!(global.row_range_start, 40);
        assert_eq!(global.row_range_end, 45);
        assert_eq!(global.index_field_id, 7);
        assert_eq!(
            global.extra_field_ids.as_deref(),
            Some([3i32, 5, 9].as_slice())
        );
        assert_eq!(global.index_meta.as_deref(), Some([1u8, 2, 3].as_slice()));
        // Parse each payload's own source metadata rather than comparing the two
        // payloads': the fixture gives them the same blob, so a comparison passes
        // even when one side is read off the wrong payload.
        assert_source_meta(full);

        let minimal = &split.payload_files()[1];
        assert_eq!(minimal.index_type(), "flat");
        assert_eq!(minimal.file_name(), "ann-1.idx");
        assert_eq!(minimal.file_size(), 0);
        assert_eq!(minimal.row_count(), 0);
        assert_eq!(minimal.deletion_vectors_ranges(), None);
        assert_eq!(minimal.external_path(), None);
        let minimal_global = minimal.global_index_meta();
        assert_eq!(minimal_global.row_range_start, 0);
        assert_eq!(minimal_global.row_range_end, 0);
        assert_eq!(minimal_global.index_field_id, 1);
        assert_eq!(minimal_global.extra_field_ids, None);
        assert_eq!(minimal_global.index_meta, None);
        assert_source_meta(minimal);
    }

    /// Both payloads carry the same `_SOURCE_META`, so each is checked by parsing
    /// its own rather than by comparing them.
    fn assert_source_meta(payload: &BucketVectorPayload) {
        let source = PrimaryKeyIndexSourceMeta::deserialize(payload.source_meta()).unwrap();
        assert_eq!(source.data_level(), 1);
        assert_eq!(source.source_files().len(), 1);
        assert_eq!(source.source_files()[0].file_name(), "data-1.orc");
        assert_eq!(source.source_files()[0].row_count(), 6);
    }

    #[test]
    fn rejects_invalid_magic() {
        let mut bytes = golden();
        put_i64(&mut bytes, 0, MAGIC + 1);
        assert_error_contains(&bytes, "invalid BucketVectorSearchSplit magic");
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut bytes = golden();
        put_i32(&mut bytes, 8, VERSION + 1);
        let error = BucketVectorSearchSplit::deserialize(&bytes).unwrap_err();
        assert!(
            matches!(error, crate::Error::Unsupported { .. }),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_truncated_header() {
        assert_error_contains(&GOLDEN[..8], "underrun");
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes = golden();
        bytes.push(0);
        assert_error_contains(&bytes, "1 trailing bytes after BucketVectorSearchSplit");
    }

    /// The nested `DataSplit` carries its own version, and one this reader cannot
    /// read has to stay distinguishable from a corrupt message.
    #[test]
    fn rejects_unsupported_nested_data_split_version() {
        let mut bytes = golden();
        put_i32(&mut bytes, DATA_SPLIT_VERSION_OFFSET, 99);
        let error = BucketVectorSearchSplit::deserialize(&bytes).unwrap_err();
        assert!(
            matches!(error, crate::Error::Unsupported { .. }),
            "unexpected error: {error}"
        );
    }

    /// A malformed nested structure is invalid data, not an internal fault:
    /// callers that treat the two differently have to be able to tell them apart.
    #[test]
    fn reports_a_malformed_nested_row_as_invalid_data() {
        let mut bytes = golden();
        // Shrink the payload row so its fixed part no longer fits.
        put_i32(&mut bytes, payload_row_length_offset(), 4);
        let error = BucketVectorSearchSplit::deserialize(&bytes).unwrap_err();
        assert!(
            matches!(error, crate::Error::DataInvalid { .. }),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_negative_payload_row_length() {
        let mut bytes = golden();
        put_i32(&mut bytes, payload_row_length_offset(), -1);
        assert_error_contains(&bytes, "negative payload row length: -1");
    }

    #[test]
    fn rejects_payload_row_longer_than_the_message() {
        let mut bytes = golden();
        put_i32(&mut bytes, payload_row_length_offset(), i32::MAX);
        assert_error_contains(&bytes, "payload row length");
    }

    #[test]
    fn rejects_negative_payload_count() {
        let mut bytes = golden();
        put_i32(&mut bytes, PAYLOAD_COUNT_OFFSET, -1);
        assert_error_contains(&bytes, "negative payload file count: -1");
    }

    /// A count is only trusted up to what the remaining bytes could hold, so an
    /// inflated one is rejected instead of driving work the message cannot
    /// contain.
    #[test]
    fn rejects_payload_count_larger_than_the_message() {
        let mut bytes = golden();
        put_i32(&mut bytes, PAYLOAD_COUNT_OFFSET, i32::MAX);
        assert_error_contains(&bytes, "payload file count");
    }

    #[test]
    fn rejects_descending_row_range() {
        let mut bytes = golden();
        put_i64(&mut bytes, range_bound_offset(0), 42);
        assert_error_contains(&bytes, "invalid row range [42, 1] for file data-1.orc");
    }

    #[test]
    fn rejects_row_range_past_the_end_of_its_file() {
        let mut bytes = golden();
        put_i64(&mut bytes, range_bound_offset(3), 6);
        assert_error_contains(&bytes, "is outside [0, 6)");
    }

    #[test]
    fn rejects_row_ranges_for_an_unknown_data_file() {
        let mut bytes = golden();
        let offset = range_file_name_offset();
        assert_eq!(&bytes[offset..offset + 10], b"data-1.orc");
        bytes[offset..offset + 10].copy_from_slice(b"data-2.orc");
        assert_error_contains(
            &bytes,
            "row ranges reference data file not present in the bucket split: data-2.orc",
        );
    }

    /// Row ranges key on the file name, so the names have to identify one file
    /// each for the mapping to mean anything.
    #[test]
    fn rejects_duplicate_data_files() {
        let mut bytes = golden();
        // Point the second data file entry at the first file's row by cloning it:
        // simplest here is to rewrite the count and append a copy of the row.
        let row_start = DATA_FILE_COUNT_OFFSET + 4;
        let row_length = read_i32_at(&bytes, row_start) as usize;
        let row = bytes[row_start..row_start + 4 + row_length].to_vec();
        put_i32(&mut bytes, DATA_FILE_COUNT_OFFSET, 2);
        let insert_at = row_start + 4 + row_length;
        bytes.splice(insert_at..insert_at, row);
        assert_error_contains(
            &bytes,
            "duplicate data file in the bucket split: data-1.orc",
        );
    }

    /// A null in a field the schema declares NOT NULL is zeroed rather than
    /// absent, so it has to be caught by the bit and not by the value.
    #[test]
    fn rejects_a_null_in_a_required_payload_field() {
        let mut bytes = golden();
        // Field 0 of the payload row: its null bit is bit 8, the first bit of the
        // second byte of the row's null region.
        let row = payload_row_length_offset() + 4;
        bytes[row + 1] |= 1;
        assert_error_contains(&bytes, "payload field 0 must not be null");
    }

    /// A forged row count must not be usable to lift the range bound.
    #[test]
    fn rejects_a_negative_data_file_row_count() {
        let mut bytes = golden();
        bytes[DATA_FILE_ROW_COUNT_OFFSET..DATA_FILE_ROW_COUNT_OFFSET + 8]
            .copy_from_slice(&(-1i64).to_le_bytes());
        assert_error_contains(&bytes, "data file data-1.orc has a negative row count: -1");
    }

    /// The same forged count, on a message that lists NO ranges at all. The range
    /// reader only ever sees the files the message lists, and `bucket_search`
    /// re-checks only the files that become ACTIVE, so for anything else this is the
    /// only check there is.
    #[test]
    fn rejects_a_negative_row_count_on_a_file_with_no_listed_ranges() {
        let mut bytes = golden_without_row_ranges();
        bytes[DATA_FILE_ROW_COUNT_OFFSET..DATA_FILE_ROW_COUNT_OFFSET + 8]
            .copy_from_slice(&crate::spec::DataFileMeta::ROW_COUNT_UNKNOWN.to_le_bytes());
        assert_error_contains(&bytes, "data file data-1.orc has a negative row count: -1");
    }

    /// The golden with its row-range section removed: the no-pre-filter shape Java
    /// emits when its planner narrowed nothing.
    fn golden_without_row_ranges() -> Vec<u8> {
        let mut bytes = golden();
        let count_offset = bytes.len() - RANGE_SECTION_BYTES - 4;
        bytes.truncate(count_offset + 4);
        bytes[count_offset..count_offset + 4].copy_from_slice(&0i32.to_be_bytes());
        bytes
    }

    #[test]
    fn a_message_that_lists_no_ranges_decodes_to_no_entries() {
        let split = BucketVectorSearchSplit::deserialize(&golden_without_row_ranges()).unwrap();
        assert!(split.row_ranges_by_file().is_empty());
        assert_eq!(split.data_split().data_files().len(), 1);
    }

    #[test]
    fn rejects_negative_row_range() {
        let mut bytes = golden();
        put_i64(&mut bytes, range_bound_offset(0), -1);
        assert_error_contains(&bytes, "negative row range [-1, 1] for file data-1.orc");
    }

    fn assert_error_contains(bytes: &[u8], expected: &str) {
        let error = BucketVectorSearchSplit::deserialize(bytes).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains(expected),
            "expected {expected:?} in {message:?}"
        );
    }
}
