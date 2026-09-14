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

//! Tag manager for reading tag metadata using FileIO.
//!
//! Reference: [org.apache.paimon.utils.TagManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java)
//! and [pypaimon.tag.tag_manager.TagManager](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/tag/tag_manager.py).

use crate::io::FileIO;
use crate::spec::Snapshot;
use futures::future::try_join_all;
use opendal::raw::get_basename;

const TAG_DIR: &str = "tag";
const TAG_PREFIX: &str = "tag-";

/// Manager for tag files using unified FileIO.
///
/// Tags are named snapshots stored as JSON files at `{table_path}/tag/tag-{name}`.
/// The tag file format is identical to a Snapshot JSON file.
///
/// Reference: [org.apache.paimon.utils.TagManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java)
#[derive(Debug, Clone)]
pub struct TagManager {
    file_io: FileIO,
    table_path: String,
}

impl TagManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
        }
    }

    /// Path to the tag directory (e.g. `table_path/tag`).
    pub fn tag_directory(&self) -> String {
        format!("{}/{}", self.table_path, TAG_DIR)
    }

    /// Create a TagManager for a branch of this table.
    pub fn with_branch(&self, branch_name: &str) -> Self {
        let branch_path = format!("{}/branch/branch-{}", self.table_path, branch_name);
        Self::new(self.file_io.clone(), branch_path)
    }

    /// Path to the tag file for the given name (e.g. `tag/tag-my_tag`).
    pub fn tag_path(&self, tag_name: &str) -> String {
        format!("{}/{}{}", self.tag_directory(), TAG_PREFIX, tag_name)
    }

    /// Check if a tag exists.
    pub async fn tag_exists(&self, tag_name: &str) -> crate::Result<bool> {
        validate_tag_name(tag_name)?;
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        input.exists().await
    }

    /// Get the snapshot for a tag, or None if the tag file does not exist.
    ///
    /// Tag files are JSON with the same schema as Snapshot.
    /// Reads directly and catches NotFound to avoid a separate exists() IO round-trip.
    pub async fn get(&self, tag_name: &str) -> crate::Result<Option<Snapshot>> {
        validate_tag_name(tag_name)?;
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        let bytes = match input.read().await {
            Ok(b) => b,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let snapshot: Snapshot =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("tag '{tag_name}' JSON invalid: {e}"),
                source: Some(Box::new(e)),
            })?;
        Ok(Some(snapshot))
    }

    /// Get a tag's snapshot together with the two tag-only fields Java writes
    /// alongside it: the creation time as epoch millis and the retention as
    /// seconds. Returns `None` when the tag file does not exist; either metadata
    /// field is `None` when absent or unparsable.
    pub async fn get_with_metadata(
        &self,
        tag_name: &str,
    ) -> crate::Result<Option<(Snapshot, Option<i64>, Option<f64>)>> {
        Ok(self.get_with_raw_metadata(tag_name).await?.map(
            |(snapshot, create_time, time_retained)| {
                (
                    snapshot,
                    create_time.map(|value| value.and_utc().timestamp_millis()),
                    time_retained,
                )
            },
        ))
    }

    pub(crate) async fn get_with_raw_metadata(
        &self,
        tag_name: &str,
    ) -> crate::Result<Option<(Snapshot, Option<chrono::NaiveDateTime>, Option<f64>)>> {
        validate_tag_name(tag_name)?;
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        let bytes = match input.read().await {
            Ok(b) => b,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("tag '{tag_name}' JSON invalid: {e}"),
                source: Some(Box::new(e)),
            })?;
        let snapshot: Snapshot =
            serde_json::from_value(value.clone()).map_err(|e| crate::Error::DataInvalid {
                message: format!("tag '{tag_name}' JSON invalid: {e}"),
                source: Some(Box::new(e)),
            })?;
        let create_time = value
            .get(FIELD_TAG_CREATE_TIME)
            .and_then(parse_tag_create_time);
        let time_retained = value
            .get(FIELD_TAG_TIME_RETAINED)
            .and_then(serde_json::Value::as_f64);
        Ok(Some((snapshot, create_time, time_retained)))
    }

    /// Like [`Self::list_all`], but each row also carries the tag creation time
    /// in epoch millis and the retention in seconds.
    #[allow(clippy::type_complexity)]
    pub async fn list_all_with_metadata(
        &self,
    ) -> crate::Result<Vec<(String, Snapshot, Option<i64>, Option<f64>)>> {
        let names = self.list_all_names().await?;
        try_join_all(names.into_iter().map(|name| async move {
            let (snap, create_time, retained) =
                self.get_with_metadata(&name)
                    .await?
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("tag '{name}' disappeared during listing"),
                        source: None,
                    })?;
            Ok::<_, crate::Error>((name, snap, create_time, retained))
        }))
        .await
    }

    /// List all tag names sorted ascending. Returns an empty vector when the
    /// tag directory does not exist.
    pub async fn list_all_names(&self) -> crate::Result<Vec<String>> {
        let tag_dir = self.tag_directory();
        let statuses = match self.file_io.list_status(&tag_dir).await {
            Ok(s) => s,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        let mut names: Vec<String> = statuses
            .into_iter()
            .filter(|s| !s.is_dir)
            .filter_map(|s| {
                get_basename(&s.path)
                    .strip_prefix(TAG_PREFIX)
                    .map(str::to_string)
            })
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    /// Create a tag by writing the snapshot JSON to the tag path.
    pub async fn create(&self, tag_name: &str, snapshot: &Snapshot) -> crate::Result<()> {
        validate_tag_name(tag_name)?;
        let path = self.tag_path(tag_name);
        let json = serde_json::to_string(snapshot).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to serialize snapshot for tag '{tag_name}': {e}"),
            source: Some(Box::new(e)),
        })?;
        self.file_io.mkdirs(&self.tag_directory()).await?;
        let output = self.file_io.new_output(&path)?;
        output.write(bytes::Bytes::from(json)).await
    }

    /// Delete a tag file.
    pub async fn delete(&self, tag_name: &str) -> crate::Result<()> {
        validate_tag_name(tag_name)?;
        let path = self.tag_path(tag_name);
        self.file_io.delete_file(&path).await
    }

    /// List all tags as `(name, snapshot)` pairs sorted by name ascending.
    pub async fn list_all(&self) -> crate::Result<Vec<(String, Snapshot)>> {
        let names = self.list_all_names().await?;
        try_join_all(names.into_iter().map(|name| async move {
            let snap = self
                .get(&name)
                .await?
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("tag '{name}' disappeared during listing"),
                    source: None,
                })?;
            Ok::<_, crate::Error>((name, snap))
        }))
        .await
    }
}

/// Java `Tag` adds these two fields on top of the snapshot schema.
const FIELD_TAG_CREATE_TIME: &str = "tagCreateTime";
const FIELD_TAG_TIME_RETAINED: &str = "tagTimeRetained";

fn validate_tag_name(tag_name: &str) -> crate::Result<()> {
    let invalid = tag_name.trim().is_empty()
        || tag_name.trim_end() != tag_name
        || tag_name.contains('/')
        || tag_name.contains('\\')
        || tag_name.chars().any(char::is_control);
    if invalid {
        return Err(crate::Error::ConfigInvalid {
            message: format!("Invalid tag name: {tag_name:?}"),
        });
    }
    Ok(())
}

/// Decode a Jackson-serialized `LocalDateTime`.
///
/// Jackson's `LocalDateTimeSerializer` emits
/// `[year, month, day, hour, minute, second, nanoOfSecond]` and omits trailing
/// zero components, so the array may hold as few as five items. Anything that is
/// not such an array -- or that does not describe a real instant -- yields
/// `None` so one odd tag file cannot fail the whole listing.
fn parse_tag_create_time(value: &serde_json::Value) -> Option<chrono::NaiveDateTime> {
    let items = value.as_array()?;
    if items.len() < 5 || items.len() > 7 {
        return None;
    }
    let mut parts = [0i64; 7];
    for (slot, item) in parts.iter_mut().zip(items) {
        *slot = item.as_i64()?;
    }
    let [year, month, day, hour, minute, second, nano] = parts;

    let date = chrono::NaiveDate::from_ymd_opt(
        i32::try_from(year).ok()?,
        u32::try_from(month).ok()?,
        u32::try_from(day).ok()?,
    )?;
    let time = chrono::NaiveTime::from_hms_nano_opt(
        u32::try_from(hour).ok()?,
        u32::try_from(minute).ok()?,
        u32::try_from(second).ok()?,
        u32::try_from(nano).ok()?,
    )?;
    Some(date.and_time(time))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::CommitKind;
    use bytes::Bytes;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_snapshot(id: i64) -> Snapshot {
        Snapshot::builder()
            .version(3)
            .id(id)
            .schema_id(0)
            .base_manifest_list("base-list".to_string())
            .delta_manifest_list("delta-list".to_string())
            .commit_user("test-user".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000 * id as u64)
            .build()
    }

    async fn write_tag(file_io: &FileIO, tm: &TagManager, name: &str, snapshot: &Snapshot) {
        let path = tm.tag_path(name);
        let json = serde_json::to_string(snapshot).unwrap();
        let output = file_io.new_output(&path).unwrap();
        output.write(Bytes::from(json)).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_all_names_missing_dir_returns_empty() {
        let file_io = test_file_io();
        let tm = TagManager::new(file_io, "memory:/test_tag_missing".to_string());
        assert!(tm.list_all_names().await.unwrap().is_empty());
        assert!(tm.list_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_tag_operations_reject_unsafe_names() {
        let tm = TagManager::new(test_file_io(), "memory:/warehouse/table".to_string());
        let snapshot = test_snapshot(1);

        for name in ["", " ", "tag ", "nested/tag", "nested\\tag", "bad\nname"] {
            assert!(matches!(
                tm.create(name, &snapshot).await,
                Err(crate::Error::ConfigInvalid { .. })
            ));
            assert!(matches!(
                tm.tag_exists(name).await,
                Err(crate::Error::ConfigInvalid { .. })
            ));
            assert!(matches!(
                tm.get(name).await,
                Err(crate::Error::ConfigInvalid { .. })
            ));
            assert!(matches!(
                tm.delete(name).await,
                Err(crate::Error::ConfigInvalid { .. })
            ));
        }
    }

    #[tokio::test]
    async fn test_leading_whitespace_tag_name_compatibility() {
        let tm = TagManager::new(test_file_io(), "memory:/warehouse/table".to_string());
        let snapshot = test_snapshot(1);

        tm.create(" tag", &snapshot).await.unwrap();
        assert_eq!(tm.get(" tag").await.unwrap(), Some(snapshot));
        assert_eq!(tm.list_all().await.unwrap().len(), 1);
        assert_eq!(tm.list_all_with_metadata().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_trailing_whitespace_does_not_alias_tag() {
        let tm = TagManager::new(test_file_io(), "memory:/warehouse/table".to_string());
        let snapshot = test_snapshot(1);

        tm.create("tag", &snapshot).await.unwrap();
        assert!(matches!(
            tm.delete("tag ").await,
            Err(crate::Error::ConfigInvalid { .. })
        ));
        assert_eq!(tm.get("tag").await.unwrap(), Some(snapshot));
    }

    #[tokio::test]
    async fn test_list_all_names_sorted() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_sorted".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);
        for name in ["v3", "v1", "v2"] {
            write_tag(&file_io, &tm, name, &test_snapshot(1)).await;
        }
        assert_eq!(tm.list_all_names().await.unwrap(), vec!["v1", "v2", "v3"]);
    }

    /// Write a raw tag JSON built from a snapshot plus the two Java-only
    /// fields, so the on-disk shape matches what Flink/Spark produce.
    async fn write_tag_json(
        file_io: &FileIO,
        tm: &TagManager,
        name: &str,
        snapshot: &Snapshot,
        extra: &[(&str, serde_json::Value)],
    ) {
        let mut value = serde_json::to_value(snapshot).unwrap();
        let map = value.as_object_mut().unwrap();
        for (key, v) in extra {
            map.insert((*key).to_string(), v.clone());
        }
        let output = file_io.new_output(&tm.tag_path(name)).unwrap();
        output
            .write(Bytes::from(serde_json::to_vec(&value).unwrap()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_get_with_metadata_reads_java_fields() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_meta".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);

        // Jackson writes LocalDateTime as [y, mo, d, h, mi, s, nano] and omits
        // trailing zero components; Duration is decimal seconds.
        write_tag_json(
            &file_io,
            &tm,
            "full",
            &test_snapshot(1),
            &[
                (
                    "tagCreateTime",
                    serde_json::json!([2024, 1, 2, 3, 4, 5, 123_000_000]),
                ),
                ("tagTimeRetained", serde_json::json!(259_200.0)),
            ],
        )
        .await;

        let (snap, create_time, retained) = tm.get_with_metadata("full").await.unwrap().unwrap();
        assert_eq!(snap.id(), 1);
        // 2024-01-02T03:04:05.123 UTC
        assert_eq!(create_time, Some(1_704_164_645_123));
        assert_eq!(retained, Some(259_200.0));
    }

    #[tokio::test]
    async fn test_get_with_metadata_pads_truncated_time_array() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_meta_short".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);

        // Jackson drops the trailing zero second and nano, leaving five items.
        write_tag_json(
            &file_io,
            &tm,
            "short",
            &test_snapshot(2),
            &[("tagCreateTime", serde_json::json!([2024, 1, 2, 3, 4]))],
        )
        .await;

        let (_, create_time, retained) = tm.get_with_metadata("short").await.unwrap().unwrap();
        // 2024-01-02T03:04:00 UTC
        assert_eq!(create_time, Some(1_704_164_640_000));
        assert_eq!(retained, None, "absent retention stays absent");
    }

    #[tokio::test]
    async fn test_get_with_metadata_absent_fields_are_none() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_meta_absent".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);
        write_tag(&file_io, &tm, "plain", &test_snapshot(3)).await;

        let (snap, create_time, retained) = tm.get_with_metadata("plain").await.unwrap().unwrap();
        assert_eq!(snap.id(), 3);
        assert_eq!(create_time, None);
        assert_eq!(retained, None);
    }

    /// A malformed shape must not fail the whole read: the snapshot still loads
    /// and the unparsable field is reported as absent.
    #[tokio::test]
    async fn test_get_with_metadata_tolerates_bad_shapes() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_meta_bad".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);

        for (name, extra) in [
            (
                "iso_string",
                vec![("tagCreateTime", serde_json::json!("2024-01-02T03:04:05"))],
            ),
            (
                "too_short",
                vec![("tagCreateTime", serde_json::json!([2024, 1]))],
            ),
            (
                "impossible_date",
                vec![("tagCreateTime", serde_json::json!([2024, 13, 40, 3, 4]))],
            ),
            (
                "retained_string",
                vec![("tagTimeRetained", serde_json::json!("PT72H"))],
            ),
        ] {
            write_tag_json(&file_io, &tm, name, &test_snapshot(4), &extra).await;
            let (snap, create_time, retained) = tm.get_with_metadata(name).await.unwrap().unwrap();
            assert_eq!(snap.id(), 4, "{name}: snapshot must still load");
            assert!(
                create_time.is_none() && retained.is_none(),
                "{name}: unparsable metadata must be reported as absent"
            );
        }
    }

    #[tokio::test]
    async fn test_list_all_with_metadata_keeps_order() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_meta_list".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);

        write_tag_json(
            &file_io,
            &tm,
            "a",
            &test_snapshot(1),
            &[("tagTimeRetained", serde_json::json!(60.0))],
        )
        .await;
        write_tag(&file_io, &tm, "b", &test_snapshot(2)).await;

        let rows = tm.list_all_with_metadata().await.unwrap();
        let names: Vec<&str> = rows.iter().map(|(n, _, _, _)| n.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(rows[0].3, Some(60.0));
        assert_eq!(rows[1].3, None);
    }

    #[tokio::test]
    async fn test_list_all_loads_pairs() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_pairs".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);
        write_tag(&file_io, &tm, "a", &test_snapshot(1)).await;
        write_tag(&file_io, &tm, "b", &test_snapshot(2)).await;
        let pairs = tm.list_all().await.unwrap();
        let names: Vec<&str> = pairs.iter().map(|(n, _)| n.as_str()).collect();
        let ids: Vec<i64> = pairs.iter().map(|(_, s)| s.id()).collect();
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(ids, vec![1, 2]);
    }
}
