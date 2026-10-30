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

//! Snapshot-bound dynamic bucket assignment before distributed file writing.

use std::collections::{HashMap, HashSet};

use arrow_array::{Int32Array, RecordBatch};

use super::dynamic_bucket_assigner_router::DynamicBucketAssignerRouter;
use super::dynamic_bucket_index::DynamicBucketIndex;
use super::write_builder::ensure_table_write_authorized;
use crate::spec::{CoreOptions, PartitionComputer};
use crate::table::Table;
use crate::Result;

/// Assign dynamic buckets using the same key ownership and bucket allocation
/// as Java's HashBucketAssigner. This stage produces no files or commit
/// messages. The final table writer maintains complete HASH index files after
/// inputs have been regrouped by their assigned partition and bucket.
pub struct DynamicBucketAssigner {
    router: DynamicBucketAssignerRouter,
    index: DynamicBucketIndex,
    num_assigners: i32,
    assign_id: i32,
    restore_snapshot_id: i64,
}

/// Configuration for a distributed dynamic-bucket assigner.
///
/// The restore snapshot is captured by [`Self::build`]. It is independent of
/// writer configuration and must also be supplied to the eventual file writers.
pub struct DynamicBucketAssignerBuilder<'a> {
    table: &'a Table,
    num_channels: i32,
    num_assigners: i32,
    assign_id: i32,
    restore_snapshot_id: Option<i64>,
    overwrite: bool,
}

impl DynamicBucketAssignerBuilder<'_> {
    /// Restore a specific snapshot. Zero denotes an empty restore state;
    /// without this option, [`Self::build`] captures the latest snapshot.
    pub fn with_restore_snapshot(mut self, snapshot_id: i64) -> Result<Self> {
        if snapshot_id < 0 {
            return Err(crate::Error::DataInvalid {
                message: "Restore snapshot id must not be negative".into(),
                source: None,
            });
        }
        self.restore_snapshot_id = Some(snapshot_id);
        Ok(self)
    }

    /// Assign against empty state, ignoring any configured restore snapshot.
    pub fn with_overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    /// Authorize and capture the immutable restore point used by this assigner.
    pub async fn build(self) -> Result<DynamicBucketAssigner> {
        DynamicBucketAssigner::new(
            self.table,
            self.num_channels,
            self.num_assigners,
            self.assign_id,
            self.restore_snapshot_id,
            self.overwrite,
        )
        .await
    }
}

impl DynamicBucketAssigner {
    /// Configure the dynamic-bucket assignment stage separately from file writing.
    pub fn builder(
        table: &Table,
        num_channels: i32,
        num_assigners: i32,
        assign_id: i32,
    ) -> DynamicBucketAssignerBuilder<'_> {
        DynamicBucketAssignerBuilder {
            table,
            num_channels,
            num_assigners,
            assign_id,
            restore_snapshot_id: None,
            overwrite: false,
        }
    }

    async fn new(
        table: &Table,
        num_channels: i32,
        num_assigners: i32,
        assign_id: i32,
        restore_snapshot_id: Option<i64>,
        overwrite: bool,
    ) -> Result<Self> {
        ensure_table_write_authorized(table)?;
        let router = DynamicBucketAssignerRouter::new(table.schema(), num_channels, num_assigners)?;
        if !(0..num_channels).contains(&assign_id) {
            return Err(crate::Error::DataInvalid {
                message: format!("assign_id must be in [0, {num_channels}), got {assign_id}"),
                source: None,
            });
        }
        let schema = table.schema();
        let fields = schema.fields();
        let indices = |names: &[String]| {
            names
                .iter()
                .map(|name| {
                    fields
                        .iter()
                        .position(|field| field.name() == name)
                        .unwrap()
                })
                .collect::<Vec<_>>()
        };
        let options = CoreOptions::new(schema.options());
        let partition_computer = PartitionComputer::new(
            schema.partition_keys(),
            fields,
            options.partition_default_name(),
            options.legacy_partition_name(),
        )?;
        let mut index = DynamicBucketIndex::new(
            table.clone(),
            indices(schema.partition_keys()),
            indices(&schema.trimmed_primary_keys()),
            overwrite,
            partition_computer,
        )?;
        let manager = table.snapshot_manager();
        let snapshot = match (overwrite, restore_snapshot_id) {
            (true, _) | (_, Some(0)) => None,
            (_, Some(id)) => Some(manager.get_snapshot(id).await?),
            (_, None) => manager.get_latest_snapshot().await?,
        };
        let restore_snapshot_id = snapshot.as_ref().map_or(0, |snapshot| snapshot.id());
        index.pin_restore_snapshot(snapshot);
        Ok(Self {
            router,
            index,
            num_assigners,
            assign_id,
            restore_snapshot_id,
        })
    }

    /// Fixed restore point shared with the eventual file writers. Zero means
    /// empty restore state, including overwrite, and must not resolve latest.
    pub fn restore_snapshot_id(&self) -> i64 {
        self.restore_snapshot_id
    }

    /// Return non-null bucket IDs in input order. Supplied hashes avoid
    /// reading or normalizing primary-key columns again; only partition
    /// columns are then required. Validate the entire input's owner and hash
    /// arrays before restoring any index state or assigning any new key.
    pub async fn assign(
        &mut self,
        batch: &RecordBatch,
        key_hashes: Option<&Int32Array>,
    ) -> Result<Int32Array> {
        let (partitions, routing) = self.router.route_with_partitions(batch, key_hashes)?;
        for owner in routing.assigners.values() {
            if *owner != self.assign_id {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Record assigner {owner} does not match writer assigner {}",
                        self.assign_id
                    ),
                    source: None,
                });
            }
        }
        let mut requested: HashMap<Vec<u8>, HashSet<i32>> = HashMap::new();
        for (partition, hash) in partitions.iter().zip(routing.key_hashes.values()) {
            requested
                .entry(partition.clone())
                .or_default()
                .insert(*hash);
        }
        self.index
            .restore_input_hashes(&requested, self.num_assigners, self.assign_id)
            .await?;
        partitions
            .iter()
            .zip(routing.key_hashes.values())
            .map(|(partition, hash)| {
                self.index
                    .assign_input_hash(partition, *hash, self.num_assigners, self.assign_id)
            })
            .collect::<Result<Vec<_>>>()
            .map(Int32Array::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, IntType, Schema, TableSchema};
    use arrow_array::{ArrayRef, RecordBatchOptions};
    use arrow_schema::Schema as ArrowSchema;
    use std::sync::Arc;

    fn table(name: &str) -> Table {
        let schema = Schema::builder()
            .option("write-only", "true")
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "-1")
            .option("dynamic-bucket.target-row-num", "1")
            .option("dynamic-bucket.max-buckets", "4")
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", name),
            format!("memory:/{name}"),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn batch(ids: &[i32]) -> RecordBatch {
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef),
            (
                "value",
                Arc::new(Int32Array::from(vec![10; ids.len()])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn keys_for_owner(table: &Table, owner: i32) -> (Vec<i32>, Int32Array) {
        let input = batch(&(0..32).collect::<Vec<_>>());
        let routing = DynamicBucketAssignerRouter::new(table.schema(), 2, 2)
            .unwrap()
            .route(&input)
            .unwrap();
        let positions = routing
            .assigners
            .values()
            .iter()
            .enumerate()
            .filter_map(|(index, assigner)| (*assigner == owner).then_some(index))
            .collect::<Vec<_>>();
        assert!(positions.len() >= 3);
        (
            positions.iter().map(|index| *index as i32).collect(),
            Int32Array::from_iter_values(
                positions
                    .iter()
                    .map(|index| routing.key_hashes.value(*index)),
            ),
        )
    }

    async fn seed(table: &Table, ids: &[i32], bucket: i32) {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow(&[batch(ids)], Some(bucket))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        builder.new_commit().commit(messages).await.unwrap();
        writer.close().await;
    }

    #[tokio::test]
    async fn input_assignment_preserves_owned_holes_foreign_keys_and_later_batches() {
        let table = table("input_owned_holes");
        let (keys, hashes) = keys_for_owner(&table, 0);
        // A changed parallelism can route this historical key to assigner 0
        // even though it was originally written to foreign bucket 3.
        seed(&table, &keys[..1], 3).await;
        let mut assigner = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .build()
            .await
            .unwrap();
        assert_eq!(assigner.restore_snapshot_id(), 1);
        let routed = assigner
            .assign(&batch(&keys[..2]), Some(&hashes.slice(0, 2)))
            .await
            .unwrap();
        assert_eq!(routed.values(), &[3, 0]);
        // Repeating the historical key does not increment a bucket's count;
        // the next new key fills unused owned bucket 2 despite global id 3.
        let routed = assigner
            .assign(&batch(&[keys[0], keys[2], keys[1]]), None)
            .await
            .unwrap();
        assert_eq!(routed.values(), &[3, 2, 0]);
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id(),
            1
        );
        // Input assignment never writes HASH files or exposes commit messages.
        let fresh = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .build()
            .await
            .unwrap();
        assert_eq!(fresh.restore_snapshot_id(), 1);
    }

    #[tokio::test]
    async fn input_assignment_restores_new_requested_hashes_without_resetting_counts() {
        let table = table("input_requested_hashes");
        let (keys, _) = keys_for_owner(&table, 0);
        seed(&table, &keys[..1], 3).await;
        seed(&table, &keys[1..2], 2).await;
        let mut assigner = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .build()
            .await
            .unwrap();
        assert_eq!(
            assigner
                .assign(&batch(&keys[..1]), None)
                .await
                .unwrap()
                .values(),
            &[3]
        );
        // This hash was not retained during the first call's restore.
        assert_eq!(
            assigner
                .assign(&batch(&keys[1..2]), None)
                .await
                .unwrap()
                .values(),
            &[2]
        );
        assert_eq!(
            assigner
                .assign(&batch(&keys[2..3]), None)
                .await
                .unwrap()
                .values(),
            &[0]
        );
    }

    #[tokio::test]
    async fn input_assignment_pins_latest_and_empty_restore_points() {
        let table = table("input_snapshot_pin");
        let (keys, _) = keys_for_owner(&table, 0);
        assert!(DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .with_restore_snapshot(-1)
            .err()
            .unwrap()
            .to_string()
            .contains("must not be negative"));
        let mut empty = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .build()
            .await
            .unwrap();
        assert_eq!(empty.restore_snapshot_id(), 0);
        // Creating the builder does not capture an earlier restore point.
        let pending = DynamicBucketAssigner::builder(&table, 2, 2, 0);
        seed(&table, &keys[..1], 3).await;
        let mut pinned = pending.build().await.unwrap();
        assert_eq!(pinned.restore_snapshot_id(), 1);
        seed(&table, &keys[1..2], 2).await;
        assert_eq!(
            empty
                .assign(&batch(&keys[..1]), None)
                .await
                .unwrap()
                .values(),
            &[0]
        );
        assert_eq!(
            pinned
                .assign(&batch(&keys[1..2]), None)
                .await
                .unwrap()
                .values(),
            &[0]
        );
        let mut explicit = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .with_restore_snapshot(1)
            .unwrap()
            .build()
            .await
            .unwrap();
        assert_eq!(explicit.restore_snapshot_id(), 1);
        assert_eq!(
            explicit
                .assign(&batch(&keys[1..2]), None)
                .await
                .unwrap()
                .values(),
            &[0]
        );
        let mut latest = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .build()
            .await
            .unwrap();
        assert_eq!(latest.restore_snapshot_id(), 2);
        assert_eq!(
            latest
                .assign(&batch(&keys[1..2]), None)
                .await
                .unwrap()
                .values(),
            &[2]
        );
        let mut explicit_empty = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .with_restore_snapshot(0)
            .unwrap()
            .build()
            .await
            .unwrap();
        assert_eq!(explicit_empty.restore_snapshot_id(), 0);
        assert_eq!(
            explicit_empty
                .assign(&batch(&keys[..1]), None)
                .await
                .unwrap()
                .values(),
            &[0]
        );
        let mut overwrite = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .with_restore_snapshot(999)
            .unwrap()
            .with_overwrite()
            .build()
            .await
            .unwrap();
        assert_eq!(overwrite.restore_snapshot_id(), 0);
        assert_eq!(
            overwrite
                .assign(&batch(&keys[..1]), None)
                .await
                .unwrap()
                .values(),
            &[0]
        );
    }

    #[tokio::test]
    async fn input_assignment_checks_all_owners_and_hashes_before_state_changes() {
        let table = table("input_validate_before_assignment");
        let (keys, hashes) = keys_for_owner(&table, 0);
        let (foreign, _) = keys_for_owner(&table, 1);
        let mut assigner = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .build()
            .await
            .unwrap();
        let error = assigner
            .assign(&batch(&[keys[0], foreign[0]]), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("does not match"));
        let error = assigner
            .assign(&batch(&keys[..2]), Some(&hashes.slice(0, 1)))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("length") || error.to_string().contains("row"));
        let error = assigner
            .assign(
                &batch(&keys[..2]),
                Some(&Int32Array::from(vec![Some(hashes.value(0)), None])),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("null") || error.to_string().contains("NULL"));
        // None of the rejected calls consumed the first bucket with keys[0].
        assert_eq!(
            assigner
                .assign(&batch(&keys[1..2]), None)
                .await
                .unwrap()
                .values(),
            &[0]
        );
    }

    #[tokio::test]
    async fn prehashed_unpartitioned_input_keeps_zero_column_batch_rows() {
        let table = table("input_prehashed_zero_columns");
        let (_, hashes) = keys_for_owner(&table, 0);
        let input = RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()),
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(2)),
        )
        .unwrap();
        let mut assigner = DynamicBucketAssigner::builder(&table, 2, 2, 0)
            .build()
            .await
            .unwrap();
        assert_eq!(
            assigner
                .assign(&input, Some(&hashes.slice(0, 2)))
                .await
                .unwrap()
                .values(),
            &[0, 2]
        );
        assert!(assigner.assign(&input, None).await.is_err());
        assert!(!table
            .file_io()
            .exists_dir("memory:/input_prehashed_zero_columns/index")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn input_assignment_rejects_data_without_complete_hash_indexes() {
        let table = table("input_missing_hash_index");
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow(&[batch(&[1])], Some(0)).await.unwrap();
        let mut messages = writer.prepare_commit().await.unwrap();
        for message in &mut messages {
            message.new_index_files.clear();
        }
        builder.new_commit().commit(messages).await.unwrap();
        writer.close().await;
        let mut assigner = DynamicBucketAssigner::builder(&table, 1, 1, 0)
            .build()
            .await
            .unwrap();
        assert!(assigner
            .assign(&batch(&[1]), None)
            .await
            .unwrap_err()
            .to_string()
            .contains("no complete HASH"));
        let mut overwrite = DynamicBucketAssigner::builder(&table, 1, 1, 0)
            .with_overwrite()
            .build()
            .await
            .unwrap();
        assert_eq!(
            overwrite.assign(&batch(&[1]), None).await.unwrap().values(),
            &[0]
        );
    }

    #[tokio::test]
    async fn input_assignment_validates_duplicate_and_corrupt_restored_hash_indexes() {
        for duplicate in [true, false] {
            let table = table(if duplicate {
                "input_duplicate_hash"
            } else {
                "input_corrupt_hash"
            });
            seed(&table, &[1], 0).await;
            let manager = table.snapshot_manager();
            let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
            let path = manager.manifest_path(snapshot.index_manifest().unwrap());
            let mut entries = crate::spec::IndexManifest::read(table.file_io(), &path)
                .await
                .unwrap();
            assert_eq!(entries.len(), 1);
            if duplicate {
                entries.push(entries[0].clone());
            } else {
                entries[0].index_file.row_count += 1;
            }
            crate::spec::IndexManifest::write(table.file_io(), &path, &entries)
                .await
                .unwrap();
            let mut assigner = DynamicBucketAssigner::builder(&table, 1, 1, 0)
                .build()
                .await
                .unwrap();
            let error = assigner.assign(&batch(&[1]), None).await.unwrap_err();
            assert!(error.to_string().contains(if duplicate {
                "Multiple HASH"
            } else {
                "Corrupt HASH"
            }));
        }
    }

    #[tokio::test]
    async fn input_assignment_uses_write_identity_without_a_select_grant() {
        let unbound = table("input_unbound_auth").copy_with_options(HashMap::from([(
            "query-auth.enabled".into(),
            "true".into(),
        )]));
        assert!(DynamicBucketAssigner::builder(&unbound, 1, 1, 0)
            .with_restore_snapshot(0)
            .unwrap()
            .build()
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("catalog-loaded REST"));
        // This REST identity intentionally has no running query endpoint or
        // SELECT grant. Empty restore state must not issue a query-auth call.
        let rest = crate::table::rest_query_auth_table().await;
        let bound = Table {
            rest_env: rest.rest_env,
            ..unbound
        }
        .with_query_auth_session();
        let mut assigner = DynamicBucketAssigner::builder(&bound, 1, 1, 0)
            .with_restore_snapshot(0)
            .unwrap()
            .build()
            .await
            .unwrap();
        assert_eq!(
            assigner.assign(&batch(&[1]), None).await.unwrap().values(),
            &[0]
        );
    }
}
