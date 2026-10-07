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

//! Java TableWrite.write(row, bucket) for groups assigned upstream.

use super::{BucketAssignerEnum, CoreOptions, RecordBatch, Result, TableWrite};
use crate::spec::{batch_to_serialized_bytes, EMPTY_SERIALIZED_ROW, MAX_DYNAMIC_BUCKETS};

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

impl TableWrite {
    /// Pin HASH-index restoration to the coordinator's base snapshot. Zero
    /// denotes an empty table; None resolves the latest snapshot now. Configure
    /// this before writing, including when the coordinator overwrites old data.
    /// Normal dynamic writes already maintain HASH indexes without this call.
    pub async fn with_dynamic_bucket_index(
        &mut self,
        ignore_existing: bool,
        base_snapshot_id: Option<i64>,
    ) -> Result<&mut Self> {
        self.ensure_active()?;
        if self.written {
            return Err(invalid(
                "Dynamic bucket index maintenance must be enabled before writing",
            ));
        }
        if !matches!(self.bucket_assigner, BucketAssignerEnum::Dynamic(_)) {
            return Err(invalid(
                "Dynamic bucket index maintenance is only valid for HASH_DYNAMIC tables",
            ));
        }
        let manager = self.table.snapshot_manager();
        let snapshot = match base_snapshot_id {
            Some(id) if id < 0 => return Err(invalid("Base snapshot id must not be negative")),
            Some(0) => None,
            Some(id) => Some(manager.get_snapshot(id).await?),
            None => manager.get_latest_snapshot().await?,
        };
        if let BucketAssignerEnum::Dynamic(assigner) = &mut self.bucket_assigner {
            assigner.configure_index(ignore_existing || self.is_overwrite, snapshot);
        }
        // Java restores sequence numbers from current data files when a writer
        // is created. The coordinator's base pins only HASH-index restoration.
        Ok(self)
    }

    /// Write one complete partition/bucket group without assigning buckets
    /// again. This is the batch counterpart of Java TableWrite.write(row, bucket).
    /// HASH_DYNAMIC groups maintain the bucket's complete HASH index. Optional
    /// hashes are Java BinaryRow key hashes carried through the shuffle. Flags
    /// are upstream hints: every surviving key is notified, as in Java. Filtering
    /// can discard the row which first created a mapping. Metadata follows input
    /// row order, before any RowKind filtering.
    pub async fn write_arrow_batch_to_bucket(
        &mut self,
        batch: &RecordBatch,
        bucket: i32,
        key_hashes: Option<&[i32]>,
        new_mappings: Option<&[bool]>,
    ) -> Result<()> {
        self.ensure_active()?;
        let dynamic = matches!(self.bucket_assigner, BucketAssignerEnum::Dynamic(_));
        let total_buckets = CoreOptions::new(self.table.schema().options()).bucket();
        if self.format_writer.is_some()
            || matches!(self.bucket_assigner, BucketAssignerEnum::CrossPartition(_))
            || (!dynamic && total_buckets <= 0)
        {
            return Err(invalid(
                "Precomputed bucket writes are only valid for HASH_FIXED or HASH_DYNAMIC tables",
            ));
        }
        let upper_bound = if dynamic {
            MAX_DYNAMIC_BUCKETS
        } else {
            total_buckets
        };
        if !(0..upper_bound).contains(&bucket) {
            return Err(invalid(format!(
                "Bucket id must be between 0 and {}, but was {bucket}",
                upper_bound - 1,
            )));
        }
        if !dynamic && (key_hashes.is_some() || new_mappings.is_some()) {
            return Err(invalid(
                "Precomputed key hashes and new-mapping flags are only valid for HASH_DYNAMIC tables",
            ));
        }
        if new_mappings.is_some() && key_hashes.is_none() {
            return Err(invalid("Precomputed new-mapping flags require key hashes"));
        }
        for (kind, len) in [
            ("key hash", key_hashes.map(<[i32]>::len)),
            ("new-mapping", new_mappings.map(<[bool]>::len)),
        ] {
            if let Some(len) = len {
                if len != batch.num_rows() {
                    return Err(invalid(format!(
                        "Precomputed {kind} count {len} does not match row count {}",
                        batch.num_rows(),
                    )));
                }
            }
        }
        let Some((batch, selection)) = self.normalize_write_batch_with_selection(batch)? else {
            return Ok(());
        };
        let partition_indices = self
            .partition_keys
            .iter()
            .map(|name| {
                self.write_fields
                    .iter()
                    .position(|field| field.name() == name)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let partitions = if partition_indices.is_empty() {
            vec![EMPTY_SERIALIZED_ROW.clone(); batch.num_rows()]
        } else {
            batch_to_serialized_bytes(&batch, &partition_indices, &self.write_fields)?
        };
        let partition = &partitions[0];
        if partitions.iter().any(|actual| actual != partition) {
            return Err(invalid(
                "A precomputed bucket group contained multiple partitions",
            ));
        }
        let selected_hashes = selection.as_ref().and_then(|rows| {
            key_hashes.map(|hashes| rows.iter().map(|&row| hashes[row]).collect::<Vec<_>>())
        });
        self.written = true;
        if let BucketAssignerEnum::Dynamic(assigner) = &mut self.bucket_assigner {
            if let Err(error) = assigner
                .notify_precomputed_batch(
                    &batch,
                    partition,
                    bucket,
                    selected_hashes.as_deref().or(key_hashes),
                )
                .await
            {
                self.fail_write().await;
                return Err(error);
            }
        }
        self.write_bucket(partition.clone(), bucket, batch).await
    }
}
