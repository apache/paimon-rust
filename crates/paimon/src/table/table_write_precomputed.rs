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
    pub(in crate::table) fn set_restore_snapshot(&mut self, snapshot_id: i64) {
        self.restore_snapshot_id = Some(snapshot_id);
        if let BucketAssignerEnum::Dynamic(assigner) = &mut self.bucket_assigner {
            assigner.set_restore_snapshot(snapshot_id);
        }
        // Java WriteRestore restores data files and indexes from the same snapshot.
    }

    pub(super) async fn write_precomputed_bucket(
        &mut self,
        batch: &RecordBatch,
        bucket: i32,
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
        let Some(batch) = self.normalize_write_batch(batch)? else {
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
        self.written = true;
        if let BucketAssignerEnum::Dynamic(assigner) = &mut self.bucket_assigner {
            if let Err(error) = assigner
                .notify_precomputed_batch(&batch, partition, bucket)
                .await
            {
                self.fail_write().await;
                return Err(error);
            }
        }
        self.write_bucket(partition.clone(), bucket, batch).await
    }
}
