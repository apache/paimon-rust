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

use std::sync::OnceLock;
use std::time::Duration;

const VECTOR_INDEX_BUILD_TIMING_ENV: &str = "PAIMON_LOG_VECTOR_INDEX_BUILD_TIMING";

pub(super) fn vector_index_build_timing_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os(VECTOR_INDEX_BUILD_TIMING_ENV).is_some_and(|value| value == "1")
    })
}

pub(super) struct VectorIndexBuildTiming {
    pub(super) total_without_commit: Duration,
    pub(super) source_batch_wait: Duration,
    pub(super) oss_read: Duration,
    pub(super) oss_read_bytes: u64,
    pub(super) oss_range_requests: u64,
    pub(super) parquet_decode: Duration,
    pub(super) file_schema_open: Duration,
    pub(super) first_batch_wait: Duration,
    pub(super) remaining_batch_wait: Duration,
    pub(super) parquet_row_group_count: u64,
    pub(super) parquet_projected_bytes_min: u64,
    pub(super) parquet_projected_bytes_max: u64,
    pub(super) parquet_projected_bytes_total: u64,
    pub(super) parquet_peak_inflight_row_groups: usize,
    pub(super) raw_temp_write: Duration,
    pub(super) capability_check: Duration,
    pub(super) sample_read: Duration,
    pub(super) train_finish: Duration,
    pub(super) raw_temp_reread: Duration,
    pub(super) index_add: Duration,
    pub(super) full_scan_add: Duration,
    pub(super) pipeline_blocked: Duration,
    pub(super) producer_blocked: Duration,
    pub(super) consumer_validate_add: Duration,
    pub(super) serialize_upload: Duration,
    pub(super) rows: usize,
    pub(super) training_rows_seen: usize,
    pub(super) training_rows_retained: usize,
    pub(super) batch_count: usize,
    pub(super) batch_bytes_min: usize,
    pub(super) batch_bytes_max: usize,
    pub(super) batch_bytes_total: usize,
    pub(super) peak_ready_batches: usize,
    pub(super) raw_temp_bytes: usize,
    pub(super) index_bytes: u64,
    pub(super) data_file_count: usize,
    pub(super) file_name: String,
}

impl VectorIndexBuildTiming {
    pub(super) fn log(self, index_type: &str, commit: Duration) {
        let total = self.total_without_commit.saturating_add(commit);
        let build = self
            .capability_check
            .saturating_add(if self.raw_temp_bytes != 0 {
                self.source_batch_wait
                    .saturating_add(self.raw_temp_write)
                    .saturating_add(self.train_finish)
                    .saturating_add(self.raw_temp_reread)
                    .saturating_add(self.index_add)
            } else {
                self.sample_read
                    .saturating_add(self.train_finish)
                    .saturating_add(self.full_scan_add)
            });
        let accounted = build
            .saturating_add(self.serialize_upload)
            .saturating_add(commit);
        let unattributed = total.saturating_sub(accounted);
        eprintln!(
            "event=paimon_vector_index_build index_type={} file={} rows={} training_rows_seen={} training_rows_retained={} batch_count={} batch_bytes_min={} batch_bytes_max={} batch_bytes_total={} raw_temp_bytes={} index_bytes={} source_batch_wait_ms={:.3} oss_read_ms={:.3} oss_read_bytes={} oss_range_requests={} parquet_decode_ms={:.3} file_schema_open_ms={:.3} first_batch_wait_ms={:.3} remaining_batch_wait_ms={:.3} parquet_row_group_count={} parquet_projected_bytes_min={} parquet_projected_bytes_max={} parquet_projected_bytes_total={} parquet_peak_inflight_row_groups={} raw_temp_write_ms={:.3} capability_check_ms={:.3} train_finish_ms={:.3} raw_temp_reread_ms={:.3} index_add_ms={:.3} serialize_upload_ms={:.3} commit_ms={:.3} sample_read_ms={:.3} full_scan_add_ms={:.3} pipeline_blocked_ms={:.3} producer_blocked_ms={:.3} consumer_validate_add_ms={:.3} data_file_count={} data_file_read_concurrency=1 peak_ready_batches={} total_ms={:.3} unattributed_ms={:.3}",
            index_type,
            self.file_name,
            self.rows,
            self.training_rows_seen,
            self.training_rows_retained,
            self.batch_count,
            self.batch_bytes_min,
            self.batch_bytes_max,
            self.batch_bytes_total,
            self.raw_temp_bytes,
            self.index_bytes,
            self.source_batch_wait.as_secs_f64() * 1000.0,
            self.oss_read.as_secs_f64() * 1000.0,
            self.oss_read_bytes,
            self.oss_range_requests,
            self.parquet_decode.as_secs_f64() * 1000.0,
            self.file_schema_open.as_secs_f64() * 1000.0,
            self.first_batch_wait.as_secs_f64() * 1000.0,
            self.remaining_batch_wait.as_secs_f64() * 1000.0,
            self.parquet_row_group_count,
            self.parquet_projected_bytes_min,
            self.parquet_projected_bytes_max,
            self.parquet_projected_bytes_total,
            self.parquet_peak_inflight_row_groups,
            self.raw_temp_write.as_secs_f64() * 1000.0,
            self.capability_check.as_secs_f64() * 1000.0,
            self.train_finish.as_secs_f64() * 1000.0,
            self.raw_temp_reread.as_secs_f64() * 1000.0,
            self.index_add.as_secs_f64() * 1000.0,
            self.serialize_upload.as_secs_f64() * 1000.0,
            commit.as_secs_f64() * 1000.0,
            self.sample_read.as_secs_f64() * 1000.0,
            self.full_scan_add.as_secs_f64() * 1000.0,
            self.pipeline_blocked.as_secs_f64() * 1000.0,
            self.producer_blocked.as_secs_f64() * 1000.0,
            self.consumer_validate_add.as_secs_f64() * 1000.0,
            self.data_file_count,
            self.peak_ready_batches,
            total.as_secs_f64() * 1000.0,
            unattributed.as_secs_f64() * 1000.0,
        );
    }
}
