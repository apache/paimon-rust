// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Stateful continuous-read C ABI.
//!
//! This is deliberately a pull API. It does not create a C++ callback thread,
//! so callers retain control of cancellation, backpressure and checkpoint
//! barriers. All handles in this module are single-thread-confined.

use std::ffi::c_void;
use std::mem::size_of;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use paimon::table::{
    ArrowRecordBatchStream, IncrementalPlan, IncrementalScanMode, IncrementalSplit, Plan,
    StreamPlan, StreamScan, StreamScanFollowUpMode, StreamScanPoll, StreamScanStartupMode,
    TableRead,
};
use paimon::DataSplit;
use serde::{Deserialize, Serialize};

use crate::error::{check_non_null, paimon_error, PaimonErrorCode};
use crate::result::{paimon_result_bytes, paimon_result_record_batch_reader};
use crate::runtime;
use crate::types::{
    paimon_bytes, paimon_read_builder, paimon_record_batch_reader, paimon_table_read,
    read_builder_fingerprint, ReadBuilderState, TableReadState,
};

pub const PAIMON_STREAM_STARTUP_LATEST_FULL: i32 = 0;
pub const PAIMON_STREAM_STARTUP_LATEST: i32 = 1;
pub const PAIMON_STREAM_STARTUP_FROM_SNAPSHOT: i32 = 2;
pub const PAIMON_STREAM_STARTUP_FROM_SNAPSHOT_FULL: i32 = 3;

pub const PAIMON_STREAM_FOLLOW_UP_AUTO: i32 = 0;
pub const PAIMON_STREAM_FOLLOW_UP_DELTA: i32 = 1;
pub const PAIMON_STREAM_FOLLOW_UP_CHANGELOG: i32 = 2;

pub const PAIMON_STREAM_POLL_DATA: i32 = 0;
pub const PAIMON_STREAM_POLL_WAITING: i32 = 1;
pub const PAIMON_STREAM_POLL_END: i32 = 2;

pub const PAIMON_STREAM_READ_DATA: i32 = 0;
pub const PAIMON_STREAM_READ_AUDIT_LOG: i32 = 1;

/// Extensible options for a continuous scan.
///
/// Initialize this with `paimon_stream_scan_options_init`; future versions may
/// consume fields from `reserved` while preserving this prefix.
#[repr(C)]
pub struct paimon_stream_scan_options {
    pub struct_size: u32,
    pub startup_mode: i32,
    pub follow_up_mode: i32,
    pub snapshot_id: i64,
    pub reserved: [u64; 4],
}

#[repr(C)]
pub struct paimon_stream_scan {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_stream_plan {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_result_stream_scan {
    pub scan: *mut paimon_stream_scan,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_stream_poll {
    pub status: i32,
    pub plan: *mut paimon_stream_plan,
    pub snapshot_id: i64,
    pub next_snapshot_id: i64,
    pub watermark: i64,
    pub has_watermark: u8,
    pub reserved: [u8; 7],
    pub error: *mut paimon_error,
}

const STREAM_PLAN_FORMAT: &str = "paimon-rust-stream-plan";
// Version 3 also binds restored work to the table branch. Earlier versions are
// rejected because location + schema id alone cannot distinguish two branches.
const STREAM_PLAN_VERSION: u32 = 3;
const MAX_STREAM_PLAN_BYTES: usize = 64 * 1024 * 1024;
const MAX_STREAM_PLAN_SPLITS: usize = 100_000;
const MAX_STREAM_SPLIT_BYTES: usize = 16 * 1024 * 1024;
const MAX_STREAM_SPLIT_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const MAX_STREAM_IDENTITY_BYTES: usize = 1024 * 1024;

struct StreamScanState {
    scan: StreamScan,
    table_location: String,
    table_branch: String,
    schema_id: i64,
    read_fingerprint: String,
}

struct StreamPlanState {
    plan: StreamPlan,
    table_location: String,
    table_branch: String,
    schema_id: i64,
    read_fingerprint: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamPlanEnvelope {
    format: String,
    version: u32,
    table_location: String,
    #[serde(default)]
    table_branch: String,
    schema_id: i64,
    read_fingerprint: String,
    kind: i32,
    incremental_mode: i32,
    snapshot_id: i64,
    next_snapshot_id: i64,
    watermark: Option<i64>,
    #[serde(with = "bounded_splits")]
    splits: Vec<Vec<u8>>,
}

mod bounded_splits {
    use std::fmt;

    use serde::de::{DeserializeSeed, Error, IgnoredAny, SeqAccess, Visitor};
    use serde::{Deserializer, Serialize, Serializer};

    use super::{MAX_STREAM_PLAN_SPLITS, MAX_STREAM_SPLIT_BYTES, MAX_STREAM_SPLIT_TOTAL_BYTES};

    pub fn serialize<S>(splits: &[Vec<u8>], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        splits.serialize(serializer)
    }

    struct SplitBytesSeed;

    impl<'de> DeserializeSeed<'de> for SplitBytesSeed {
        type Value = Vec<u8>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(SplitBytesVisitor)
        }
    }

    struct SplitBytesVisitor;

    impl<'de> Visitor<'de> for SplitBytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded stream split byte array")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_STREAM_SPLIT_BYTES),
            );
            while bytes.len() < MAX_STREAM_SPLIT_BYTES {
                let Some(value) = sequence.next_element::<u8>()? else {
                    return Ok(bytes);
                };
                bytes.push(value);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format!(
                    "stream plan split exceeds {MAX_STREAM_SPLIT_BYTES} bytes"
                )));
            }
            Ok(bytes)
        }
    }

    struct SplitsVisitor;

    impl<'de> Visitor<'de> for SplitsVisitor {
        type Value = Vec<Vec<u8>>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded list of stream split byte arrays")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut splits = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_STREAM_PLAN_SPLITS),
            );
            let mut total_bytes = 0usize;
            while splits.len() < MAX_STREAM_PLAN_SPLITS {
                let Some(split) = sequence.next_element_seed(SplitBytesSeed)? else {
                    return Ok(splits);
                };
                total_bytes = total_bytes
                    .checked_add(split.len())
                    .ok_or_else(|| A::Error::custom("stream plan split byte count overflows"))?;
                if total_bytes > MAX_STREAM_SPLIT_TOTAL_BYTES {
                    return Err(A::Error::custom(format!(
                        "stream plan split bytes exceed {MAX_STREAM_SPLIT_TOTAL_BYTES}"
                    )));
                }
                splits.push(split);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format!(
                    "stream plan contains more than {MAX_STREAM_PLAN_SPLITS} splits"
                )));
            }
            Ok(splits)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(SplitsVisitor)
    }
}

fn validate_stream_plan_envelope(envelope: &StreamPlanEnvelope) -> Result<(), *mut paimon_error> {
    if envelope.table_location.is_empty()
        || envelope.table_location.len() > MAX_STREAM_IDENTITY_BYTES
        || envelope.table_branch.is_empty()
        || envelope.table_branch.len() > MAX_STREAM_IDENTITY_BYTES
        || envelope.schema_id < 0
        || envelope.read_fingerprint.is_empty()
        || envelope.read_fingerprint.len() > MAX_STREAM_IDENTITY_BYTES
    {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "stream plan contains an invalid table or read identity".to_string(),
        ));
    }
    if envelope.splits.len() > MAX_STREAM_PLAN_SPLITS {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!(
                "stream plan contains {} splits; maximum is {}",
                envelope.splits.len(),
                MAX_STREAM_PLAN_SPLITS
            ),
        ));
    }
    if envelope
        .splits
        .iter()
        .any(|split| split.len() > MAX_STREAM_SPLIT_BYTES)
    {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!("stream plan split exceeds {MAX_STREAM_SPLIT_BYTES} bytes"),
        ));
    }
    let total_split_bytes = envelope
        .splits
        .iter()
        .try_fold(0usize, |total, split| total.checked_add(split.len()));
    if total_split_bytes.is_none_or(|total| total > MAX_STREAM_SPLIT_TOTAL_BYTES) {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!("stream plan split bytes exceed {MAX_STREAM_SPLIT_TOTAL_BYTES}"),
        ));
    }
    let Some(expected_next_snapshot_id) = envelope.snapshot_id.checked_add(1) else {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "stream plan snapshot cursor overflows".to_string(),
        ));
    };
    let valid_next_snapshot_id = envelope.next_snapshot_id == expected_next_snapshot_id
        || (envelope.kind == 0 && envelope.next_snapshot_id == envelope.snapshot_id);
    if envelope.snapshot_id < 1 || !valid_next_snapshot_id {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "stream plan contains an invalid snapshot cursor".to_string(),
        ));
    }
    Ok(())
}

fn validate_stream_plan_recovery_paths(state: &StreamPlanState) -> Result<(), *mut paimon_error> {
    match &state.plan {
        StreamPlan::Full { plan, .. } => {
            for split in plan.splits() {
                split
                    .validate_restored_containment(&state.table_location)
                    .map_err(paimon_error::from_paimon)?;
            }
        }
        StreamPlan::Incremental { plan, .. } => {
            for split in plan.splits() {
                let IncrementalSplit::Data(split) = split else {
                    return Err(paimon_error::new(
                        PaimonErrorCode::Unsupported,
                        "DiffPair plans are not valid continuous stream plans".to_string(),
                    ));
                };
                split
                    .validate_restored_containment(&state.table_location)
                    .map_err(paimon_error::from_paimon)?;
            }
        }
    }
    Ok(())
}

fn panic_error(operation: &str) -> *mut paimon_error {
    paimon_error::new(
        PaimonErrorCode::Unexpected,
        format!("Rust panic while executing {operation}"),
    )
}

fn invalid_mode(name: &str, value: i32) -> *mut paimon_error {
    paimon_error::new(
        PaimonErrorCode::InvalidInput,
        format!("invalid {name} value {value}"),
    )
}

fn empty_bytes() -> paimon_bytes {
    paimon_bytes {
        data: ptr::null_mut(),
        len: 0,
    }
}

fn empty_poll(status: i32, scan: Option<&StreamScan>) -> paimon_result_stream_poll {
    paimon_result_stream_poll {
        status,
        plan: ptr::null_mut(),
        snapshot_id: -1,
        next_snapshot_id: scan.and_then(StreamScan::checkpoint).unwrap_or(-1),
        watermark: scan.and_then(StreamScan::watermark).unwrap_or(0),
        has_watermark: u8::from(scan.and_then(StreamScan::watermark).is_some()),
        reserved: [0; 7],
        error: ptr::null_mut(),
    }
}

fn error_poll(error: *mut paimon_error) -> paimon_result_stream_poll {
    let mut result = empty_poll(PAIMON_STREAM_POLL_END, None);
    result.error = error;
    result
}

fn configure_builder<'a>(
    state: &'a ReadBuilderState,
) -> Result<paimon::table::ReadBuilder<'a>, *mut paimon_error> {
    let mut builder = state.table.new_read_builder();
    builder.with_case_sensitive(state.case_sensitive);
    if let Some(columns) = &state.projected_columns {
        let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
        builder
            .with_projection(&columns)
            .map_err(paimon_error::from_paimon)?;
    }
    if let Some(filter) = &state.filter {
        builder.with_filter(filter.clone());
    }
    Ok(builder)
}

/// Fill stream options with forward-compatible defaults (`latest-full`,
/// automatic delta/changelog selection).
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_scan_options_init(
    options: *mut paimon_stream_scan_options,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(options, "options") {
        return error;
    }
    ptr::write(
        options,
        paimon_stream_scan_options {
            struct_size: size_of::<paimon_stream_scan_options>() as u32,
            startup_mode: PAIMON_STREAM_STARTUP_LATEST_FULL,
            follow_up_mode: PAIMON_STREAM_FOLLOW_UP_AUTO,
            snapshot_id: -1,
            reserved: [0; 4],
        },
    );
    ptr::null_mut()
}

/// Create an owned stream scan from a read builder.
///
/// The returned scan clones all required Rust state and remains valid after
/// the read builder and table handles are freed. A scan handle is
/// single-thread-confined: callers must serialize poll/checkpoint/restore/free.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_new_stream_scan(
    read_builder: *const paimon_read_builder,
    options: *const paimon_stream_scan_options,
) -> paimon_result_stream_scan {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(read_builder, "read_builder") {
            return paimon_result_stream_scan {
                scan: ptr::null_mut(),
                error,
            };
        }
        if let Err(error) = check_non_null(options, "options") {
            return paimon_result_stream_scan {
                scan: ptr::null_mut(),
                error,
            };
        }
        if (*options).struct_size < size_of::<paimon_stream_scan_options>() as u32 {
            return paimon_result_stream_scan {
                scan: ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!(
                        "stream options struct_size {} is smaller than required {}",
                        (*options).struct_size,
                        size_of::<paimon_stream_scan_options>()
                    ),
                ),
            };
        }
        if (*options).reserved.iter().any(|value| *value != 0) {
            return paimon_result_stream_scan {
                scan: ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::Unsupported,
                    "stream options reserved fields must be zero for ABI version 1".to_string(),
                ),
            };
        }
        let startup = match (*options).startup_mode {
            PAIMON_STREAM_STARTUP_LATEST_FULL => StreamScanStartupMode::LatestFull,
            PAIMON_STREAM_STARTUP_LATEST => StreamScanStartupMode::Latest,
            PAIMON_STREAM_STARTUP_FROM_SNAPSHOT => {
                StreamScanStartupMode::FromSnapshot((*options).snapshot_id)
            }
            PAIMON_STREAM_STARTUP_FROM_SNAPSHOT_FULL => {
                StreamScanStartupMode::FromSnapshotFull((*options).snapshot_id)
            }
            value => {
                return paimon_result_stream_scan {
                    scan: ptr::null_mut(),
                    error: invalid_mode("stream startup mode", value),
                }
            }
        };
        let follow_up = match (*options).follow_up_mode {
            PAIMON_STREAM_FOLLOW_UP_AUTO => StreamScanFollowUpMode::Auto,
            PAIMON_STREAM_FOLLOW_UP_DELTA => StreamScanFollowUpMode::Delta,
            PAIMON_STREAM_FOLLOW_UP_CHANGELOG => StreamScanFollowUpMode::Changelog,
            value => {
                return paimon_result_stream_scan {
                    scan: ptr::null_mut(),
                    error: invalid_mode("stream follow-up mode", value),
                }
            }
        };
        let state = &*((*read_builder).inner as *const ReadBuilderState);
        let table_location = state.table.location().to_string();
        let table_branch = state.table.branch().to_string();
        let schema_id = state.table.schema().id();
        let read_fingerprint = read_builder_fingerprint(state);
        let builder = match configure_builder(state) {
            Ok(builder) => builder,
            Err(error) => {
                return paimon_result_stream_scan {
                    scan: ptr::null_mut(),
                    error,
                }
            }
        };
        match runtime().block_on(builder.new_stream_scan(startup, follow_up)) {
            Ok(scan) => {
                let inner = Box::into_raw(Box::new(StreamScanState {
                    scan,
                    table_location,
                    table_branch,
                    schema_id,
                    read_fingerprint,
                })) as *mut c_void;
                paimon_result_stream_scan {
                    scan: Box::into_raw(Box::new(paimon_stream_scan { inner })),
                    error: ptr::null_mut(),
                }
            }
            Err(error) => paimon_result_stream_scan {
                scan: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            },
        }
    }));
    outcome.unwrap_or_else(|_| paimon_result_stream_scan {
        scan: ptr::null_mut(),
        error: panic_error("paimon_read_builder_new_stream_scan"),
    })
}

/// Poll once for a snapshot plan. This call never waits for a future snapshot.
/// Calls using the same scan handle must not overlap on different threads.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_scan_poll(
    scan: *mut paimon_stream_scan,
) -> paimon_result_stream_poll {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(scan, "scan") {
            return error_poll(error);
        }
        let state = &mut *((*scan).inner as *mut StreamScanState);
        match runtime().block_on(state.scan.poll_next()) {
            Ok(StreamScanPoll::Data(plan)) => {
                let snapshot_id = plan.snapshot_id();
                let next_snapshot_id = plan.next_snapshot_id();
                let watermark = plan.watermark();
                let inner = Box::into_raw(Box::new(StreamPlanState {
                    plan,
                    table_location: state.table_location.clone(),
                    table_branch: state.table_branch.clone(),
                    schema_id: state.schema_id,
                    read_fingerprint: state.read_fingerprint.clone(),
                })) as *mut c_void;
                paimon_result_stream_poll {
                    status: PAIMON_STREAM_POLL_DATA,
                    plan: Box::into_raw(Box::new(paimon_stream_plan { inner })),
                    snapshot_id,
                    next_snapshot_id,
                    watermark: watermark.unwrap_or(0),
                    has_watermark: u8::from(watermark.is_some()),
                    reserved: [0; 7],
                    error: ptr::null_mut(),
                }
            }
            Ok(StreamScanPoll::Waiting) => {
                empty_poll(PAIMON_STREAM_POLL_WAITING, Some(&state.scan))
            }
            Ok(StreamScanPoll::End) => empty_poll(PAIMON_STREAM_POLL_END, Some(&state.scan)),
            Err(error) => error_poll(paimon_error::from_paimon(error)),
        }
    }));
    outcome.unwrap_or_else(|_| error_poll(panic_error("paimon_stream_scan_poll")))
}

/// Return the next-snapshot cursor, or -1 before a startup position exists.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_scan_checkpoint(scan: *const paimon_stream_scan) -> i64 {
    if scan.is_null() || (*scan).inner.is_null() {
        return -1;
    }
    let state = &*((*scan).inner as *const StreamScanState);
    state.scan.checkpoint().unwrap_or(-1)
}

/// Restore a next-snapshot cursor. Pass -1 to reapply the configured startup
/// mode; non-negative values must name a valid Paimon snapshot position.
/// This call must not overlap poll/checkpoint/free on the same handle.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_scan_restore(
    scan: *mut paimon_stream_scan,
    next_snapshot_id: i64,
) -> *mut paimon_error {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(scan, "scan") {
            return error;
        }
        if next_snapshot_id != -1 && next_snapshot_id < 1 {
            return paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "next_snapshot_id must be -1 or a positive snapshot id".to_string(),
            );
        }
        let state = &mut *((*scan).inner as *mut StreamScanState);
        match state
            .scan
            .restore((next_snapshot_id >= 0).then_some(next_snapshot_id))
        {
            Ok(()) => ptr::null_mut(),
            Err(error) => paimon_error::from_paimon(error),
        }
    }));
    outcome.unwrap_or_else(|_| panic_error("paimon_stream_scan_restore"))
}

/// Free a stream scan. It is valid to pass null.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_scan_free(scan: *mut paimon_stream_scan) {
    if !scan.is_null() {
        let wrapper = Box::from_raw(scan);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut StreamScanState));
        }
    }
}

/// Return whether a stream plan is an initial full-snapshot plan.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_plan_is_full(plan: *const paimon_stream_plan) -> u8 {
    if plan.is_null() || (*plan).inner.is_null() {
        return 0;
    }
    let state = &*((*plan).inner as *const StreamPlanState);
    u8::from(state.plan.full_plan().is_some())
}

/// Return the number of work splits in a stream plan.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_plan_num_splits(plan: *const paimon_stream_plan) -> usize {
    if plan.is_null() || (*plan).inner.is_null() {
        return 0;
    }
    let state = &*((*plan).inner as *const StreamPlanState);
    match &state.plan {
        StreamPlan::Full { plan, .. } => plan.splits().len(),
        StreamPlan::Incremental { plan, .. } => plan.splits().len(),
    }
}

/// Serialize planned-but-not-yet-consumed work for an external checkpoint.
///
/// The current format checkpoints at plan boundaries. If rows from a plan have already
/// been exposed, callers must either replay the plan after recovery or persist
/// their own logical rows-to-skip position alongside this buffer.
/// Plans containing external data-file paths are rejected because version 1
/// recovery cannot revalidate those paths against a trusted manifest.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_plan_serialize(
    plan: *const paimon_stream_plan,
) -> paimon_result_bytes {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(plan, "plan") {
            return paimon_result_bytes {
                bytes: empty_bytes(),
                error,
            };
        }
        let state = &*((*plan).inner as *const StreamPlanState);
        if let Err(error) = validate_stream_plan_recovery_paths(state) {
            return paimon_result_bytes {
                bytes: empty_bytes(),
                error,
            };
        }
        let envelope = match &state.plan {
            StreamPlan::Full {
                snapshot_id,
                watermark,
                next_snapshot_id,
                plan,
            } => {
                let splits = match plan
                    .splits()
                    .iter()
                    .map(DataSplit::serialize_split_v1)
                    .collect::<paimon::Result<Vec<_>>>()
                {
                    Ok(splits) => splits,
                    Err(error) => {
                        return paimon_result_bytes {
                            bytes: empty_bytes(),
                            error: paimon_error::from_paimon(error),
                        }
                    }
                };
                StreamPlanEnvelope {
                    format: STREAM_PLAN_FORMAT.to_string(),
                    version: STREAM_PLAN_VERSION,
                    table_location: state.table_location.clone(),
                    table_branch: state.table_branch.clone(),
                    schema_id: state.schema_id,
                    read_fingerprint: state.read_fingerprint.clone(),
                    kind: 0,
                    incremental_mode: -1,
                    snapshot_id: *snapshot_id,
                    next_snapshot_id: *next_snapshot_id,
                    watermark: *watermark,
                    splits,
                }
            }
            StreamPlan::Incremental {
                snapshot_id,
                watermark,
                next_snapshot_id,
                plan,
            } => {
                let mut splits = Vec::with_capacity(plan.splits().len());
                for split in plan.splits() {
                    let IncrementalSplit::Data(split) = split else {
                        return paimon_result_bytes {
                            bytes: empty_bytes(),
                            error: paimon_error::new(
                                PaimonErrorCode::Unsupported,
                                "DiffPair plans are not valid continuous stream plans".to_string(),
                            ),
                        };
                    };
                    match split.serialize_split_v1() {
                        Ok(bytes) => splits.push(bytes),
                        Err(error) => {
                            return paimon_result_bytes {
                                bytes: empty_bytes(),
                                error: paimon_error::from_paimon(error),
                            }
                        }
                    }
                }
                let incremental_mode = match plan.mode() {
                    IncrementalScanMode::Delta => 0,
                    IncrementalScanMode::Changelog => 1,
                    IncrementalScanMode::Auto | IncrementalScanMode::Diff => {
                        return paimon_result_bytes {
                            bytes: empty_bytes(),
                            error: paimon_error::new(
                                PaimonErrorCode::Unsupported,
                                "unresolved Auto and Diff plans are not valid continuous stream plans"
                                    .to_string(),
                            ),
                        };
                    }
                };
                StreamPlanEnvelope {
                    format: STREAM_PLAN_FORMAT.to_string(),
                    version: STREAM_PLAN_VERSION,
                    table_location: state.table_location.clone(),
                    table_branch: state.table_branch.clone(),
                    schema_id: state.schema_id,
                    read_fingerprint: state.read_fingerprint.clone(),
                    kind: 1,
                    incremental_mode,
                    snapshot_id: *snapshot_id,
                    next_snapshot_id: *next_snapshot_id,
                    watermark: *watermark,
                    splits,
                }
            }
        };
        if let Err(error) = validate_stream_plan_envelope(&envelope) {
            return paimon_result_bytes {
                bytes: empty_bytes(),
                error,
            };
        }
        match serde_json::to_vec(&envelope) {
            Ok(bytes) if bytes.len() <= MAX_STREAM_PLAN_BYTES => paimon_result_bytes {
                bytes: paimon_bytes::new(bytes),
                error: ptr::null_mut(),
            },
            Ok(bytes) => paimon_result_bytes {
                bytes: empty_bytes(),
                error: paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!(
                        "serialized stream plan is {} bytes; maximum is {}",
                        bytes.len(),
                        MAX_STREAM_PLAN_BYTES
                    ),
                ),
            },
            Err(error) => paimon_result_bytes {
                bytes: empty_bytes(),
                error: paimon_error::new(
                    PaimonErrorCode::Unexpected,
                    format!("failed to serialize stream plan: {error}"),
                ),
            },
        }
    }));
    outcome.unwrap_or_else(|_| paimon_result_bytes {
        bytes: empty_bytes(),
        error: panic_error("paimon_stream_plan_serialize"),
    })
}

/// Restore a stream plan serialized by `paimon_stream_plan_serialize`.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_plan_deserialize(
    data: *const u8,
    len: usize,
) -> paimon_result_stream_poll {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if data.is_null() || len == 0 {
            return error_poll(paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "stream plan buffer must not be null or empty".to_string(),
            ));
        }
        if len > MAX_STREAM_PLAN_BYTES {
            return error_poll(paimon_error::new(
                PaimonErrorCode::InvalidInput,
                format!("stream plan buffer exceeds {MAX_STREAM_PLAN_BYTES} bytes"),
            ));
        }
        let envelope: StreamPlanEnvelope =
            match serde_json::from_slice(std::slice::from_raw_parts(data, len)) {
                Ok(envelope) => envelope,
                Err(error) => {
                    return error_poll(paimon_error::new(
                        PaimonErrorCode::InvalidInput,
                        format!("invalid stream plan buffer: {error}"),
                    ))
                }
            };
        if envelope.format != STREAM_PLAN_FORMAT || envelope.version != STREAM_PLAN_VERSION {
            return error_poll(paimon_error::new(
                PaimonErrorCode::Unsupported,
                format!(
                    "unsupported stream plan format '{}' version {}",
                    envelope.format, envelope.version
                ),
            ));
        }
        if let Err(error) = validate_stream_plan_envelope(&envelope) {
            return error_poll(error);
        }
        let splits = match envelope
            .splits
            .iter()
            .map(|split| DataSplit::deserialize_split_v1(split))
            .collect::<paimon::Result<Vec<_>>>()
        {
            Ok(splits) => splits,
            Err(error) => return error_poll(paimon_error::from_paimon(error)),
        };
        if splits
            .iter()
            .any(|split| split.snapshot_id() != envelope.snapshot_id)
        {
            return error_poll(paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "stream plan split snapshot does not match its envelope".to_string(),
            ));
        }
        for split in &splits {
            if let Err(error) = split.validate_restored_containment(&envelope.table_location) {
                return error_poll(paimon_error::from_paimon(error));
            }
        }
        let plan = match (envelope.kind, envelope.incremental_mode) {
            (0, -1) => StreamPlan::Full {
                snapshot_id: envelope.snapshot_id,
                watermark: envelope.watermark,
                next_snapshot_id: envelope.next_snapshot_id,
                plan: Plan::new(splits),
            },
            (1, mode @ (0 | 1)) => {
                let mode = if mode == 0 {
                    IncrementalScanMode::Delta
                } else {
                    IncrementalScanMode::Changelog
                };
                let splits = splits.into_iter().map(IncrementalSplit::Data).collect();
                let plan = match IncrementalPlan::try_new(mode, splits) {
                    Ok(plan) => plan,
                    Err(error) => return error_poll(paimon_error::from_paimon(error)),
                };
                StreamPlan::Incremental {
                    snapshot_id: envelope.snapshot_id,
                    watermark: envelope.watermark,
                    next_snapshot_id: envelope.next_snapshot_id,
                    plan,
                }
            }
            _ => {
                return error_poll(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "stream plan contains an invalid kind or mode".to_string(),
                ))
            }
        };
        let snapshot_id = plan.snapshot_id();
        let next_snapshot_id = plan.next_snapshot_id();
        let watermark = plan.watermark();
        let inner = Box::into_raw(Box::new(StreamPlanState {
            plan,
            table_location: envelope.table_location,
            table_branch: envelope.table_branch,
            schema_id: envelope.schema_id,
            read_fingerprint: envelope.read_fingerprint,
        })) as *mut c_void;
        paimon_result_stream_poll {
            status: PAIMON_STREAM_POLL_DATA,
            plan: Box::into_raw(Box::new(paimon_stream_plan { inner })),
            snapshot_id,
            next_snapshot_id,
            watermark: watermark.unwrap_or(0),
            has_watermark: u8::from(watermark.is_some()),
            reserved: [0; 7],
            error: ptr::null_mut(),
        }
    }));
    outcome.unwrap_or_else(|_| error_poll(panic_error("paimon_stream_plan_deserialize")))
}

/// Read a contiguous split range from a stream plan.
///
/// `read_mode=PAIMON_STREAM_READ_AUDIT_LOG` exposes a stable UTF-8 `rowkind`
/// column for incremental plans. Full startup plans currently support data
/// mode only; callers requiring one fixed audit schema should start at
/// `latest` or `from-snapshot`.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_plan_read_to_arrow(
    read: *const paimon_table_read,
    plan: *const paimon_stream_plan,
    offset: usize,
    length: usize,
    read_mode: i32,
) -> paimon_result_record_batch_reader {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(read, "read") {
            return paimon_result_record_batch_reader {
                reader: ptr::null_mut(),
                error,
            };
        }
        if let Err(error) = check_non_null(plan, "plan") {
            return paimon_result_record_batch_reader {
                reader: ptr::null_mut(),
                error,
            };
        }
        if read_mode != PAIMON_STREAM_READ_DATA && read_mode != PAIMON_STREAM_READ_AUDIT_LOG {
            return paimon_result_record_batch_reader {
                reader: ptr::null_mut(),
                error: invalid_mode("stream read mode", read_mode),
            };
        }
        let state = &*((*read).inner as *const TableReadState);
        let plan_state = &*((*plan).inner as *const StreamPlanState);
        if state.table_location != plan_state.table_location
            || state.table_branch != plan_state.table_branch
            || state.schema_id != plan_state.schema_id
            || state.read_fingerprint != plan_state.read_fingerprint
        {
            return paimon_result_record_batch_reader {
                reader: ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "stream plan was created for a different table, branch, schema, or read builder"
                        .to_string(),
                ),
            };
        }
        let stream_plan = &plan_state.plan;
        let table_read = TableRead::new(
            &state.table,
            state.read_type.clone(),
            state.data_predicates.clone(),
        );
        let stream_result: paimon::Result<ArrowRecordBatchStream> = match stream_plan {
            StreamPlan::Full { plan, .. } => {
                if read_mode == PAIMON_STREAM_READ_AUDIT_LOG {
                    Err(paimon::Error::Unsupported {
                        message: "Audit-log mode for a full stream startup plan is not implemented; use latest/from-snapshot startup or data mode".to_string(),
                    })
                } else {
                    let splits = plan.splits();
                    let start = offset.min(splits.len());
                    let end = offset.saturating_add(length).min(splits.len());
                    table_read.to_arrow(&splits[start..end])
                }
            }
            StreamPlan::Incremental { plan, .. } => {
                let splits = plan.splits();
                let start = offset.min(splits.len());
                let end = offset.saturating_add(length).min(splits.len());
                let selected = paimon::table::IncrementalPlan::try_new(
                    plan.mode(),
                    splits[start..end].to_vec(),
                );
                match selected {
                    Ok(selected) if read_mode == PAIMON_STREAM_READ_AUDIT_LOG => {
                        table_read.to_audit_log_arrow(&selected)
                    }
                    Ok(selected) => table_read.to_incremental_arrow(&selected),
                    Err(error) => Err(error),
                }
            }
        };
        match stream_result {
            Ok(stream) => {
                let inner = Box::into_raw(Box::new(stream)) as *mut c_void;
                paimon_result_record_batch_reader {
                    reader: Box::into_raw(Box::new(paimon_record_batch_reader { inner })),
                    error: ptr::null_mut(),
                }
            }
            Err(error) => paimon_result_record_batch_reader {
                reader: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            },
        }
    }));
    outcome.unwrap_or_else(|_| paimon_result_record_batch_reader {
        reader: ptr::null_mut(),
        error: panic_error("paimon_stream_plan_read_to_arrow"),
    })
}

/// Free a stream plan. It is valid to pass null.
#[no_mangle]
pub unsafe extern "C" fn paimon_stream_plan_free(plan: *mut paimon_stream_plan) {
    if !plan.is_null() {
        let wrapper = Box::from_raw(plan);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut StreamPlanState));
        }
    }
}

// C ABI signature guards.
const _: unsafe extern "C" fn(
    *const paimon_read_builder,
    *const paimon_stream_scan_options,
) -> paimon_result_stream_scan = paimon_read_builder_new_stream_scan;
const _: unsafe extern "C" fn(*mut paimon_stream_scan) -> paimon_result_stream_poll =
    paimon_stream_scan_poll;
const _: unsafe extern "C" fn(
    *const paimon_table_read,
    *const paimon_stream_plan,
    usize,
    usize,
    i32,
) -> paimon_result_record_batch_reader = paimon_stream_plan_read_to_arrow;
const _: unsafe extern "C" fn(*const paimon_stream_plan) -> paimon_result_bytes =
    paimon_stream_plan_serialize;
const _: unsafe extern "C" fn(*const u8, usize) -> paimon_result_stream_poll =
    paimon_stream_plan_deserialize;

#[cfg(test)]
mod tests {
    use paimon::spec::BinaryRow;
    use paimon::table::DataSplitBuilder;

    use super::*;

    fn serialized_plan(bucket_path: &str, next_snapshot_id: i64) -> Vec<u8> {
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path.to_string())
            .with_total_buckets(1)
            .with_data_files(Vec::new())
            .build()
            .unwrap();
        serde_json::to_vec(&StreamPlanEnvelope {
            format: STREAM_PLAN_FORMAT.to_string(),
            version: STREAM_PLAN_VERSION,
            table_location: "memory:/table".to_string(),
            table_branch: "main".to_string(),
            schema_id: 0,
            read_fingerprint: "fingerprint".to_string(),
            kind: 0,
            incremental_mode: -1,
            snapshot_id: 1,
            next_snapshot_id,
            watermark: None,
            splits: vec![split.serialize_split_v1().unwrap()],
        })
        .unwrap()
    }

    fn full_plan_state(bucket_path: &str) -> StreamPlanState {
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path.to_string())
            .with_total_buckets(1)
            .with_data_files(Vec::new())
            .build()
            .unwrap();
        StreamPlanState {
            plan: StreamPlan::Full {
                snapshot_id: 1,
                watermark: None,
                next_snapshot_id: 2,
                plan: Plan::new(vec![split]),
            },
            table_location: "memory:/table".to_string(),
            table_branch: "main".to_string(),
            schema_id: 0,
            read_fingerprint: "fingerprint".to_string(),
        }
    }

    #[test]
    fn serialization_rejects_plan_which_cannot_be_restored() {
        assert!(
            validate_stream_plan_recovery_paths(&full_plan_state("memory:/table/bucket-0")).is_ok()
        );
        let error =
            validate_stream_plan_recovery_paths(&full_plan_state("memory:/table-evil/bucket-0"))
                .unwrap_err();
        unsafe { crate::error::paimon_error_free(error) };
    }

    #[test]
    fn restored_plan_rejects_bucket_path_outside_table() {
        let bytes = serialized_plan("memory:/table-evil/bucket-0", 2);
        let result = unsafe { paimon_stream_plan_deserialize(bytes.as_ptr(), bytes.len()) };
        assert!(result.plan.is_null());
        assert!(!result.error.is_null());
        unsafe { crate::error::paimon_error_free(result.error) };
    }

    #[test]
    fn full_plan_allows_same_snapshot_follow_up_cursor() {
        let bytes = serialized_plan("memory:/table/bucket-0", 1);
        let result = unsafe { paimon_stream_plan_deserialize(bytes.as_ptr(), bytes.len()) };
        assert!(result.error.is_null());
        assert_eq!(result.next_snapshot_id, 1);
        unsafe { paimon_stream_plan_free(result.plan) };
    }

    #[test]
    fn older_plan_version_is_reported_as_unsupported() {
        let bytes = serialized_plan("memory:/table/bucket-0", 2);
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["version"] = serde_json::json!(2);
        value.as_object_mut().unwrap().remove("table_branch");
        let bytes = serde_json::to_vec(&value).unwrap();
        let result = unsafe { paimon_stream_plan_deserialize(bytes.as_ptr(), bytes.len()) };
        assert!(result.plan.is_null());
        assert!(!result.error.is_null());
        assert_eq!(
            unsafe { (*result.error).code },
            PaimonErrorCode::Unsupported as i32
        );
        unsafe { crate::error::paimon_error_free(result.error) };
    }
}
