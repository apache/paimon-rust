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

use std::ffi::c_void;
use std::ptr;

use arrow_array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{RecordBatch, StructArray};
use paimon::table::{CommitMessage, Table, TableCommit, TableWrite};

use crate::error::{check_non_null, paimon_error, PaimonErrorCode};
use crate::result::{
    paimon_result_prepare_commit, paimon_result_table_commit, paimon_result_table_write,
    paimon_result_write_builder,
};
use crate::runtime;
use crate::types::*;

// ======================= WriteBuilder ===============================

/// Create a new WriteBuilder from a Table.
///
/// The returned WriteBuilder holds a shared `commit_user` (UUID) that will be
/// used by both `new_write()` and `new_commit()` for duplicate-commit detection.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_write_builder(
    table: *const paimon_table,
) -> paimon_result_write_builder {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_write_builder {
            write_builder: ptr::null_mut(),
            error: e,
        };
    }
    let table_ref = &*((*table).inner as *const Table);
    let wb = table_ref.new_write_builder();
    let commit_user = wb.commit_user().to_string();
    let state = WriteBuilderState {
        table: table_ref.clone(),
        commit_user,
        overwrite: false,
    };
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    paimon_result_write_builder {
        write_builder: Box::into_raw(Box::new(paimon_write_builder { inner })),
        error: ptr::null_mut(),
    }
}

/// Free a paimon_write_builder.
///
/// # Safety
/// Only call with a write_builder returned from `paimon_table_new_write_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_write_builder_free(wb: *mut paimon_write_builder) {
    if !wb.is_null() {
        let wrapper = Box::from_raw(wb);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut WriteBuilderState));
        }
    }
}

/// Enable overwrite mode for the WriteBuilder.
///
/// In overwrite mode, a subsequent `paimon_table_commit_overwrite` will replace
/// the data in the written partitions rather than appending.
///
/// # Safety
/// `wb` must be a valid pointer from `paimon_table_new_write_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_write_builder_with_overwrite(
    wb: *mut paimon_write_builder,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(wb, "wb") {
        return e;
    }
    let state = &mut *((*wb).inner as *mut WriteBuilderState);
    state.overwrite = true;
    ptr::null_mut()
}

// ======================= TableWrite ===============================

/// Create a new TableWrite from the WriteBuilder.
///
/// The returned TableWrite accumulates Arrow batches until `prepare_commit` is called.
///
/// # Safety
/// `wb` must be a valid pointer from `paimon_table_new_write_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_write_builder_new_write(
    wb: *const paimon_write_builder,
) -> paimon_result_table_write {
    if let Err(e) = check_non_null(wb, "wb") {
        return paimon_result_table_write {
            write: ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*wb).inner as *const WriteBuilderState);

    let mut builder = match state
        .table
        .new_write_builder()
        .with_commit_user(state.commit_user.clone())
    {
        Ok(b) => b,
        Err(e) => {
            return paimon_result_table_write {
                write: ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };

    if state.overwrite {
        builder = builder.with_overwrite();
    }

    let tw = match builder.new_write() {
        Ok(w) => w,
        Err(e) => {
            return paimon_result_table_write {
                write: ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };

    let inner = Box::into_raw(Box::new(tw)) as *mut c_void;
    paimon_result_table_write {
        write: Box::into_raw(Box::new(paimon_table_write { inner })),
        error: ptr::null_mut(),
    }
}

/// Free a paimon_table_write.
///
/// Dropping a TableWrite before calling `prepare_commit` discards any
/// uncommitted data.
///
/// # Safety
/// Only call with a write returned from `paimon_write_builder_new_write`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_write_free(tw: *mut paimon_table_write) {
    if !tw.is_null() {
        let wrapper = Box::from_raw(tw);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableWrite));
        }
    }
}

/// Write a single Arrow record batch into the table's writers.
///
/// The Arrow data is imported via the Arrow C Data Interface. `array` and
/// `schema` must point to valid `ArrowArray` and `ArrowSchema` structs
/// filled by the caller. Ownership is transferred — the caller must not
/// release the structs after this call.
///
/// # Safety
/// `tw` must be a valid pointer from `paimon_write_builder_new_write`, or null (returns error).
/// `array` and `schema` must be valid pointers to initialized ArrowArray /
/// ArrowSchema structs, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_write_write_arrow_batch(
    tw: *mut paimon_table_write,
    array: *mut c_void,
    schema: *mut c_void,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tw, "tw") {
        return e;
    }
    if let Err(e) = check_non_null(array, "array") {
        return e;
    }
    if let Err(e) = check_non_null(schema, "schema") {
        return e;
    }

    let table_write = &mut *((*tw).inner as *mut TableWrite);

    let ffi_array = ptr::read(array as *const FFI_ArrowArray);
    let ffi_schema = ptr::read(schema as *const FFI_ArrowSchema);

    let batch = match unsafe { from_ffi(ffi_array, &ffi_schema) } {
        Ok(data) => {
            // The ffi_array was consumed by from_ffi (moved by value).
            // The ffi_schema was only borrowed; it will be dropped below,
            // calling release on the schema resources.
            drop(ffi_schema);
            RecordBatch::from(StructArray::from(data))
        }
        Err(e) => {
            // from_ffi consumed ffi_array (by value). Its drop will call release.
            // We let ffi_schema drop normally here too.
            drop(ffi_schema);
            return paimon_error::new(
                PaimonErrorCode::InvalidInput,
                format!("Failed to import Arrow record batch: {e}"),
            );
        }
    };

    match runtime().block_on(table_write.write_arrow_batch(&batch)) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

/// Close file writers and produce CommitMessages.
///
/// Consumes the open file writers (they are flushed and closed). After this
/// call, the TableWrite can be reused — `write_arrow_batch` may be called
/// again to start a new round of writes.
///
/// The returned `paimon_commit_messages` must be passed to a
/// `paimon_table_commit_*` function and then freed with
/// `paimon_commit_messages_free`.
///
/// # Safety
/// `tw` must be a valid pointer from `paimon_write_builder_new_write`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_write_prepare_commit(
    tw: *mut paimon_table_write,
) -> paimon_result_prepare_commit {
    if let Err(e) = check_non_null(tw, "tw") {
        return paimon_result_prepare_commit {
            messages: ptr::null_mut(),
            error: e,
        };
    }
    let table_write = &mut *((*tw).inner as *mut TableWrite);

    match runtime().block_on(table_write.prepare_commit()) {
        Ok(messages) => {
            let inner = Box::into_raw(Box::new(messages)) as *mut c_void;
            paimon_result_prepare_commit {
                messages: Box::into_raw(Box::new(paimon_commit_messages { inner })),
                error: ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_prepare_commit {
            messages: ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= TableCommit ===============================

/// Create a new TableCommit from the WriteBuilder.
///
/// The committer shares the same `commit_user` as the writer, which is
/// required for duplicate-commit detection.
///
/// # Safety
/// `wb` must be a valid pointer from `paimon_table_new_write_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_write_builder_new_commit(
    wb: *const paimon_write_builder,
) -> paimon_result_table_commit {
    if let Err(e) = check_non_null(wb, "wb") {
        return paimon_result_table_commit {
            commit: ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*wb).inner as *const WriteBuilderState);

    let builder = match state
        .table
        .new_write_builder()
        .with_commit_user(state.commit_user.clone())
    {
        Ok(b) => b,
        Err(e) => {
            return paimon_result_table_commit {
                commit: ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };

    let tc = match builder.try_new_commit() {
        Ok(c) => c,
        Err(e) => {
            return paimon_result_table_commit {
                commit: ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };

    let inner = Box::into_raw(Box::new(tc)) as *mut c_void;
    paimon_result_table_commit {
        commit: Box::into_raw(Box::new(paimon_table_commit { inner })),
        error: ptr::null_mut(),
    }
}

/// Free a paimon_table_commit.
///
/// # Safety
/// Only call with a commit returned from `paimon_write_builder_new_commit`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_free(tc: *mut paimon_table_commit) {
    if !tc.is_null() {
        let wrapper = Box::from_raw(tc);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableCommit));
        }
    }
}

// ======================= CommitMessages ===============================

/// Free a paimon_commit_messages.
///
/// # Safety
/// Only call with messages returned from `paimon_table_write_prepare_commit`.
#[no_mangle]
pub unsafe extern "C" fn paimon_commit_messages_free(msgs: *mut paimon_commit_messages) {
    if !msgs.is_null() {
        let wrapper = Box::from_raw(msgs);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut Vec<CommitMessage>));
        }
    }
}

// ======================= Commit operations ===============================

/// Commit the given messages in APPEND mode.
///
/// Empty messages is a no-op success.
///
/// # Safety
/// `tc` must be a valid pointer from `paimon_write_builder_new_commit`, or null (returns error).
/// `msgs` must be a valid pointer from `paimon_table_write_prepare_commit`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_commit(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }
    if let Err(e) = check_non_null(msgs, "msgs") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommit);
    let messages = Box::from_raw((*msgs).inner as *mut Vec<CommitMessage>);
    // Release the outer wrapper to avoid double-free when caller frees msgs
    drop(Box::from_raw(msgs));

    if messages.is_empty() {
        return ptr::null_mut();
    }

    match runtime().block_on(table_commit.commit(*messages)) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

/// Commit in OVERWRITE mode, replacing data in the written partitions.
///
/// `static_partitions` is currently passed as `None` (overwrite all
/// partitions that were written to).
///
/// # Safety
/// `tc` must be a valid pointer from `paimon_write_builder_new_commit`, or null (returns error).
/// `msgs` must be a valid pointer from `paimon_table_write_prepare_commit`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_overwrite(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }
    if let Err(e) = check_non_null(msgs, "msgs") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommit);
    let messages = Box::from_raw((*msgs).inner as *mut Vec<CommitMessage>);
    drop(Box::from_raw(msgs));

    if messages.is_empty() {
        return ptr::null_mut();
    }

    match runtime().block_on(table_commit.overwrite(*messages, None)) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

/// Truncate the entire table — removes all data.
///
/// This is an OVERWRITE with zero new files. The table's latest snapshot
/// will have no data.
///
/// # Safety
/// `tc` must be a valid pointer from `paimon_write_builder_new_commit`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_truncate_table(
    tc: *const paimon_table_commit,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommit);

    match runtime().block_on(table_commit.truncate_table()) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

/// Abort a prepared commit, cleaning up written data files.
///
/// This is a best-effort cleanup — it attempts to delete new data and
/// changelog files produced by the writer. Errors during cleanup are
/// returned but do not roll back the cleanup of previously-deleted files.
///
/// # Safety
/// `tc` must be a valid pointer from `paimon_write_builder_new_commit`, or null (returns error).
/// `msgs` must be a valid pointer from `paimon_table_write_prepare_commit`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_abort(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }
    if let Err(e) = check_non_null(msgs, "msgs") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommit);
    let messages = Box::from_raw((*msgs).inner as *mut Vec<CommitMessage>);
    drop(Box::from_raw(msgs));

    if messages.is_empty() {
        return ptr::null_mut();
    }

    match runtime().block_on(table_commit.abort(&messages)) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

// --- C ABI signature guards -------------------------------------------------

const _: unsafe extern "C" fn(*const paimon_table) -> paimon_result_write_builder =
    paimon_table_new_write_builder;
const _: unsafe extern "C" fn(*const paimon_write_builder) -> paimon_result_table_write =
    paimon_write_builder_new_write;
const _: unsafe extern "C" fn(*const paimon_write_builder) -> paimon_result_table_commit =
    paimon_write_builder_new_commit;
const _: unsafe extern "C" fn(*mut paimon_table_write) -> paimon_result_prepare_commit =
    paimon_table_write_prepare_commit;
const _: unsafe extern "C" fn(
    *mut paimon_table_write,
    *mut c_void,
    *mut c_void,
) -> *mut paimon_error = paimon_table_write_write_arrow_batch;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
) -> *mut paimon_error = paimon_table_commit_commit;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
) -> *mut paimon_error = paimon_table_commit_overwrite;
const _: unsafe extern "C" fn(*const paimon_table_commit) -> *mut paimon_error =
    paimon_table_commit_truncate_table;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
) -> *mut paimon_error = paimon_table_commit_abort;
