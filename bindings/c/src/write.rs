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

use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use arrow_array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use paimon::table::Table;

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{
    paimon_result_prepare_commit, paimon_result_table_commit, paimon_result_table_write,
    paimon_result_write_builder,
};
use crate::runtime;
use crate::types::*;

// ======================= WriteBuilder ===============================

unsafe fn new_write_builder(
    table: *const paimon_table,
    commit_user: Option<String>,
) -> paimon_result_write_builder {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_write_builder {
            write_builder: ptr::null_mut(),
            error: e,
        };
    }
    let table_ref = &*((*table).inner as *const Table);
    let builder = table_ref.new_write_builder();
    let commit_user = match commit_user {
        Some(commit_user) => match builder.with_commit_user(commit_user) {
            Ok(builder) => builder.commit_user().to_string(),
            Err(e) => {
                return paimon_result_write_builder {
                    write_builder: ptr::null_mut(),
                    error: paimon_error::from_paimon(e),
                }
            }
        },
        None => builder.commit_user().to_string(),
    };
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
    new_write_builder(table, None)
}

/// Create a WriteBuilder with a caller-provided stable commit identity.
///
/// Writers whose messages are merged into one logical commit must use the
/// same `commit_user`.
///
/// # Safety
/// `table` must be a valid table pointer. `commit_user` must be a valid UTF-8
/// C string and a safe file-name segment.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_write_builder_with_commit_user(
    table: *const paimon_table,
    commit_user: *const c_char,
) -> paimon_result_write_builder {
    let commit_user = match validate_cstr(commit_user, "commit_user") {
        Ok(commit_user) => commit_user,
        Err(error) => {
            return paimon_result_write_builder {
                write_builder: ptr::null_mut(),
                error,
            }
        }
    };
    new_write_builder(table, Some(commit_user))
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

fn invalid_input(message: impl Into<String>) -> *mut paimon_error {
    paimon_error::new(PaimonErrorCode::InvalidInput, message.into())
}

fn validate_batch_schema(
    input: &RecordBatch,
    target: &ArrowSchema,
) -> Result<(), *mut paimon_error> {
    let input_schema = input.schema();
    if input_schema.fields().len() != target.fields().len() {
        return Err(invalid_input(format!(
            "Input schema is not consistent with the table schema. input: {input_schema:?}, table: {target:?}"
        )));
    }
    for (index, (input_field, target_field)) in input_schema
        .fields()
        .iter()
        .zip(target.fields().iter())
        .enumerate()
    {
        if input_field.name() != target_field.name()
            || input_field.data_type() != target_field.data_type()
        {
            return Err(invalid_input(format!(
                "Input schema is not consistent with the table schema. input: {input_schema:?}, table: {target:?}"
            )));
        }
        if !target_field.is_nullable() && input.column(index).null_count() != 0 {
            return Err(invalid_input(format!(
                "Column '{}' is NOT NULL but the Arrow batch contains {} null value(s)",
                target_field.name(),
                input.column(index).null_count()
            )));
        }
    }
    Ok(())
}

unsafe fn import_record_batch(
    array: *mut c_void,
    schema: *mut c_void,
) -> Result<RecordBatch, *mut paimon_error> {
    // Arrow's from_raw implements the C Data Interface move operation: it
    // replaces the caller-owned struct with an empty/released value.
    let ffi_array = FFI_ArrowArray::from_raw(array as *mut FFI_ArrowArray);
    let ffi_schema = FFI_ArrowSchema::from_raw(schema as *mut FFI_ArrowSchema);
    let data = match from_ffi(ffi_array, &ffi_schema) {
        Ok(data) => data,
        Err(e) => {
            drop(ffi_schema);
            return Err(invalid_input(format!(
                "Failed to import Arrow record batch: {e}"
            )));
        }
    };
    drop(ffi_schema);

    if !matches!(data.data_type(), ArrowDataType::Struct(_)) {
        return Err(invalid_input(format!(
            "Arrow record batch root must be Struct, got {:?}",
            data.data_type()
        )));
    }

    let struct_array = StructArray::from(data);
    if struct_array.null_count() != 0 {
        return Err(invalid_input(
            "Arrow record batch root Struct must not contain nulls",
        ));
    }

    let row_count = struct_array.len();
    let (fields, columns, _) = struct_array.into_parts();
    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| invalid_input(format!("Failed to construct Arrow record batch: {e}")))
}

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

    let target_schema =
        match paimon::arrow::build_target_arrow_schema(state.table.schema().fields()) {
            Ok(schema) => schema,
            Err(e) => {
                return paimon_result_table_write {
                    write: ptr::null_mut(),
                    error: paimon_error::from_paimon(e),
                }
            }
        };
    let table_write = TableWriteState {
        write: tw,
        target_schema,
        table_location: state.table.location().to_string(),
        commit_user: state.commit_user.clone(),
    };
    let inner = Box::into_raw(Box::new(table_write)) as *mut c_void;
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
            drop(Box::from_raw(wrapper.inner as *mut TableWriteState));
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

    let table_write = &mut *((*tw).inner as *mut TableWriteState);
    let batch = match import_record_batch(array, schema) {
        Ok(batch) => batch,
        Err(error) => return error,
    };
    if let Err(error) = validate_batch_schema(&batch, &table_write.target_schema) {
        return error;
    }

    match runtime().block_on(table_write.write.write_arrow_batch(&batch)) {
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
    let table_write = &mut *((*tw).inner as *mut TableWriteState);

    match runtime().block_on(table_write.write.prepare_commit()) {
        Ok(messages) => {
            let messages = CommitMessagesState {
                messages,
                table_location: table_write.table_location.clone(),
                commit_user: table_write.commit_user.clone(),
            };
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

    let table_commit = TableCommitState {
        commit: tc,
        table_location: state.table.location().to_string(),
        commit_user: state.commit_user.clone(),
    };
    let inner = Box::into_raw(Box::new(table_commit)) as *mut c_void;
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
            drop(Box::from_raw(wrapper.inner as *mut TableCommitState));
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
            drop(Box::from_raw(wrapper.inner as *mut CommitMessagesState));
        }
    }
}

/// Merge `source` messages into `target` for one logical commit.
///
/// Both handles retain ownership and must be freed separately. They must have
/// been prepared for the same table and `commit_user`.
///
/// # Safety
/// `target` and `source` must be distinct valid commit-message handles.
#[no_mangle]
pub unsafe extern "C" fn paimon_commit_messages_merge(
    target: *mut paimon_commit_messages,
    source: *const paimon_commit_messages,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(target, "target") {
        return error;
    }
    if let Err(error) = check_non_null(source, "source") {
        return error;
    }
    if ptr::eq(target, source.cast_mut()) {
        return invalid_input("target and source commit messages must be distinct handles");
    }

    let target = &mut *((*target).inner as *mut CommitMessagesState);
    let source = &*((*source).inner as *const CommitMessagesState);
    if target.table_location != source.table_location || target.commit_user != source.commit_user {
        return invalid_input(
            "commit messages can only be merged when table and commit_user both match",
        );
    }
    target.messages.extend(source.messages.clone());
    ptr::null_mut()
}

// ======================= Commit operations ===============================

fn validate_commit_context(
    commit: &TableCommitState,
    messages: &CommitMessagesState,
) -> Result<(), *mut paimon_error> {
    if commit.table_location != messages.table_location {
        return Err(invalid_input(format!(
            "commit messages were prepared for a different table (message table '{}', committer table '{}')",
            messages.table_location, commit.table_location
        )));
    }
    if commit.commit_user != messages.commit_user {
        return Err(invalid_input(
            "commit messages were prepared with a different commit_user",
        ));
    }
    Ok(())
}

/// Commit the given messages in APPEND mode.
///
/// Empty messages is a no-op success.
/// The caller retains ownership of `msgs`; it may retry after an error and
/// must eventually release the handle with `paimon_commit_messages_free`.
///
/// # Safety
/// `tc` must be a valid pointer from `paimon_write_builder_new_commit`, or null (returns error).
/// `msgs` must be a valid pointer from `paimon_table_write_prepare_commit`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_commit(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    paimon_table_commit_commit_with_identifier(tc, msgs, i64::MAX)
}

/// Commit the given messages with a caller-provided identifier.
///
/// Identifiers must increase monotonically for a `commit_user`. All messages
/// for one identifier must be merged and submitted in a single call.
/// This operation does not filter previously committed identifiers. Use
/// `paimon_table_commit_filter_and_commit_with_identifier` when retrying an
/// uncertain result.
/// The caller retains ownership of `msgs` and must free it explicitly.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_commit_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }
    if let Err(e) = check_non_null(msgs, "msgs") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommitState);
    let messages = &*((*msgs).inner as *const CommitMessagesState);
    if let Err(error) = validate_commit_context(table_commit, messages) {
        return error;
    }

    match runtime().block_on(
        table_commit
            .commit
            .commit_with_identifier(messages.messages.clone(), commit_identifier),
    ) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

/// Filter a previously committed identifier, then commit if it is new.
///
/// Identifiers must increase monotonically for a `commit_user`. Use this only
/// to retry the same uncertain result after all writer messages for the
/// logical commit have been merged.
/// The caller retains ownership of `msgs` and must free it explicitly.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_filter_and_commit_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }
    if let Err(e) = check_non_null(msgs, "msgs") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommitState);
    let messages = &*((*msgs).inner as *const CommitMessagesState);
    if let Err(error) = validate_commit_context(table_commit, messages) {
        return error;
    }

    match runtime().block_on(
        table_commit
            .commit
            .filter_and_commit_with_identifier(messages.messages.clone(), commit_identifier),
    ) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

/// Commit in OVERWRITE mode, replacing data in the written partitions.
///
/// `static_partitions` is currently passed as `None` (overwrite all
/// partitions that were written to).
/// The caller retains ownership of `msgs`; it may retry after an error and
/// must eventually release the handle with `paimon_commit_messages_free`.
///
/// # Safety
/// `tc` must be a valid pointer from `paimon_write_builder_new_commit`, or null (returns error).
/// `msgs` must be a valid pointer from `paimon_table_write_prepare_commit`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_overwrite(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    paimon_table_commit_overwrite_impl(tc, msgs, None)
}

/// Overwrite with a caller-provided stable commit identifier.
///
/// The caller retains ownership of `msgs` and must free it explicitly.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_overwrite_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    paimon_table_commit_overwrite_impl(tc, msgs, Some(commit_identifier))
}

unsafe fn paimon_table_commit_overwrite_impl(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: Option<i64>,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }
    if let Err(e) = check_non_null(msgs, "msgs") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommitState);
    let messages = &*((*msgs).inner as *const CommitMessagesState);
    if let Err(error) = validate_commit_context(table_commit, messages) {
        return error;
    }

    let result = match commit_identifier {
        Some(commit_identifier) => {
            runtime().block_on(table_commit.commit.overwrite_with_identifier(
                messages.messages.clone(),
                None,
                commit_identifier,
            ))
        }
        None => runtime().block_on(
            table_commit
                .commit
                .overwrite(messages.messages.clone(), None),
        ),
    };
    match result {
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
    paimon_table_commit_truncate_table_impl(tc, None)
}

/// Truncate the table with a caller-provided stable commit identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_truncate_table_with_identifier(
    tc: *const paimon_table_commit,
    commit_identifier: i64,
) -> *mut paimon_error {
    paimon_table_commit_truncate_table_impl(tc, Some(commit_identifier))
}

unsafe fn paimon_table_commit_truncate_table_impl(
    tc: *const paimon_table_commit,
    commit_identifier: Option<i64>,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(tc, "tc") {
        return e;
    }

    let table_commit = &*((*tc).inner as *const TableCommitState);

    let result = match commit_identifier {
        Some(commit_identifier) => runtime().block_on(
            table_commit
                .commit
                .truncate_table_with_identifier(commit_identifier),
        ),
        None => runtime().block_on(table_commit.commit.truncate_table()),
    };
    match result {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

/// Abort a prepared commit, cleaning up written data files.
///
/// This is a best-effort cleanup — it attempts to delete new data, changelog,
/// and index files produced by the writer. Storage deletion errors are ignored
/// so cleanup does not mask an earlier write or commit failure.
/// The caller retains ownership of `msgs` and must free it explicitly.
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

    let table_commit = &*((*tc).inner as *const TableCommitState);
    let messages = &*((*msgs).inner as *const CommitMessagesState);
    if let Err(error) = validate_commit_context(table_commit, messages) {
        return error;
    }

    match runtime().block_on(table_commit.commit.abort(&messages.messages)) {
        Ok(()) => ptr::null_mut(),
        Err(e) => paimon_error::from_paimon(e),
    }
}

// --- C ABI signature guards -------------------------------------------------

const _: unsafe extern "C" fn(*const paimon_table) -> paimon_result_write_builder =
    paimon_table_new_write_builder;
const _: unsafe extern "C" fn(*const paimon_table, *const c_char) -> paimon_result_write_builder =
    paimon_table_new_write_builder_with_commit_user;
const _: unsafe extern "C" fn(*const paimon_write_builder) -> paimon_result_table_write =
    paimon_write_builder_new_write;
const _: unsafe extern "C" fn(*const paimon_write_builder) -> paimon_result_table_commit =
    paimon_write_builder_new_commit;
const _: unsafe extern "C" fn(*mut paimon_table_write) -> paimon_result_prepare_commit =
    paimon_table_write_prepare_commit;
const _: unsafe extern "C" fn(
    *mut paimon_commit_messages,
    *const paimon_commit_messages,
) -> *mut paimon_error = paimon_commit_messages_merge;
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
    i64,
) -> *mut paimon_error = paimon_table_commit_commit_with_identifier;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
    i64,
) -> *mut paimon_error = paimon_table_commit_filter_and_commit_with_identifier;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
) -> *mut paimon_error = paimon_table_commit_overwrite;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
    i64,
) -> *mut paimon_error = paimon_table_commit_overwrite_with_identifier;
const _: unsafe extern "C" fn(*const paimon_table_commit) -> *mut paimon_error =
    paimon_table_commit_truncate_table;
const _: unsafe extern "C" fn(*const paimon_table_commit, i64) -> *mut paimon_error =
    paimon_table_commit_truncate_table_with_identifier;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
) -> *mut paimon_error = paimon_table_commit_abort;
