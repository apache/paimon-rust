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

use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_void};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use arrow_array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use paimon::table::{CommitMessage, PostponeBucketPlan, Table};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{
    paimon_result_bytes, paimon_result_postpone_fixed_bucket_prepare_commit,
    paimon_result_postpone_fixed_bucket_table_commit,
    paimon_result_postpone_fixed_bucket_table_write,
    paimon_result_postpone_fixed_bucket_write_builder, paimon_result_prepare_commit,
    paimon_result_prepared_commit, paimon_result_table_commit, paimon_result_table_write,
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
        Some(commit_user) => builder
            .with_commit_user(commit_user)
            .map(|builder| builder.commit_user().to_string()),
        None => Ok(builder.commit_user().to_string()),
    };
    let state = match commit_user {
        Ok(commit_user) => WriteBuilderState {
            table: table_ref.clone(),
            commit_user,
            overwrite: false,
        },
        Err(error) => {
            return paimon_result_write_builder {
                write_builder: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            }
        }
    };
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    paimon_result_write_builder {
        write_builder: Box::into_raw(Box::new(paimon_write_builder { inner })),
        error: ptr::null_mut(),
    }
}

unsafe fn new_postpone_fixed_bucket_write_builder(
    table: *const paimon_table,
    commit_user: Option<String>,
) -> paimon_result_postpone_fixed_bucket_write_builder {
    if let Err(error) = check_non_null(table, "table") {
        return paimon_result_postpone_fixed_bucket_write_builder {
            write_builder: ptr::null_mut(),
            error,
        };
    }
    let table_ref = &*((*table).inner as *const Table);
    let builder = match table_ref.new_postpone_fixed_bucket_write_builder() {
        Ok(builder) => builder,
        Err(error) => {
            return paimon_result_postpone_fixed_bucket_write_builder {
                write_builder: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            }
        }
    };
    let commit_user = match commit_user {
        Some(commit_user) => match builder.with_commit_user(commit_user) {
            Ok(builder) => builder.commit_user().to_string(),
            Err(error) => {
                return paimon_result_postpone_fixed_bucket_write_builder {
                    write_builder: ptr::null_mut(),
                    error: paimon_error::from_paimon(error),
                }
            }
        },
        None => builder.commit_user().to_string(),
    };
    let state = PostponeFixedBucketWriteBuilderState {
        table: table_ref.clone(),
        commit_user,
        overwrite: false,
        bucket_plan: None,
    };
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    paimon_result_postpone_fixed_bucket_write_builder {
        write_builder: Box::into_raw(Box::new(paimon_postpone_fixed_bucket_write_builder {
            inner,
        })),
        error: ptr::null_mut(),
    }
}

unsafe fn new_write_builder_with_commit_user(
    table: *const paimon_table,
    commit_user: *const c_char,
) -> paimon_result_write_builder {
    match validate_cstr(commit_user, "commit_user") {
        Ok(commit_user) => new_write_builder(table, Some(commit_user)),
        Err(error) => paimon_result_write_builder {
            write_builder: ptr::null_mut(),
            error,
        },
    }
}

unsafe fn new_postpone_fixed_bucket_write_builder_with_commit_user(
    table: *const paimon_table,
    commit_user: *const c_char,
) -> paimon_result_postpone_fixed_bucket_write_builder {
    match validate_cstr(commit_user, "commit_user") {
        Ok(commit_user) => new_postpone_fixed_bucket_write_builder(table, Some(commit_user)),
        Err(error) => paimon_result_postpone_fixed_bucket_write_builder {
            write_builder: ptr::null_mut(),
            error,
        },
    }
}

/// Create a new WriteBuilder from a Table.
///
/// The returned WriteBuilder holds a shared `commit_user` (UUID) that will be
/// used by both `new_write()` and `new_commit()` for duplicate-commit detection.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table` or
/// `paimon_table_from_schema_json`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_write_builder(
    table: *const paimon_table,
) -> paimon_result_write_builder {
    new_write_builder(table, None)
}

/// Create a one-shot fixed-bucket WriteBuilder for a postpone table.
/// A bucket plan must be set before creating a writer.
///
/// # Safety
/// `table` must be a valid table pointer, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_postpone_fixed_bucket_write_builder(
    table: *const paimon_table,
) -> paimon_result_postpone_fixed_bucket_write_builder {
    new_postpone_fixed_bucket_write_builder(table, None)
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
    new_write_builder_with_commit_user(table, commit_user)
}

/// Create a fixed-bucket WriteBuilder with a stable commit identity.
/// A bucket plan must be set before creating a writer.
///
/// # Safety
/// `table` must be a valid table pointer. `commit_user` must be a valid UTF-8
/// C string and a safe file-name segment.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user(
    table: *const paimon_table,
    commit_user: *const c_char,
) -> paimon_result_postpone_fixed_bucket_write_builder {
    new_postpone_fixed_bucket_write_builder_with_commit_user(table, commit_user)
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

/// Free a postpone fixed-bucket write builder.
///
/// # Safety
/// Only call with a builder returned from
/// `paimon_table_new_postpone_fixed_bucket_write_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_write_builder_free(
    wb: *mut paimon_postpone_fixed_bucket_write_builder,
) {
    if !wb.is_null() {
        let wrapper = Box::from_raw(wb);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(
                wrapper.inner as *mut PostponeFixedBucketWriteBuilderState,
            ));
        }
    }
}

/// Enable overwrite mode for a postpone fixed-bucket write operation.
///
/// # Safety
/// `wb` must be a valid fixed-bucket builder, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_write_builder_with_overwrite(
    wb: *mut paimon_postpone_fixed_bucket_write_builder,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(wb, "wb") {
        return error;
    }
    let state = &mut *((*wb).inner as *mut PostponeFixedBucketWriteBuilderState);
    state.overwrite = true;
    ptr::null_mut()
}

/// Set a shared `partition -> total_buckets` plan.
/// The caller retains ownership when pointer or builder validation fails. Once
/// Arrow import starts, this call consumes both structs even if plan validation
/// returns an error.
///
/// # Safety
/// `wb` must be a valid postpone fixed-bucket builder. `array` and
/// `schema` must point to initialized Arrow C Data structs.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_write_builder_with_bucket_plan(
    wb: *mut paimon_postpone_fixed_bucket_write_builder,
    array: *mut c_void,
    schema: *mut c_void,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(wb, "wb") {
        return error;
    }
    if let Err(error) = check_non_null(array, "array") {
        return error;
    }
    if let Err(error) = check_non_null(schema, "schema") {
        return error;
    }
    let state = &mut *((*wb).inner as *mut PostponeFixedBucketWriteBuilderState);

    let batch = match import_record_batch(array, schema) {
        Ok(batch) => batch,
        Err(error) => return error,
    };
    match PostponeBucketPlan::from_arrow(&state.table, &batch) {
        Ok(plan) => {
            state.bucket_plan = Some(plan);
            ptr::null_mut()
        }
        Err(error) => paimon_error::from_paimon(error),
    }
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

/// Create a standard TableWrite from a standard WriteBuilder.
///
/// # Safety
/// wb must be a valid standard builder, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_write_builder_new_write(
    wb: *const paimon_write_builder,
) -> paimon_result_table_write {
    if let Err(error) = check_non_null(wb, "wb") {
        return paimon_result_table_write {
            write: ptr::null_mut(),
            error,
        };
    }
    let state = &*((*wb).inner as *const WriteBuilderState);
    let mut builder = match state
        .table
        .new_write_builder()
        .with_commit_user(state.commit_user.clone())
    {
        Ok(builder) => builder,
        Err(error) => {
            return paimon_result_table_write {
                write: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            };
        }
    };
    if state.overwrite {
        builder = builder.with_overwrite();
    }
    let result = builder.new_write().and_then(|write| {
        paimon::arrow::build_target_arrow_schema(state.table.schema().fields())
            .map(|schema| (Box::new(write), schema))
    });
    let (write, target_schema) = match result {
        Ok(result) => result,
        Err(error) => {
            return paimon_result_table_write {
                write: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            };
        }
    };
    let inner = Box::into_raw(Box::new(TableWriteState {
        write,
        overwrite: state.overwrite,
        target_schema,
        table_location: state.table.location().to_string(),
        commit_user: state.commit_user.clone(),
    })) as *mut c_void;
    paimon_result_table_write {
        write: Box::into_raw(Box::new(paimon_table_write { inner })),
        error: ptr::null_mut(),
    }
}

/// Create a postpone fixed-bucket TableWrite.
///
/// # Safety
/// wb must be a valid fixed-bucket builder, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_write_builder_new_write(
    wb: *const paimon_postpone_fixed_bucket_write_builder,
) -> paimon_result_postpone_fixed_bucket_table_write {
    if let Err(error) = check_non_null(wb, "wb") {
        return paimon_result_postpone_fixed_bucket_table_write {
            write: ptr::null_mut(),
            error,
        };
    }
    let state = &*((*wb).inner as *const PostponeFixedBucketWriteBuilderState);
    let mut builder = match state
        .table
        .new_postpone_fixed_bucket_write_builder()
        .and_then(|builder| builder.with_commit_user(state.commit_user.clone()))
    {
        Ok(builder) => builder,
        Err(error) => {
            return paimon_result_postpone_fixed_bucket_table_write {
                write: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            };
        }
    };
    if let Some(plan) = state.bucket_plan.clone() {
        builder = builder.with_bucket_plan(plan);
    }
    if state.overwrite {
        builder = builder.with_overwrite();
    }
    let result = builder.new_write().and_then(|write| {
        paimon::arrow::build_target_arrow_schema(state.table.schema().fields())
            .map(|schema| (Box::new(write), schema))
    });
    let (write, target_schema) = match result {
        Ok(result) => result,
        Err(error) => {
            return paimon_result_postpone_fixed_bucket_table_write {
                write: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            };
        }
    };
    let inner = Box::into_raw(Box::new(PostponeFixedBucketTableWriteState {
        write,
        overwrite: state.overwrite,
        target_schema,
        table_location: state.table.location().to_string(),
        commit_user: state.commit_user.clone(),
    })) as *mut c_void;
    paimon_result_postpone_fixed_bucket_table_write {
        write: Box::into_raw(Box::new(paimon_postpone_fixed_bucket_table_write { inner })),
        error: ptr::null_mut(),
    }
}

/// Free a standard TableWrite.
///
/// # Safety
/// Only call with a write returned from paimon_write_builder_new_write.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_write_free(tw: *mut paimon_table_write) {
    if !tw.is_null() {
        let wrapper = Box::from_raw(tw);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableWriteState));
        }
    }
}

/// Free a postpone fixed-bucket TableWrite.
///
/// # Safety
/// Only call with a write returned from
/// paimon_postpone_fixed_bucket_write_builder_new_write.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_write_free(
    tw: *mut paimon_postpone_fixed_bucket_table_write,
) {
    if !tw.is_null() {
        let wrapper = Box::from_raw(tw);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(
                wrapper.inner as *mut PostponeFixedBucketTableWriteState,
            ));
        }
    }
}

unsafe fn import_write_batch(
    array: *mut c_void,
    schema: *mut c_void,
    target_schema: &ArrowSchema,
) -> Result<RecordBatch, *mut paimon_error> {
    check_non_null(array, "array")?;
    check_non_null(schema, "schema")?;
    let batch = import_record_batch(array, schema)?;
    validate_batch_schema(&batch, target_schema)?;
    Ok(batch)
}

/// Write one Arrow record batch with a standard TableWrite.
///
/// Ownership of array and schema is transferred once Arrow import starts.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_write_write_arrow_batch(
    tw: *mut paimon_table_write,
    array: *mut c_void,
    schema: *mut c_void,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tw, "tw") {
        return error;
    }
    let table_write = &mut *((*tw).inner as *mut TableWriteState);
    let batch = match import_write_batch(array, schema, &table_write.target_schema) {
        Ok(batch) => batch,
        Err(error) => return error,
    };
    match runtime().block_on(table_write.write.write_arrow_batch(&batch)) {
        Ok(()) => ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Write one Arrow record batch with a postpone fixed-bucket TableWrite.
///
/// Ownership of array and schema is transferred once Arrow import starts.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_write_write_arrow_batch(
    tw: *mut paimon_postpone_fixed_bucket_table_write,
    array: *mut c_void,
    schema: *mut c_void,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tw, "tw") {
        return error;
    }
    let table_write = &mut *((*tw).inner as *mut PostponeFixedBucketTableWriteState);
    let batch = match import_write_batch(array, schema, &table_write.target_schema) {
        Ok(batch) => batch,
        Err(error) => return error,
    };
    match runtime().block_on(table_write.write.write_arrow_batch(&batch)) {
        Ok(()) => ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Prepare standard commit messages.
///
/// The returned handle remains owned by the caller.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_write_prepare_commit(
    tw: *mut paimon_table_write,
) -> paimon_result_prepare_commit {
    if let Err(error) = check_non_null(tw, "tw") {
        return paimon_result_prepare_commit {
            messages: ptr::null_mut(),
            error,
        };
    }
    let table_write = &mut *((*tw).inner as *mut TableWriteState);
    match runtime().block_on(table_write.write.prepare_commit()) {
        Ok(messages) => {
            let inner = Box::into_raw(Box::new(CommitMessagesState {
                messages,
                overwrite: table_write.overwrite,
                table_location: table_write.table_location.clone(),
                commit_user: table_write.commit_user.clone(),
            })) as *mut c_void;
            paimon_result_prepare_commit {
                messages: Box::into_raw(Box::new(paimon_commit_messages { inner })),
                error: ptr::null_mut(),
            }
        }
        Err(error) => paimon_result_prepare_commit {
            messages: ptr::null_mut(),
            error: paimon_error::from_paimon(error),
        },
    }
}

/// Prepare postpone fixed-bucket commit messages.
///
/// The returned handle remains owned by the caller.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_write_prepare_commit(
    tw: *mut paimon_postpone_fixed_bucket_table_write,
) -> paimon_result_postpone_fixed_bucket_prepare_commit {
    if let Err(error) = check_non_null(tw, "tw") {
        return paimon_result_postpone_fixed_bucket_prepare_commit {
            messages: ptr::null_mut(),
            error,
        };
    }
    let table_write = &mut *((*tw).inner as *mut PostponeFixedBucketTableWriteState);
    match runtime().block_on(table_write.write.prepare_commit()) {
        Ok(messages) => {
            let inner = Box::into_raw(Box::new(PostponeFixedBucketCommitMessagesState {
                messages,
                overwrite: table_write.overwrite,
                table_location: table_write.table_location.clone(),
                commit_user: table_write.commit_user.clone(),
            })) as *mut c_void;
            paimon_result_postpone_fixed_bucket_prepare_commit {
                messages: Box::into_raw(Box::new(paimon_postpone_fixed_bucket_commit_messages {
                    inner,
                })),
                error: ptr::null_mut(),
            }
        }
        Err(error) => paimon_result_postpone_fixed_bucket_prepare_commit {
            messages: ptr::null_mut(),
            error: paimon_error::from_paimon(error),
        },
    }
}

// ======================= TableCommit ===============================

/// Create a standard TableCommit from a standard WriteBuilder.
#[no_mangle]
pub unsafe extern "C" fn paimon_write_builder_new_commit(
    wb: *const paimon_write_builder,
) -> paimon_result_table_commit {
    if let Err(error) = check_non_null(wb, "wb") {
        return paimon_result_table_commit {
            commit: ptr::null_mut(),
            error,
        };
    }
    let state = &*((*wb).inner as *const WriteBuilderState);
    let commit = match state
        .table
        .new_write_builder()
        .with_commit_user(state.commit_user.clone())
        .and_then(|builder| builder.try_new_commit())
    {
        Ok(commit) => commit,
        Err(error) => {
            return paimon_result_table_commit {
                commit: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            };
        }
    };
    let inner = Box::into_raw(Box::new(TableCommitState {
        commit,
        overwrite: state.overwrite,
        table_location: state.table.location().to_string(),
        commit_user: state.commit_user.clone(),
    })) as *mut c_void;
    paimon_result_table_commit {
        commit: Box::into_raw(Box::new(paimon_table_commit { inner })),
        error: ptr::null_mut(),
    }
}

/// Create a postpone fixed-bucket TableCommit.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_write_builder_new_commit(
    wb: *const paimon_postpone_fixed_bucket_write_builder,
) -> paimon_result_postpone_fixed_bucket_table_commit {
    if let Err(error) = check_non_null(wb, "wb") {
        return paimon_result_postpone_fixed_bucket_table_commit {
            commit: ptr::null_mut(),
            error,
        };
    }
    let state = &*((*wb).inner as *const PostponeFixedBucketWriteBuilderState);
    let builder = match state
        .table
        .new_postpone_fixed_bucket_write_builder()
        .and_then(|builder| builder.with_commit_user(state.commit_user.clone()))
    {
        Ok(builder) => builder,
        Err(error) => {
            return paimon_result_postpone_fixed_bucket_table_commit {
                commit: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            };
        }
    };
    let builder = if state.overwrite {
        builder.with_overwrite()
    } else {
        builder
    };
    let commit = match builder.try_new_commit() {
        Ok(commit) => commit,
        Err(error) => {
            return paimon_result_postpone_fixed_bucket_table_commit {
                commit: ptr::null_mut(),
                error: paimon_error::from_paimon(error),
            };
        }
    };
    let inner = Box::into_raw(Box::new(PostponeFixedBucketTableCommitState {
        commit,
        overwrite: state.overwrite,
        table_location: state.table.location().to_string(),
        commit_user: state.commit_user.clone(),
    })) as *mut c_void;
    paimon_result_postpone_fixed_bucket_table_commit {
        commit: Box::into_raw(Box::new(paimon_postpone_fixed_bucket_table_commit {
            inner,
        })),
        error: ptr::null_mut(),
    }
}

/// Free a standard TableCommit.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_free(tc: *mut paimon_table_commit) {
    if !tc.is_null() {
        let wrapper = Box::from_raw(tc);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableCommitState));
        }
    }
}

/// Free a postpone fixed-bucket TableCommit.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_free(
    tc: *mut paimon_postpone_fixed_bucket_table_commit,
) {
    if !tc.is_null() {
        let wrapper = Box::from_raw(tc);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(
                wrapper.inner as *mut PostponeFixedBucketTableCommitState,
            ));
        }
    }
}

// ======================= CommitMessages ===============================

/// Free standard commit messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_commit_messages_free(msgs: *mut paimon_commit_messages) {
    if !msgs.is_null() {
        let wrapper = Box::from_raw(msgs);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut CommitMessagesState));
        }
    }
}

/// Free postpone fixed-bucket commit messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_commit_messages_free(
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
) {
    if !msgs.is_null() {
        let wrapper = Box::from_raw(msgs);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(
                wrapper.inner as *mut PostponeFixedBucketCommitMessagesState,
            ));
        }
    }
}

const PREPARED_COMMIT_FORMAT: &str = "paimon-rust-prepared-commit";
// Version 2 adds strict resource and path validation. Version 1 is rejected:
// accepting its unconstrained internal CommitMessage representation would
// reintroduce unsafe file references after recovery.
const PREPARED_COMMIT_VERSION: u32 = 2;
const MAX_PREPARED_COMMIT_BYTES: usize = 64 * 1024 * 1024;
const MAX_PREPARED_MESSAGES: usize = 100_000;
const MAX_PREPARED_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_FILES_PER_MESSAGE: usize = 100_000;
const MAX_TOTAL_FILE_REFERENCES: usize = 1_000_000;
const MAX_EXTRA_FILES_PER_DATA_FILE: usize = 10_000;
const MAX_PARTITION_BYTES: usize = 16 * 1024 * 1024;
const MAX_IDENTITY_BYTES: usize = 1024 * 1024;
const MAX_FILE_NAME_BYTES: usize = 4 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedCommitEnvelope {
    format: String,
    version: u32,
    commit_identifier: i64,
    table_location: String,
    commit_user: String,
    overwrite: bool,
    messages: Vec<paimon::table::CommitMessage>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPreparedCommitEnvelope<'a> {
    format: String,
    version: u32,
    commit_identifier: i64,
    table_location: String,
    commit_user: String,
    overwrite: bool,
    #[serde(borrow, deserialize_with = "deserialize_bounded_raw_messages")]
    messages: Vec<&'a RawValue>,
}

fn deserialize_bounded_raw_messages<'de, D>(deserializer: D) -> Result<Vec<&'de RawValue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, IgnoredAny, SeqAccess, Visitor};

    struct RawMessagesVisitor;

    impl<'de> Visitor<'de> for RawMessagesVisitor {
        type Value = Vec<&'de RawValue>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded list of prepared commit messages")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut messages = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_PREPARED_MESSAGES),
            );
            while messages.len() < MAX_PREPARED_MESSAGES {
                let Some(message) = sequence.next_element::<&'de RawValue>()? else {
                    return Ok(messages);
                };
                messages.push(message);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format!(
                    "prepared commit contains more than {MAX_PREPARED_MESSAGES} messages"
                )));
            }
            Ok(messages)
        }
    }

    deserializer.deserialize_seq(RawMessagesVisitor)
}

fn prepared_panic_error(operation: &str) -> *mut paimon_error {
    paimon_error::new(
        PaimonErrorCode::Unexpected,
        format!("Rust panic while executing {operation}"),
    )
}

fn validate_file_component(kind: &str, name: &str) -> Result<(), *mut paimon_error> {
    if name.is_empty()
        || name.len() > MAX_FILE_NAME_BYTES
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err(invalid_input(format!(
            "prepared commit contains unsafe {kind} '{name}'"
        )));
    }
    Ok(())
}

fn validate_data_file(file: &paimon::spec::DataFileMeta) -> Result<usize, *mut paimon_error> {
    if file.external_path.is_some() {
        return Err(invalid_input(
            "prepared commits with external data-file paths are not supported",
        ));
    }
    validate_file_component("data file name", &file.file_name)?;
    if file.extra_files.len() > MAX_EXTRA_FILES_PER_DATA_FILE {
        return Err(invalid_input(format!(
            "data file contains {} extra files; maximum is {}",
            file.extra_files.len(),
            MAX_EXTRA_FILES_PER_DATA_FILE
        )));
    }
    for extra in &file.extra_files {
        validate_file_component("extra file name", extra)?;
    }
    Ok(1 + file.extra_files.len())
}

fn validate_index_file(file: &paimon::spec::IndexFileMeta) -> Result<usize, *mut paimon_error> {
    validate_file_component("index file name", &file.file_name)?;
    if let Some(ranges) = &file.deletion_vectors_ranges {
        for data_file_name in ranges.keys() {
            validate_file_component("deletion-vector data file name", data_file_name)?;
        }
    }
    Ok(1)
}

fn validate_prepared_commit_envelope(
    envelope: &PreparedCommitEnvelope,
) -> Result<(), *mut paimon_error> {
    if envelope.commit_identifier < 0
        || envelope.commit_identifier == i64::MAX
        || envelope.table_location.is_empty()
        || envelope.table_location.len() > MAX_IDENTITY_BYTES
        || envelope.commit_user.is_empty()
        || envelope.commit_user.len() > MAX_IDENTITY_BYTES
    {
        return Err(invalid_input(
            "prepared commit contains an invalid identity",
        ));
    }
    if envelope.messages.len() > MAX_PREPARED_MESSAGES {
        return Err(invalid_input(format!(
            "prepared commit contains {} messages; maximum is {}",
            envelope.messages.len(),
            MAX_PREPARED_MESSAGES
        )));
    }

    let mut total_file_references = 0usize;
    for message in &envelope.messages {
        if message.partition.len() > MAX_PARTITION_BYTES {
            return Err(invalid_input(format!(
                "prepared commit partition exceeds {MAX_PARTITION_BYTES} bytes"
            )));
        }
        let message_file_count = message
            .new_files
            .len()
            .checked_add(message.new_changelog_files.len())
            .and_then(|count| count.checked_add(message.deleted_files.len()))
            .and_then(|count| count.checked_add(message.new_index_files.len()))
            .and_then(|count| count.checked_add(message.deleted_index_files.len()))
            .ok_or_else(|| invalid_input("prepared commit file count overflows"))?;
        if message_file_count > MAX_FILES_PER_MESSAGE {
            return Err(invalid_input(format!(
                "prepared commit message contains {message_file_count} files; maximum is {MAX_FILES_PER_MESSAGE}"
            )));
        }
        for file in message
            .new_files
            .iter()
            .chain(message.new_changelog_files.iter())
            .chain(message.deleted_files.iter())
        {
            total_file_references = total_file_references
                .checked_add(validate_data_file(file)?)
                .ok_or_else(|| invalid_input("prepared commit file count overflows"))?;
        }
        for file in message
            .new_index_files
            .iter()
            .chain(message.deleted_index_files.iter())
        {
            total_file_references = total_file_references
                .checked_add(validate_index_file(file)?)
                .ok_or_else(|| invalid_input("prepared commit file count overflows"))?;
        }
        if total_file_references > MAX_TOTAL_FILE_REFERENCES {
            return Err(invalid_input(format!(
                "prepared commit contains more than {MAX_TOTAL_FILE_REFERENCES} file references"
            )));
        }
    }
    Ok(())
}

fn empty_bytes() -> paimon_bytes {
    paimon_bytes {
        data: ptr::null_mut(),
        len: 0,
    }
}

/// Bind standard commit messages to a monotonically increasing streaming
/// commit identifier. The returned prepared commit owns a clone of the
/// messages, so the source handle remains valid. Valid identifiers are in
/// `[0, INT64_MAX)`; `INT64_MAX` is reserved for unidentified batch commits.
#[no_mangle]
pub unsafe extern "C" fn paimon_commit_messages_prepare(
    msgs: *const paimon_commit_messages,
    commit_identifier: i64,
) -> paimon_result_prepared_commit {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(msgs, "msgs") {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error,
            };
        }
        if commit_identifier < 0 || commit_identifier == i64::MAX {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error: invalid_input(
                    "streaming commit_identifier must be non-negative and less than i64::MAX",
                ),
            };
        }
        let source = &*((*msgs).inner as *const CommitMessagesState);
        let mut messages = Vec::new();
        if let Err(error) = merge_messages_idempotently(&mut messages, &source.messages) {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error,
            };
        }
        let state = PreparedCommitState {
            commit_identifier,
            messages: CommitMessagesState {
                messages,
                overwrite: source.overwrite,
                table_location: source.table_location.clone(),
                commit_user: source.commit_user.clone(),
            },
        };
        let inner = Box::into_raw(Box::new(state)) as *mut c_void;
        paimon_result_prepared_commit {
            prepared: Box::into_raw(Box::new(paimon_prepared_commit { inner })),
            error: ptr::null_mut(),
        }
    }));
    outcome.unwrap_or_else(|_| paimon_result_prepared_commit {
        prepared: ptr::null_mut(),
        error: prepared_panic_error("paimon_commit_messages_prepare"),
    })
}

/// Serialize a prepared commit into a process-independent, versioned buffer.
/// The bytes must be released with `paimon_bytes_free`.
#[no_mangle]
pub unsafe extern "C" fn paimon_prepared_commit_serialize(
    prepared: *const paimon_prepared_commit,
) -> paimon_result_bytes {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(prepared, "prepared") {
            return paimon_result_bytes {
                bytes: empty_bytes(),
                error,
            };
        }
        let state = &*((*prepared).inner as *const PreparedCommitState);
        let envelope = PreparedCommitEnvelope {
            format: PREPARED_COMMIT_FORMAT.to_string(),
            version: PREPARED_COMMIT_VERSION,
            commit_identifier: state.commit_identifier,
            table_location: state.messages.table_location.clone(),
            commit_user: state.messages.commit_user.clone(),
            overwrite: state.messages.overwrite,
            messages: state.messages.messages.clone(),
        };
        if let Err(error) = validate_prepared_commit_envelope(&envelope) {
            return paimon_result_bytes {
                bytes: empty_bytes(),
                error,
            };
        }
        match serde_json::to_vec(&envelope) {
            Ok(bytes) if bytes.len() <= MAX_PREPARED_COMMIT_BYTES => paimon_result_bytes {
                bytes: paimon_bytes::new(bytes),
                error: ptr::null_mut(),
            },
            Ok(bytes) => paimon_result_bytes {
                bytes: empty_bytes(),
                error: invalid_input(format!(
                    "serialized prepared commit is {} bytes; maximum is {}",
                    bytes.len(),
                    MAX_PREPARED_COMMIT_BYTES
                )),
            },
            Err(error) => paimon_result_bytes {
                bytes: empty_bytes(),
                error: paimon_error::new(
                    PaimonErrorCode::Unexpected,
                    format!("failed to serialize prepared commit: {error}"),
                ),
            },
        }
    }));
    outcome.unwrap_or_else(|_| paimon_result_bytes {
        bytes: empty_bytes(),
        error: prepared_panic_error("paimon_prepared_commit_serialize"),
    })
}

/// Restore a prepared commit serialized by `paimon_prepared_commit_serialize`.
#[no_mangle]
pub unsafe extern "C" fn paimon_prepared_commit_deserialize(
    data: *const u8,
    len: usize,
) -> paimon_result_prepared_commit {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if data.is_null() || len == 0 {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error: invalid_input("prepared commit buffer must not be null or empty"),
            };
        }
        if len > MAX_PREPARED_COMMIT_BYTES {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error: invalid_input(format!(
                    "prepared commit buffer exceeds {MAX_PREPARED_COMMIT_BYTES} bytes"
                )),
            };
        }
        let bytes = std::slice::from_raw_parts(data, len);
        let raw: RawPreparedCommitEnvelope<'_> = match serde_json::from_slice(bytes) {
            Ok(envelope) => envelope,
            Err(error) => {
                return paimon_result_prepared_commit {
                    prepared: ptr::null_mut(),
                    error: invalid_input(format!("invalid prepared commit buffer: {error}")),
                };
            }
        };
        if raw.format != PREPARED_COMMIT_FORMAT || raw.version != PREPARED_COMMIT_VERSION {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::Unsupported,
                    format!(
                        "unsupported prepared commit format '{}' version {}",
                        raw.format, raw.version
                    ),
                ),
            };
        }
        if raw.commit_identifier < 0
            || raw.commit_identifier == i64::MAX
            || raw.table_location.is_empty()
            || raw.table_location.len() > MAX_IDENTITY_BYTES
            || raw.commit_user.is_empty()
            || raw.commit_user.len() > MAX_IDENTITY_BYTES
        {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error: invalid_input("prepared commit contains an invalid identity"),
            };
        }
        if raw.messages.len() > MAX_PREPARED_MESSAGES {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error: invalid_input(format!(
                    "prepared commit contains {} messages; maximum is {}",
                    raw.messages.len(),
                    MAX_PREPARED_MESSAGES
                )),
            };
        }
        let mut messages = Vec::with_capacity(raw.messages.len());
        for (index, raw_message) in raw.messages.into_iter().enumerate() {
            if raw_message.get().len() > MAX_PREPARED_MESSAGE_BYTES {
                return paimon_result_prepared_commit {
                    prepared: ptr::null_mut(),
                    error: invalid_input(format!(
                        "prepared commit message {index} exceeds {MAX_PREPARED_MESSAGE_BYTES} bytes"
                    )),
                };
            }
            match serde_json::from_str::<CommitMessage>(raw_message.get()) {
                Ok(message) => messages.push(message),
                Err(error) => {
                    return paimon_result_prepared_commit {
                        prepared: ptr::null_mut(),
                        error: invalid_input(format!(
                            "invalid prepared commit message {index}: {error}"
                        )),
                    };
                }
            }
        }
        let mut envelope = PreparedCommitEnvelope {
            format: raw.format,
            version: raw.version,
            commit_identifier: raw.commit_identifier,
            table_location: raw.table_location,
            commit_user: raw.commit_user,
            overwrite: raw.overwrite,
            messages,
        };
        if let Err(error) = validate_prepared_commit_envelope(&envelope) {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error,
            };
        }
        let mut normalized_messages = Vec::new();
        if let Err(error) =
            merge_messages_idempotently(&mut normalized_messages, &envelope.messages)
        {
            return paimon_result_prepared_commit {
                prepared: ptr::null_mut(),
                error,
            };
        }
        envelope.messages = normalized_messages;
        let state = PreparedCommitState {
            commit_identifier: envelope.commit_identifier,
            messages: CommitMessagesState {
                messages: envelope.messages,
                overwrite: envelope.overwrite,
                table_location: envelope.table_location,
                commit_user: envelope.commit_user,
            },
        };
        let inner = Box::into_raw(Box::new(state)) as *mut c_void;
        paimon_result_prepared_commit {
            prepared: Box::into_raw(Box::new(paimon_prepared_commit { inner })),
            error: ptr::null_mut(),
        }
    }));
    outcome.unwrap_or_else(|_| paimon_result_prepared_commit {
        prepared: ptr::null_mut(),
        error: prepared_panic_error("paimon_prepared_commit_deserialize"),
    })
}

/// Return the commit identifier carried by a prepared commit, or -1 for null.
#[no_mangle]
pub unsafe extern "C" fn paimon_prepared_commit_identifier(
    prepared: *const paimon_prepared_commit,
) -> i64 {
    catch_unwind(AssertUnwindSafe(|| {
        if prepared.is_null() || (*prepared).inner.is_null() {
            return -1;
        }
        let state = &*((*prepared).inner as *const PreparedCommitState);
        state.commit_identifier
    }))
    .unwrap_or(-1)
}

/// Free a prepared commit.
#[no_mangle]
pub unsafe extern "C" fn paimon_prepared_commit_free(prepared: *mut paimon_prepared_commit) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !prepared.is_null() {
            let wrapper = Box::from_raw(prepared);
            if !wrapper.inner.is_null() {
                drop(Box::from_raw(wrapper.inner as *mut PreparedCommitState));
            }
        }
    }));
}

fn validate_message_context(
    target_table: &str,
    target_user: &str,
    target_overwrite: bool,
    source_table: &str,
    source_user: &str,
    source_overwrite: bool,
) -> Result<(), *mut paimon_error> {
    if target_table != source_table || target_user != source_user {
        return Err(invalid_input(
            "commit messages can only be merged when table and commit_user both match",
        ));
    }
    if target_overwrite != source_overwrite {
        return Err(invalid_input(
            "commit messages can only be merged when overwrite modes match",
        ));
    }
    Ok(())
}

type CommitMessageGroupKey = (Vec<u8>, i32);
type CommitFileKey = (u8, String);

fn commit_message_file_keys(message: &CommitMessage) -> Vec<CommitFileKey> {
    let mut keys = Vec::new();
    let mut add_data_files = |category: u8, files: &[paimon::spec::DataFileMeta]| {
        for file in files {
            keys.push((category, file.file_name.clone()));
            for extra in &file.extra_files {
                keys.push((category + 1, extra.clone()));
            }
        }
    };
    add_data_files(0, &message.new_files);
    add_data_files(2, &message.new_changelog_files);
    add_data_files(4, &message.deleted_files);
    for (category, files) in [
        (6u8, &message.new_index_files),
        (7u8, &message.deleted_index_files),
    ] {
        for file in files {
            keys.push((category, file.file_name.clone()));
        }
    }
    keys
}

fn merge_messages_idempotently(
    target: &mut Vec<CommitMessage>,
    source: &[CommitMessage],
) -> Result<(), *mut paimon_error> {
    let capacity = target
        .len()
        .checked_add(source.len())
        .unwrap_or(MAX_PREPARED_MESSAGES)
        .min(MAX_PREPARED_MESSAGES);
    let mut merged = Vec::with_capacity(capacity);
    let mut key_owners: HashMap<CommitMessageGroupKey, HashMap<CommitFileKey, usize>> =
        HashMap::new();

    for message in target.iter().chain(source) {
        let message_keys = commit_message_file_keys(message);
        // Empty writer fragments do not publish any metadata and can be
        // removed without changing commit semantics.
        if message_keys.is_empty() {
            continue;
        }
        let unique_keys = message_keys.iter().cloned().collect::<HashSet<_>>();
        if unique_keys.len() != message_keys.len() {
            return Err(invalid_input(
                "commit message contains a duplicate file identity",
            ));
        }

        // Hash/copy a partition only once per message. Putting it in every
        // file key makes merge CPU and memory proportional to
        // partition_bytes * file_count.
        let group = (message.partition.clone(), message.bucket);
        let group_owners = key_owners.entry(group).or_default();
        let owners = unique_keys
            .iter()
            .filter_map(|key| group_owners.get(key).copied())
            .collect::<HashSet<_>>();
        if !owners.is_empty() {
            if owners.iter().any(|index| merged[*index] == *message) {
                continue;
            }
            return Err(invalid_input(
                "commit message merge found the same file identity with different fragment metadata",
            ));
        }
        if merged.len() >= MAX_PREPARED_MESSAGES {
            return Err(invalid_input(format!(
                "merged commit contains more than {MAX_PREPARED_MESSAGES} messages"
            )));
        }
        let owner = merged.len();
        group_owners.extend(unique_keys.into_iter().map(|key| (key, owner)));
        merged.push(message.clone());
    }
    *target = merged;
    Ok(())
}

/// Merge standard commit messages for one logical commit.
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
    if let Err(error) = validate_message_context(
        &target.table_location,
        &target.commit_user,
        target.overwrite,
        &source.table_location,
        &source.commit_user,
        source.overwrite,
    ) {
        return error;
    }
    match merge_messages_idempotently(&mut target.messages, &source.messages) {
        Ok(()) => ptr::null_mut(),
        Err(error) => error,
    }
}

/// Merge two durable prepared commits produced by parallel writers for the
/// same table, commit user, mode and identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_prepared_commit_merge(
    target: *mut paimon_prepared_commit,
    source: *const paimon_prepared_commit,
) -> *mut paimon_error {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(target, "target") {
            return error;
        }
        if let Err(error) = check_non_null(source, "source") {
            return error;
        }
        if ptr::eq(target, source.cast_mut()) {
            return invalid_input("target and source prepared commits must be distinct handles");
        }
        let target = &mut *((*target).inner as *mut PreparedCommitState);
        let source = &*((*source).inner as *const PreparedCommitState);
        if target.commit_identifier != source.commit_identifier {
            return invalid_input("prepared commits must have the same commit_identifier");
        }
        if let Err(error) = validate_message_context(
            &target.messages.table_location,
            &target.messages.commit_user,
            target.messages.overwrite,
            &source.messages.table_location,
            &source.messages.commit_user,
            source.messages.overwrite,
        ) {
            return error;
        }
        match merge_messages_idempotently(&mut target.messages.messages, &source.messages.messages)
        {
            Ok(()) => ptr::null_mut(),
            Err(error) => error,
        }
    }));
    outcome.unwrap_or_else(|_| prepared_panic_error("paimon_prepared_commit_merge"))
}

/// Merge postpone fixed-bucket messages for one logical commit.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_commit_messages_merge(
    target: *mut paimon_postpone_fixed_bucket_commit_messages,
    source: *const paimon_postpone_fixed_bucket_commit_messages,
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
    let target = &mut *((*target).inner as *mut PostponeFixedBucketCommitMessagesState);
    let source = &*((*source).inner as *const PostponeFixedBucketCommitMessagesState);
    if let Err(error) = validate_message_context(
        &target.table_location,
        &target.commit_user,
        target.overwrite,
        &source.table_location,
        &source.commit_user,
        source.overwrite,
    ) {
        return error;
    }
    match merge_messages_idempotently(&mut target.messages, &source.messages) {
        Ok(()) => ptr::null_mut(),
        Err(error) => error,
    }
}

// ======================= Commit operations ===============================

fn validate_commit_context(
    commit_table: &str,
    commit_user: &str,
    commit_overwrite: bool,
    message_table: &str,
    message_user: &str,
    message_overwrite: bool,
) -> Result<(), *mut paimon_error> {
    if commit_table != message_table {
        return Err(invalid_input(format!(
            "commit messages were prepared for a different table (message table '{}', committer table '{}')",
            message_table, commit_table
        )));
    }
    if commit_user != message_user {
        return Err(invalid_input(
            "commit messages were prepared with a different commit_user",
        ));
    }
    if commit_overwrite != message_overwrite {
        return Err(invalid_input(
            "commit messages were prepared with a different overwrite mode",
        ));
    }
    Ok(())
}

/// Commit a durable prepared commit using the retry-safe identifier path.
///
/// This is the correct operation after restoring a prepared commit or after a
/// previous commit returned an indeterminate transport/IO error. A successful
/// earlier commit with the same `(commit_user, commit_identifier)` is filtered.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_commit_prepared(
    tc: *const paimon_table_commit,
    prepared: *const paimon_prepared_commit,
) -> *mut paimon_error {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(tc, "tc") {
            return error;
        }
        if let Err(error) = check_non_null(prepared, "prepared") {
            return error;
        }
        let table_commit = &*((*tc).inner as *const TableCommitState);
        let prepared = &*((*prepared).inner as *const PreparedCommitState);
        let messages = &prepared.messages;
        if let Err(error) = validate_commit_context(
            &table_commit.table_location,
            &table_commit.commit_user,
            table_commit.overwrite,
            &messages.table_location,
            &messages.commit_user,
            messages.overwrite,
        ) {
            return error;
        }
        let result = if messages.overwrite {
            runtime().block_on(table_commit.commit.overwrite_with_identifier(
                messages.messages.clone(),
                None,
                prepared.commit_identifier,
            ))
        } else {
            runtime().block_on(table_commit.commit.filter_and_commit_with_identifier(
                messages.messages.clone(),
                prepared.commit_identifier,
            ))
        };
        match result {
            Ok(()) => ptr::null_mut(),
            Err(error) => paimon_error::from_paimon(error),
        }
    }));
    outcome.unwrap_or_else(|_| prepared_panic_error("paimon_table_commit_commit_prepared"))
}

/// Abort files referenced by a durable prepared commit.
///
/// Do not call this after an indeterminate commit response: retry
/// `paimon_table_commit_commit_prepared` first so a successful commit is not
/// followed by deletion of its files. The caller must also fence/serialize all
/// commit and abort operations for the same `(table, commit_user)` across
/// processes. If retained snapshot history cannot prove that abort is safe,
/// this function fails closed and deletes nothing.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_abort_prepared(
    tc: *const paimon_table_commit,
    prepared: *const paimon_prepared_commit,
) -> *mut paimon_error {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(tc, "tc") {
            return error;
        }
        if let Err(error) = check_non_null(prepared, "prepared") {
            return error;
        }
        let table_commit = &*((*tc).inner as *const TableCommitState);
        let prepared = &*((*prepared).inner as *const PreparedCommitState);
        let messages = &prepared.messages;
        if let Err(error) = validate_commit_context(
            &table_commit.table_location,
            &table_commit.commit_user,
            table_commit.overwrite,
            &messages.table_location,
            &messages.commit_user,
            messages.overwrite,
        ) {
            return error;
        }
        match runtime().block_on(
            table_commit
                .commit
                .abort_if_uncommitted(&messages.messages, prepared.commit_identifier),
        ) {
            Ok(()) => ptr::null_mut(),
            Err(error) => paimon_error::from_paimon(error),
        }
    }));
    outcome.unwrap_or_else(|_| prepared_panic_error("paimon_table_commit_abort_prepared"))
}

unsafe fn standard_commit_with_identifier_impl(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
    filter_committed: bool,
    batch_commit: bool,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tc, "tc") {
        return error;
    }
    if let Err(error) = check_non_null(msgs, "msgs") {
        return error;
    }
    if commit_identifier < 0 || (!batch_commit && commit_identifier == i64::MAX) {
        return invalid_input(
            "streaming commit_identifier must be non-negative and less than i64::MAX",
        );
    }
    let table_commit = &*((*tc).inner as *const TableCommitState);
    let messages = &*((*msgs).inner as *const CommitMessagesState);
    if let Err(error) = validate_commit_context(
        &table_commit.table_location,
        &table_commit.commit_user,
        table_commit.overwrite,
        &messages.table_location,
        &messages.commit_user,
        messages.overwrite,
    ) {
        return error;
    }
    if messages.overwrite {
        return invalid_input(
            "standard overwrite messages must be committed with paimon_table_commit_overwrite",
        );
    }
    let messages = messages.messages.clone();
    let result = if batch_commit {
        runtime().block_on(table_commit.commit.commit(messages))
    } else if filter_committed {
        runtime().block_on(
            table_commit
                .commit
                .filter_and_commit_with_identifier(messages, commit_identifier),
        )
    } else {
        runtime().block_on(
            table_commit
                .commit
                .commit_with_identifier(messages, commit_identifier),
        )
    };
    match result {
        Ok(()) => ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Commit standard append messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_commit(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    standard_commit_with_identifier_impl(tc, msgs, i64::MAX, false, true)
}

/// Commit standard append messages with an identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_commit_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    standard_commit_with_identifier_impl(tc, msgs, commit_identifier, false, false)
}

/// Filter a committed identifier before committing standard append messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_filter_and_commit_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    standard_commit_with_identifier_impl(tc, msgs, commit_identifier, true, false)
}

/// Commit standard overwrite messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_overwrite(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    standard_overwrite_impl(tc, msgs, None)
}

/// Commit standard overwrite messages with an identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_overwrite_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    standard_overwrite_impl(tc, msgs, Some(commit_identifier))
}

unsafe fn standard_overwrite_impl(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: Option<i64>,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tc, "tc") {
        return error;
    }
    if let Err(error) = check_non_null(msgs, "msgs") {
        return error;
    }
    if commit_identifier.is_some_and(|identifier| identifier < 0 || identifier == i64::MAX) {
        return invalid_input(
            "streaming commit_identifier must be non-negative and less than i64::MAX",
        );
    }
    let table_commit = &*((*tc).inner as *const TableCommitState);
    let messages = &*((*msgs).inner as *const CommitMessagesState);
    if let Err(error) = validate_commit_context(
        &table_commit.table_location,
        &table_commit.commit_user,
        table_commit.overwrite,
        &messages.table_location,
        &messages.commit_user,
        messages.overwrite,
    ) {
        return error;
    }
    if !messages.overwrite {
        return invalid_input(
            "append messages cannot be committed with paimon_table_commit_overwrite",
        );
    }
    let messages = messages.messages.clone();
    let result = match commit_identifier {
        Some(commit_identifier) => runtime().block_on(
            table_commit
                .commit
                .overwrite_with_identifier(messages, None, commit_identifier),
        ),
        None => runtime().block_on(table_commit.commit.overwrite(messages, None)),
    };
    match result {
        Ok(()) => ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Truncate a table with a standard TableCommit.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_truncate_table(
    tc: *const paimon_table_commit,
) -> *mut paimon_error {
    paimon_table_commit_truncate_table_impl(tc, None)
}

/// Truncate a table with a stable identifier.
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
    if let Err(error) = check_non_null(tc, "tc") {
        return error;
    }
    if commit_identifier.is_some_and(|identifier| identifier < 0 || identifier == i64::MAX) {
        return invalid_input(
            "streaming commit_identifier must be non-negative and less than i64::MAX",
        );
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
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Abort standard commit messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_abort(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tc, "tc") {
        return error;
    }
    if let Err(error) = check_non_null(msgs, "msgs") {
        return error;
    }
    let table_commit = &*((*tc).inner as *const TableCommitState);
    let messages = &*((*msgs).inner as *const CommitMessagesState);
    if let Err(error) = validate_commit_context(
        &table_commit.table_location,
        &table_commit.commit_user,
        table_commit.overwrite,
        &messages.table_location,
        &messages.commit_user,
        messages.overwrite,
    ) {
        return error;
    }
    match runtime().block_on(table_commit.commit.abort(&messages.messages)) {
        Ok(()) => ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

unsafe fn fixed_commit_with_identifier_impl(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
    commit_identifier: i64,
    filter_committed: bool,
    batch_commit: bool,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tc, "tc") {
        return error;
    }
    if let Err(error) = check_non_null(msgs, "msgs") {
        return error;
    }
    if commit_identifier < 0 || (!batch_commit && commit_identifier == i64::MAX) {
        return invalid_input(
            "streaming commit_identifier must be non-negative and less than i64::MAX",
        );
    }
    let table_commit = &*((*tc).inner as *const PostponeFixedBucketTableCommitState);
    let messages = &*((*msgs).inner as *const PostponeFixedBucketCommitMessagesState);
    if let Err(error) = validate_commit_context(
        &table_commit.table_location,
        &table_commit.commit_user,
        table_commit.overwrite,
        &messages.table_location,
        &messages.commit_user,
        messages.overwrite,
    ) {
        return error;
    }
    let messages = messages.messages.clone();
    let result = if batch_commit {
        runtime().block_on(table_commit.commit.commit(messages))
    } else if filter_committed {
        runtime().block_on(
            table_commit
                .commit
                .filter_and_commit_with_identifier(messages, commit_identifier),
        )
    } else {
        runtime().block_on(
            table_commit
                .commit
                .commit_with_identifier(messages, commit_identifier),
        )
    };
    match result {
        Ok(()) => ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Commit postpone fixed-bucket messages using the builder's mode.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_commit(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
) -> *mut paimon_error {
    fixed_commit_with_identifier_impl(tc, msgs, i64::MAX, false, true)
}

/// Commit postpone fixed-bucket messages with an identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_commit_with_identifier(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    fixed_commit_with_identifier_impl(tc, msgs, commit_identifier, false, false)
}

/// Filter a committed identifier before committing fixed-bucket messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_filter_and_commit_with_identifier(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    fixed_commit_with_identifier_impl(tc, msgs, commit_identifier, true, false)
}

/// Truncate a table with a postpone fixed-bucket TableCommit.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_truncate_table(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
) -> *mut paimon_error {
    fixed_truncate_table_impl(tc, None)
}

/// Truncate a table with a stable identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_truncate_table_with_identifier(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    commit_identifier: i64,
) -> *mut paimon_error {
    fixed_truncate_table_impl(tc, Some(commit_identifier))
}

unsafe fn fixed_truncate_table_impl(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    commit_identifier: Option<i64>,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tc, "tc") {
        return error;
    }
    if commit_identifier.is_some_and(|identifier| identifier < 0 || identifier == i64::MAX) {
        return invalid_input(
            "streaming commit_identifier must be non-negative and less than i64::MAX",
        );
    }
    let table_commit = &*((*tc).inner as *const PostponeFixedBucketTableCommitState);
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
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Abort postpone fixed-bucket commit messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_abort(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(tc, "tc") {
        return error;
    }
    if let Err(error) = check_non_null(msgs, "msgs") {
        return error;
    }
    let table_commit = &*((*tc).inner as *const PostponeFixedBucketTableCommitState);
    let messages = &*((*msgs).inner as *const PostponeFixedBucketCommitMessagesState);
    if let Err(error) = validate_commit_context(
        &table_commit.table_location,
        &table_commit.commit_user,
        table_commit.overwrite,
        &messages.table_location,
        &messages.commit_user,
        messages.overwrite,
    ) {
        return error;
    }
    match runtime().block_on(table_commit.commit.abort(&messages.messages)) {
        Ok(()) => ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

// --- C ABI signature guards -------------------------------------------------

const _: unsafe extern "C" fn(*const paimon_table) -> paimon_result_write_builder =
    paimon_table_new_write_builder;
const _: unsafe extern "C" fn(
    *const paimon_table,
) -> paimon_result_postpone_fixed_bucket_write_builder =
    paimon_table_new_postpone_fixed_bucket_write_builder;
const _: unsafe extern "C" fn(*const paimon_table, *const c_char) -> paimon_result_write_builder =
    paimon_table_new_write_builder_with_commit_user;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const c_char,
) -> paimon_result_postpone_fixed_bucket_write_builder =
    paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user;
const _: unsafe extern "C" fn(*const paimon_write_builder) -> paimon_result_table_write =
    paimon_write_builder_new_write;
const _: unsafe extern "C" fn(
    *const paimon_postpone_fixed_bucket_write_builder,
) -> paimon_result_postpone_fixed_bucket_table_write =
    paimon_postpone_fixed_bucket_write_builder_new_write;
const _: unsafe extern "C" fn(*const paimon_write_builder) -> paimon_result_table_commit =
    paimon_write_builder_new_commit;
const _: unsafe extern "C" fn(
    *const paimon_postpone_fixed_bucket_write_builder,
) -> paimon_result_postpone_fixed_bucket_table_commit =
    paimon_postpone_fixed_bucket_write_builder_new_commit;
const _: unsafe extern "C" fn(
    *mut paimon_postpone_fixed_bucket_write_builder,
    *mut c_void,
    *mut c_void,
) -> *mut paimon_error = paimon_postpone_fixed_bucket_write_builder_with_bucket_plan;
const _: unsafe extern "C" fn(*mut paimon_table_write) -> paimon_result_prepare_commit =
    paimon_table_write_prepare_commit;
const _: unsafe extern "C" fn(
    *mut paimon_postpone_fixed_bucket_table_write,
) -> paimon_result_postpone_fixed_bucket_prepare_commit =
    paimon_postpone_fixed_bucket_table_write_prepare_commit;
const _: unsafe extern "C" fn(
    *mut paimon_commit_messages,
    *const paimon_commit_messages,
) -> *mut paimon_error = paimon_commit_messages_merge;
const _: unsafe extern "C" fn(*const paimon_commit_messages, i64) -> paimon_result_prepared_commit =
    paimon_commit_messages_prepare;
const _: unsafe extern "C" fn(*const paimon_prepared_commit) -> paimon_result_bytes =
    paimon_prepared_commit_serialize;
const _: unsafe extern "C" fn(*const u8, usize) -> paimon_result_prepared_commit =
    paimon_prepared_commit_deserialize;
const _: unsafe extern "C" fn(
    *mut paimon_prepared_commit,
    *const paimon_prepared_commit,
) -> *mut paimon_error = paimon_prepared_commit_merge;
const _: unsafe extern "C" fn(
    *mut paimon_postpone_fixed_bucket_commit_messages,
    *const paimon_postpone_fixed_bucket_commit_messages,
) -> *mut paimon_error = paimon_postpone_fixed_bucket_commit_messages_merge;
const _: unsafe extern "C" fn(
    *mut paimon_table_write,
    *mut c_void,
    *mut c_void,
) -> *mut paimon_error = paimon_table_write_write_arrow_batch;
const _: unsafe extern "C" fn(
    *mut paimon_postpone_fixed_bucket_table_write,
    *mut c_void,
    *mut c_void,
) -> *mut paimon_error = paimon_postpone_fixed_bucket_table_write_write_arrow_batch;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
) -> *mut paimon_error = paimon_table_commit_commit;
const _: unsafe extern "C" fn(
    *const paimon_postpone_fixed_bucket_table_commit,
    *mut paimon_postpone_fixed_bucket_commit_messages,
) -> *mut paimon_error = paimon_postpone_fixed_bucket_table_commit_commit;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
    i64,
) -> *mut paimon_error = paimon_table_commit_commit_with_identifier;
const _: unsafe extern "C" fn(
    *const paimon_postpone_fixed_bucket_table_commit,
    *mut paimon_postpone_fixed_bucket_commit_messages,
    i64,
) -> *mut paimon_error = paimon_postpone_fixed_bucket_table_commit_commit_with_identifier;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *mut paimon_commit_messages,
    i64,
) -> *mut paimon_error = paimon_table_commit_filter_and_commit_with_identifier;
const _: unsafe extern "C" fn(
    *const paimon_postpone_fixed_bucket_table_commit,
    *mut paimon_postpone_fixed_bucket_commit_messages,
    i64,
) -> *mut paimon_error =
    paimon_postpone_fixed_bucket_table_commit_filter_and_commit_with_identifier;
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
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *const paimon_prepared_commit,
) -> *mut paimon_error = paimon_table_commit_commit_prepared;
const _: unsafe extern "C" fn(
    *const paimon_table_commit,
    *const paimon_prepared_commit,
) -> *mut paimon_error = paimon_table_commit_abort_prepared;
const _: unsafe extern "C" fn(
    *const paimon_postpone_fixed_bucket_table_commit,
    *mut paimon_postpone_fixed_bucket_commit_messages,
) -> *mut paimon_error = paimon_postpone_fixed_bucket_table_commit_abort;

#[cfg(test)]
mod raw_message_limit_tests {
    use super::{RawPreparedCommitEnvelope, MAX_PREPARED_MESSAGES};

    #[test]
    fn raw_message_count_is_rejected_during_deserialization() {
        let messages = (0..=MAX_PREPARED_MESSAGES)
            .map(|_| "{}")
            .collect::<Vec<_>>()
            .join(",");
        let json = format!(
            r#"{{"format":"paimon-rust-prepared-commit","version":2,"commit_identifier":1,"table_location":"memory:/table","commit_user":"job","overwrite":false,"messages":[{messages}]}}"#
        );
        let error = match serde_json::from_str::<RawPreparedCommitEnvelope<'_>>(&json) {
            Ok(_) => panic!("oversized raw message list must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("more than"));
        assert!(error
            .to_string()
            .contains(&MAX_PREPARED_MESSAGES.to_string()));
    }
}
