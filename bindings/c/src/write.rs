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
use paimon::table::{PostponeBucketPlan, Table};

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{
    paimon_result_postpone_fixed_bucket_prepare_commit,
    paimon_result_postpone_fixed_bucket_table_commit,
    paimon_result_postpone_fixed_bucket_table_write,
    paimon_result_postpone_fixed_bucket_write_builder, paimon_result_prepare_commit,
    paimon_result_table_commit, paimon_result_table_write, paimon_result_write_builder,
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
    target.messages.extend(source.messages.clone());
    ptr::null_mut()
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
    target.messages.extend(source.messages.clone());
    ptr::null_mut()
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

unsafe fn standard_commit_with_identifier_impl(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
    filter_committed: bool,
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
    if messages.overwrite {
        return invalid_input(
            "standard overwrite messages must be committed with paimon_table_commit_overwrite",
        );
    }
    let messages = messages.messages.clone();
    let result = if filter_committed {
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
    paimon_table_commit_commit_with_identifier(tc, msgs, i64::MAX)
}

/// Commit standard append messages with an identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_commit_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    standard_commit_with_identifier_impl(tc, msgs, commit_identifier, false)
}

/// Filter a committed identifier before committing standard append messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_commit_filter_and_commit_with_identifier(
    tc: *const paimon_table_commit,
    msgs: *mut paimon_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    standard_commit_with_identifier_impl(tc, msgs, commit_identifier, true)
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
    let messages = messages.messages.clone();
    let result = if filter_committed {
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
    paimon_postpone_fixed_bucket_table_commit_commit_with_identifier(tc, msgs, i64::MAX)
}

/// Commit postpone fixed-bucket messages with an identifier.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_commit_with_identifier(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    fixed_commit_with_identifier_impl(tc, msgs, commit_identifier, false)
}

/// Filter a committed identifier before committing fixed-bucket messages.
#[no_mangle]
pub unsafe extern "C" fn paimon_postpone_fixed_bucket_table_commit_filter_and_commit_with_identifier(
    tc: *const paimon_postpone_fixed_bucket_table_commit,
    msgs: *mut paimon_postpone_fixed_bucket_commit_messages,
    commit_identifier: i64,
) -> *mut paimon_error {
    fixed_commit_with_identifier_impl(tc, msgs, commit_identifier, true)
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
    *const paimon_postpone_fixed_bucket_table_commit,
    *mut paimon_postpone_fixed_bucket_commit_messages,
) -> *mut paimon_error = paimon_postpone_fixed_bucket_table_commit_abort;
