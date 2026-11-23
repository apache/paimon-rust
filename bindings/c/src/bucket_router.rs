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

use paimon::spec::TableSchema;
use paimon::table::{BucketRouter, PostponeBucketPlan};

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{paimon_result_bucket_router, paimon_result_next_batch};
use crate::types::{paimon_arrow_batch, paimon_bucket_router};
use crate::write::import_record_batch;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, StructArray};

fn router_error(error: *mut paimon_error) -> paimon_result_bucket_router {
    paimon_result_bucket_router {
        router: ptr::null_mut(),
        error,
    }
}

struct RouterState {
    router: BucketRouter,
    schema: TableSchema,
}

/// Create a bucket router directly from a Paimon TableSchema JSON.
///
/// Does not open a catalog, touch storage, or allocate file writers. Supports
/// fixed-bucket (`default`, `mod`, `hive`) and postpone-bucket schemas.
/// For postpone buckets with per-partition counts, pass the same plan to
/// `paimon_bucket_router_with_bucket_plan` and
/// `paimon_postpone_fixed_bucket_write_builder_with_bucket_plan`. After shuffling,
/// call `paimon_postpone_fixed_bucket_table_write_write_arrow_batch`; that writer
/// routes rows using the shared plan.
///
/// # Safety
/// `table_schema_json` must be a valid null-terminated UTF-8 C string, or null
/// (returns an error). Free the returned handle with `paimon_bucket_router_free`.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_router_new_from_schema_json(
    table_schema_json: *const c_char,
) -> paimon_result_bucket_router {
    let json = match validate_cstr(table_schema_json, "table_schema_json") {
        Ok(value) => value,
        Err(error) => return router_error(error),
    };
    let schema = match serde_json::from_str::<TableSchema>(&json) {
        Ok(schema) => schema,
        Err(error) => {
            return router_error(paimon_error::new(
                PaimonErrorCode::InvalidInput,
                format!("Failed to parse table schema JSON: {error}"),
            ));
        }
    };
    match BucketRouter::new(&schema) {
        Ok(router) => {
            let inner = Box::into_raw(Box::new(RouterState { router, schema })).cast();
            paimon_result_bucket_router {
                router: Box::into_raw(Box::new(paimon_bucket_router { inner })),
                error: ptr::null_mut(),
            }
        }
        Err(error) => router_error(paimon_error::from_paimon(error)),
    }
}

/// Replace the routing plan of a postpone-bucket router, including its default.
/// The Arrow batch uses the writer's plan format: partition columns followed
/// by an Int32 `total_buckets` column. An error leaves the router unchanged.
/// Once Arrow import starts this call consumes both Arrow structs.
///
/// # Safety
/// The caller must have exclusive access to a live router for this call.
/// `array` and `schema` must point to initialized Arrow C Data structs.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_router_with_bucket_plan(
    router: *mut paimon_bucket_router,
    array: *mut c_void,
    schema: *mut c_void,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(router, "router") {
        return error;
    }
    if let Err(error) = check_non_null((*router).inner, "router.inner") {
        return error;
    }
    if let Err(error) = check_non_null(array, "array") {
        return error;
    }
    if let Err(error) = check_non_null(schema, "schema") {
        return error;
    }
    let batch = match import_record_batch(array, schema) {
        Ok(batch) => batch,
        Err(error) => return error,
    };
    let state = &mut *(*router).inner.cast::<RouterState>();
    let result = PostponeBucketPlan::from_arrow(&state.schema, &batch)
        .and_then(|plan| state.router.clone().with_bucket_plan(plan));
    match result {
        Ok(router) => {
            state.router = router;
            ptr::null_mut()
        }
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Compute the Paimon bucket ID for every row of an Arrow record batch.
///
/// `out_buckets` must point to caller-allocated storage for `out_len` `int32_t`
/// elements, where `out_len` must equal the batch row count. When `out_len == 0`,
/// `out_buckets` may be null.
///
/// For fixed-bucket routers, the input batch may contain only the bucket-key
/// columns (plus any unrelated columns in any order). Ownership of `array` and
/// `schema` is transferred once Arrow import starts.
///
/// This function is thread-safe and may be called concurrently on the same
/// `router` handle from multiple threads.
///
/// # Safety
/// `router` must be a live bucket router handle. `array` and `schema` must
/// point to valid Arrow C Data Interface structs. `out_buckets` must be valid
/// for writes of `out_len` `i32` values when `out_len > 0`.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_router_route_arrow(
    router: *const paimon_bucket_router,
    array: *mut c_void,
    schema: *mut c_void,
    out_buckets: *mut i32,
    out_len: usize,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(router, "router") {
        return error;
    }
    if let Err(error) = check_non_null((*router).inner, "router.inner") {
        return error;
    }
    if let Err(error) = check_non_null(array, "array") {
        return error;
    }
    if let Err(error) = check_non_null(schema, "schema") {
        return error;
    }
    if out_len > 0 && out_buckets.is_null() {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "out_buckets must not be null when out_len > 0".into(),
        );
    }

    let batch = match import_record_batch(array, schema) {
        Ok(batch) => batch,
        Err(error) => return error,
    };
    if batch.num_rows() != out_len {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!(
                "out_len ({out_len}) does not match Arrow batch row count ({})",
                batch.num_rows()
            ),
        );
    }

    let state = &*(*router).inner.cast::<RouterState>();
    let buckets = match state.router.route(&batch) {
        Ok(buckets) => buckets,
        Err(error) => return paimon_error::from_paimon(error),
    };
    if out_len > 0 {
        std::slice::from_raw_parts_mut(out_buckets, out_len).copy_from_slice(buckets.values());
    }
    ptr::null_mut()
}

/// Export required routing fields into an initialized empty ArrowSchema.
/// The result has independent ownership and must be released by its callback.
/// Shared read-only calls are allowed. Plan mutation and free require exclusive
/// access to the router, including exclusion of in-flight route/schema calls.
///
/// # Safety
/// router must be live and schema must point to writable initialized empty
/// ArrowSchema storage. Null pointers return an error.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_router_export_arrow_schema(
    router: *const paimon_bucket_router,
    schema: *mut c_void,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(router, "router") {
        return error;
    }
    if let Err(error) = check_non_null((*router).inner, "router.inner") {
        return error;
    }
    if let Err(error) = check_non_null(schema, "schema") {
        return error;
    }
    let output = schema.cast::<FFI_ArrowSchema>();
    if (*output).release.is_some() {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "ArrowSchema output must be empty".into(),
        );
    }
    let state = &*(*router).inner.cast::<RouterState>();
    let schema = match state.router.routing_schema() {
        Ok(schema) => schema,
        Err(error) => return paimon_error::from_paimon(error),
    };
    match FFI_ArrowSchema::try_from(schema.as_ref()) {
        Ok(schema) => {
            ptr::write(output, schema);
            ptr::null_mut()
        }
        Err(error) => paimon_error::new(PaimonErrorCode::InvalidInput, error.to_string()),
    }
}

/// Return a row-aligned Arrow batch with partition_key (LargeBinary) and
/// bucket_id (Int32), using fixed-bucket routing. Release the output containers
/// with paimon_arrow_batch_free, including after moving their Arrow contents.
/// Output can outlive the router. Empty input returns an owned empty batch.
///
/// Once pointer validation succeeds, both input Arrow structs are consumed,
/// including on routing/import errors. Before import, validation errors leave
/// input ownership with the caller. No input references survive this call.
/// Calls may share a router concurrently with independent input/output storage.
/// Plan mutation and free require exclusive access. Temporary allocations and
/// output scale with batch rows and key sizes and use the normal Rust allocator.
///
/// # Safety
/// router must be live. array and schema must point to valid initialized Arrow
/// C Data structs with a Struct root. Null pointers return an error.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_router_route_with_partitions_arrow(
    router: *const paimon_bucket_router,
    array: *mut c_void,
    schema: *mut c_void,
) -> paimon_result_next_batch {
    let route = || -> Result<paimon_arrow_batch, *mut paimon_error> {
        check_non_null(router, "router")?;
        check_non_null((*router).inner, "router.inner")?;
        check_non_null(array, "array")?;
        check_non_null(schema, "schema")?;
        let batch = import_record_batch(array, schema)?;
        let state = &*(*router).inner.cast::<RouterState>();
        let output = state
            .router
            .route_with_partitions(&batch)
            .map_err(paimon_error::from_paimon)?;
        let ffi_schema = FFI_ArrowSchema::try_from(output.schema().as_ref())
            .map_err(|error| paimon_error::new(PaimonErrorCode::InvalidInput, error.to_string()))?;
        let ffi_array = FFI_ArrowArray::new(&StructArray::from(output).to_data());
        Ok(paimon_arrow_batch {
            array: Box::into_raw(Box::new(ffi_array)).cast(),
            schema: Box::into_raw(Box::new(ffi_schema)).cast(),
        })
    };
    match route() {
        Ok(batch) => paimon_result_next_batch {
            batch,
            error: ptr::null_mut(),
        },
        Err(error) => paimon_result_next_batch {
            batch: paimon_arrow_batch {
                array: ptr::null_mut(),
                schema: ptr::null_mut(),
            },
            error,
        },
    }
}

/// Free a bucket router handle.
///
/// # Safety
/// Only call with a handle returned from `paimon_bucket_router_new_from_schema_json`
/// once, or pass null. Exclude all in-flight router calls before freeing.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_router_free(router: *mut paimon_bucket_router) {
    if !router.is_null() {
        let wrapper = Box::from_raw(router);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner.cast::<RouterState>()));
        }
    }
}

// --- C ABI signature guards -------------------------------------------------

const _: unsafe extern "C" fn(*const c_char) -> paimon_result_bucket_router =
    paimon_bucket_router_new_from_schema_json;
const _: unsafe extern "C" fn(
    *mut paimon_bucket_router,
    *mut c_void,
    *mut c_void,
) -> *mut paimon_error = paimon_bucket_router_with_bucket_plan;
const _: unsafe extern "C" fn(
    *const paimon_bucket_router,
    *mut c_void,
    *mut c_void,
    *mut i32,
    usize,
) -> *mut paimon_error = paimon_bucket_router_route_arrow;
const _: unsafe extern "C" fn(*mut paimon_bucket_router) = paimon_bucket_router_free;

const _: unsafe extern "C" fn(*const paimon_bucket_router, *mut c_void) -> *mut paimon_error =
    paimon_bucket_router_export_arrow_schema;
const _: unsafe extern "C" fn(
    *const paimon_bucket_router,
    *mut c_void,
    *mut c_void,
) -> paimon_result_next_batch = paimon_bucket_router_route_with_partitions_arrow;
