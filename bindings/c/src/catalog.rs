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
use std::sync::Arc;

use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogFactory, Options};

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{paimon_result_catalog_new, paimon_result_get_table, paimon_result_get_tag};
use crate::runtime;
use crate::types::{paimon_bytes, paimon_catalog, paimon_identifier, paimon_option, paimon_table};

/// Create a catalog using CatalogFactory with the given options.
///
/// # Safety
/// `options` must be a valid pointer to an array of `paimon_option` with `options_len` elements.
/// Each key and value in the options must be valid null-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_create(
    options: *const paimon_option,
    options_len: usize,
) -> paimon_result_catalog_new {
    // Build Options from the array
    let mut opts = Options::new();
    if !options.is_null() && options_len > 0 {
        let options_slice = std::slice::from_raw_parts(options, options_len);
        for opt in options_slice {
            let key = match validate_cstr(opt.key, "option key") {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_catalog_new {
                        catalog: std::ptr::null_mut(),
                        error: e,
                    }
                }
            };
            let value = match validate_cstr(opt.value, "option value") {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_catalog_new {
                        catalog: std::ptr::null_mut(),
                        error: e,
                    }
                }
            };
            opts.set(key, value);
        }
    }

    // Create catalog using CatalogFactory
    match runtime().block_on(CatalogFactory::create(opts)) {
        Ok(catalog) => {
            let wrapper = Box::new(paimon_catalog {
                inner: Box::into_raw(Box::new(catalog)) as *mut c_void,
            });
            paimon_result_catalog_new {
                catalog: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_catalog_new {
            catalog: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Free a paimon_catalog.
///
/// # Safety
/// Only call with a catalog returned from `paimon_catalog_create`.
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_free(catalog: *mut paimon_catalog) {
    if !catalog.is_null() {
        let c = Box::from_raw(catalog);
        if !c.inner.is_null() {
            drop(Box::from_raw(c.inner as *mut Arc<dyn Catalog>));
        }
    }
}

/// Get a table from the catalog.
///
/// # Safety
/// `catalog` and `identifier` must be valid pointers from previous paimon C calls, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_get_table(
    catalog: *const paimon_catalog,
    identifier: *const crate::types::paimon_identifier,
) -> paimon_result_get_table {
    if let Err(e) = check_non_null(catalog, "catalog") {
        return paimon_result_get_table {
            table: std::ptr::null_mut(),
            error: e,
        };
    }
    if let Err(e) = check_non_null(identifier, "identifier") {
        return paimon_result_get_table {
            table: std::ptr::null_mut(),
            error: e,
        };
    }

    let catalog_ref = &*((*catalog).inner as *const Arc<dyn Catalog>);
    let identifier_ref = &*((*identifier).inner as *const Identifier);

    match runtime().block_on(catalog_ref.get_table(identifier_ref)) {
        Ok(table) => {
            let wrapper = Box::new(paimon_table {
                inner: Box::into_raw(Box::new(table)) as *mut c_void,
            });
            paimon_result_get_table {
                table: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_get_table {
            table: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Create a tag for a snapshot, or the latest snapshot when `snapshot_id` is null.
///
/// # Safety
/// `catalog` and `identifier` must be valid pointers from previous paimon C calls.
/// `tag_name` must be a valid null-terminated C string.
/// If non-null, `snapshot_id` must point to a valid `i64`.
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_create_tag(
    catalog: *const paimon_catalog,
    identifier: *const paimon_identifier,
    tag_name: *const c_char,
    snapshot_id: *const i64,
    ignore_if_exists: bool,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(catalog, "catalog") {
        return error;
    }
    if let Err(error) = check_non_null(identifier, "identifier") {
        return error;
    }
    let tag_name = match validate_cstr(tag_name, "tag_name") {
        Ok(tag_name) => tag_name,
        Err(error) => return error,
    };

    let catalog = &*((*catalog).inner as *const Arc<dyn Catalog>);
    let identifier = &*((*identifier).inner as *const Identifier);
    let snapshot_id = snapshot_id.as_ref().copied();
    match runtime().block_on(catalog.create_tag(
        identifier,
        &tag_name,
        snapshot_id,
        ignore_if_exists,
    )) {
        Ok(()) => std::ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Get a tag and its snapshot metadata as JSON.
///
/// # Safety
/// `catalog` and `identifier` must be valid pointers from previous paimon C calls.
/// `tag_name` must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_get_tag(
    catalog: *const paimon_catalog,
    identifier: *const paimon_identifier,
    tag_name: *const c_char,
) -> paimon_result_get_tag {
    if let Err(error) = check_non_null(catalog, "catalog") {
        return paimon_result_get_tag {
            tag: paimon_bytes::empty(),
            error,
        };
    }
    if let Err(error) = check_non_null(identifier, "identifier") {
        return paimon_result_get_tag {
            tag: paimon_bytes::empty(),
            error,
        };
    }
    let tag_name = match validate_cstr(tag_name, "tag_name") {
        Ok(tag_name) => tag_name,
        Err(error) => {
            return paimon_result_get_tag {
                tag: paimon_bytes::empty(),
                error,
            }
        }
    };

    let catalog = &*((*catalog).inner as *const Arc<dyn Catalog>);
    let identifier = &*((*identifier).inner as *const Identifier);
    match runtime().block_on(catalog.get_tag(identifier, &tag_name)) {
        Ok(tag) => match serde_json::to_vec(&tag) {
            Ok(tag) => paimon_result_get_tag {
                tag: paimon_bytes::new(tag),
                error: std::ptr::null_mut(),
            },
            Err(error) => paimon_result_get_tag {
                tag: paimon_bytes::empty(),
                error: paimon_error::new(
                    PaimonErrorCode::Unexpected,
                    format!("failed to serialize tag metadata: {error}"),
                ),
            },
        },
        Err(error) => paimon_result_get_tag {
            tag: paimon_bytes::empty(),
            error: paimon_error::from_paimon(error),
        },
    }
}

/// Delete a tag.
///
/// # Safety
/// `catalog` and `identifier` must be valid pointers from previous paimon C calls.
/// `tag_name` must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_delete_tag(
    catalog: *const paimon_catalog,
    identifier: *const paimon_identifier,
    tag_name: *const c_char,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(catalog, "catalog") {
        return error;
    }
    if let Err(error) = check_non_null(identifier, "identifier") {
        return error;
    }
    let tag_name = match validate_cstr(tag_name, "tag_name") {
        Ok(tag_name) => tag_name,
        Err(error) => return error,
    };

    let catalog = &*((*catalog).inner as *const Arc<dyn Catalog>);
    let identifier = &*((*identifier).inner as *const Identifier);
    match runtime().block_on(catalog.delete_tag(identifier, &tag_name, false)) {
        Ok(()) => std::ptr::null_mut(),
        Err(error) => paimon_error::from_paimon(error),
    }
}
