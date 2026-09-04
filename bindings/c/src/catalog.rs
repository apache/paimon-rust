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
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use paimon::catalog::Identifier;
use paimon::spec::Schema;
use paimon::{Catalog, CatalogFactory, Options};

use crate::error::{check_non_null, paimon_error, validate_cstr};
use crate::result::{paimon_result_catalog_new, paimon_result_get_table};
use crate::runtime;
use crate::types::{paimon_catalog, paimon_option, paimon_table};

fn catalog_panic_error(operation: &str) -> *mut paimon_error {
    paimon_error::new(
        crate::error::PaimonErrorCode::Unexpected,
        format!("Rust panic while executing {operation}"),
    )
}

fn validate_creation_schema_json(schema_json: &str) -> Result<Schema, *mut paimon_error> {
    let parsed = serde_json::from_str::<Schema>(schema_json).map_err(|error| {
        paimon_error::new(
            crate::error::PaimonErrorCode::InvalidInput,
            format!("Failed to parse creation schema JSON: {error}"),
        )
    })?;

    let mut builder = Schema::builder();
    for field in parsed.fields() {
        builder = builder.column_with_description(
            field.name(),
            field.data_type().clone(),
            field.description().map(str::to_string),
        );
    }
    builder
        .partition_keys(parsed.partition_keys().iter().cloned())
        .primary_key(parsed.primary_keys().iter().cloned())
        .options(parsed.options().clone())
        .comment(parsed.comment().map(str::to_string))
        .build()
        .map_err(paimon_error::from_paimon)
}

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

/// Create a table from a logical Paimon `Schema` JSON document.
///
/// The input is normalized and validated through `SchemaBuilder` before it is
/// sent to the catalog. Field IDs in the JSON are therefore treated as input
/// ordering hints and reassigned canonically from zero.
///
/// # Safety
/// `catalog` and `identifier` must be valid Paimon handles. `schema_json` must
/// point to a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_create_table_from_schema_json(
    catalog: *const paimon_catalog,
    identifier: *const crate::types::paimon_identifier,
    schema_json: *const c_char,
    ignore_if_exists: bool,
) -> *mut paimon_error {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(catalog, "catalog") {
            return error;
        }
        if let Err(error) = check_non_null(identifier, "identifier") {
            return error;
        }
        let schema_json = match validate_cstr(schema_json, "schema_json") {
            Ok(value) => value,
            Err(error) => return error,
        };
        let schema = match validate_creation_schema_json(&schema_json) {
            Ok(value) => value,
            Err(error) => return error,
        };
        let catalog_ref = &*((*catalog).inner as *const Arc<dyn Catalog>);
        let identifier_ref = &*((*identifier).inner as *const Identifier);
        match runtime().block_on(catalog_ref.create_table(identifier_ref, schema, ignore_if_exists))
        {
            Ok(()) => std::ptr::null_mut(),
            Err(error) => paimon_error::from_paimon(error),
        }
    }));
    outcome.unwrap_or_else(|_| catalog_panic_error("paimon_catalog_create_table_from_schema_json"))
}

/// Drop a table from the catalog.
///
/// # Safety
/// `catalog` and `identifier` must be valid Paimon handles, or null (returns an
/// error).
#[no_mangle]
pub unsafe extern "C" fn paimon_catalog_drop_table(
    catalog: *const paimon_catalog,
    identifier: *const crate::types::paimon_identifier,
    ignore_if_not_exists: bool,
) -> *mut paimon_error {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if let Err(error) = check_non_null(catalog, "catalog") {
            return error;
        }
        if let Err(error) = check_non_null(identifier, "identifier") {
            return error;
        }
        let catalog_ref = &*((*catalog).inner as *const Arc<dyn Catalog>);
        let identifier_ref = &*((*identifier).inner as *const Identifier);
        match runtime().block_on(catalog_ref.drop_table(identifier_ref, ignore_if_not_exists)) {
            Ok(()) => std::ptr::null_mut(),
            Err(error) => paimon_error::from_paimon(error),
        }
    }));
    outcome.unwrap_or_else(|_| catalog_panic_error("paimon_catalog_drop_table"))
}

const _: unsafe extern "C" fn(
    *const paimon_catalog,
    *const crate::types::paimon_identifier,
    *const c_char,
    bool,
) -> *mut paimon_error = paimon_catalog_create_table_from_schema_json;
const _: unsafe extern "C" fn(
    *const paimon_catalog,
    *const crate::types::paimon_identifier,
    bool,
) -> *mut paimon_error = paimon_catalog_drop_table;
