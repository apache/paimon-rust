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

//! Current-state audit reads over audit-planned or externally assigned splits.

use std::ffi::c_void;

use arrow_array::ffi::FFI_ArrowSchema;
use paimon::table::{AuditLogRead, AuditLogTable, Table, TableRead};
use paimon::Plan;

use crate::error::{check_non_null, paimon_error, PaimonErrorCode};
use crate::result::{paimon_result_plan, paimon_result_record_batch_reader};
use crate::runtime;
use crate::types::*;

/// Export the full current-state audit schema, including rowkind and the
/// optional configured sequence number, through the Arrow C Data Interface.
/// The output remains valid after the table is freed. Release it using its
/// ArrowSchema release callback. Errors leave the output unchanged.
///
/// # Safety
/// `table` must be a live table or null (returns error). `schema` must point to
/// initialized, writable, empty ArrowSchema storage or be null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_export_audit_arrow_schema(
    table: *const paimon_table,
    schema: *mut c_void,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(table, "table") {
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
    let table = &*((*table).inner as *const Table);
    let fields = match AuditLogTable::new(table.clone()).fields() {
        Ok(fields) => fields,
        Err(error) => return paimon_error::from_paimon(error),
    };
    let exported = paimon::arrow::build_read_arrow_schema(&fields).and_then(|schema| {
        FFI_ArrowSchema::try_from(schema.as_ref()).map_err(|error| paimon::Error::DataInvalid {
            message: format!("Failed to export audit schema: {error}"),
            source: Some(Box::new(error)),
        })
    });
    match exported {
        Ok(schema) => {
            std::ptr::write(output, schema);
            std::ptr::null_mut()
        }
        Err(error) => paimon_error::from_paimon(error),
    }
}

/// Plan a current-state audit scan using an ordinary ReadBuilder's table view,
/// user-column projection, case sensitivity and filters. Core audit planning
/// preserves retracts and complete merge groups; its LIMIT pruning is disabled.
/// Free the result with `paimon_plan_free`.
///
/// # Safety
/// `builder` must be a live ReadBuilder or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_plan_audit(
    builder: *const paimon_read_builder,
) -> paimon_result_plan {
    if let Err(error) = check_non_null(builder, "builder") {
        return paimon_result_plan {
            plan: std::ptr::null_mut(),
            error,
        };
    }
    let state = &*((*builder).inner as *const ReadBuilderState);
    let planned = (|| {
        let mut read_builder = state.table.new_read_builder();
        read_builder.with_case_sensitive(state.case_sensitive);
        if let Some(columns) = &state.projected_columns {
            let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
            read_builder.with_projection(&columns)?;
        }
        if let Some(filter) = &state.filter {
            read_builder.with_filter(filter.clone());
        }
        runtime().block_on(read_builder.new_audit_scan().plan())
    })();
    match planned {
        Ok(plan) => paimon_result_plan {
            plan: Box::into_raw(Box::new(paimon_plan {
                inner: Box::into_raw(Box::new(plan)).cast(),
            })),
            error: std::ptr::null_mut(),
        },
        Err(error) => paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::from_paimon(error),
        },
    }
}

/// Read current-state audit rows from the selected logical splits. `plan` can
/// come from audit planning or `paimon_plan_from_split_bytes`; external callers
/// must preserve audit merge groups, including first-row/DV overlap boundaries.
/// This entry point does not interpret incremental or changelog plans.
///
/// The ordinary read selects user columns. Output prepends rowkind and the
/// table-configured optional sequence column; an empty user projection reads
/// only those system columns. Enforce final system-column filters and LIMIT in
/// the caller. Offset/length select complete splits and saturate as for ordinary
/// reads. The stream owns its inputs and outlives the read and plan handles.
///
/// # Safety
/// `read` and `plan` must be live handles or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_to_audit_log_arrow(
    read: *const paimon_table_read,
    plan: *const paimon_plan,
    offset: usize,
    length: usize,
) -> paimon_result_record_batch_reader {
    if let Err(error) = check_non_null(read, "read") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error,
        };
    }
    if let Err(error) = check_non_null(plan, "plan") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error,
        };
    }
    let state = &*((*read).inner as *const TableReadState);
    let plan = &*((*plan).inner as *const Plan);
    let start = offset.min(plan.splits().len());
    let end = offset.saturating_add(length).min(plan.splits().len());
    let stream = (|| {
        let all_fields = AuditLogTable::new(state.table.clone()).fields()?;
        let system_count = all_fields.len() - state.table.schema().fields().len();
        let mut fields = all_fields[..system_count].to_vec();
        for field in &state.read_type {
            if !fields.iter().any(|selected| selected.id() == field.id()) {
                fields.push(field.clone());
            }
        }
        let mut table_read = TableRead::new(&state.table, fields, state.data_predicates.clone());
        if let Some(resources) = &state.resources {
            table_read = table_read.with_resources(resources.clone());
        }
        AuditLogRead::new(table_read)?.to_arrow(&plan.splits()[start..end])
    })();
    match stream {
        Ok(stream) => paimon_result_record_batch_reader {
            reader: Box::into_raw(Box::new(paimon_record_batch_reader {
                inner: Box::into_raw(Box::new(stream)).cast(),
            })),
            error: std::ptr::null_mut(),
        },
        Err(error) => paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::from_paimon(error),
        },
    }
}
