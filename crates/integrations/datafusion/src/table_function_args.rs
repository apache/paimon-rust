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

use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use paimon::catalog::{Identifier, SYSTEM_TABLE_SPLITTER};
use paimon::table::IncrementalScanMode;

/// Parsed target of `paimon_incremental_query`: base table plus optional `$audit_log`.
#[derive(Debug, Clone)]
pub(crate) struct IncrementalTableRef {
    pub identifier: Identifier,
    pub audit_log: bool,
}

/// Parse table name for incremental query TVF.
///
/// Accepts `table`, `database.table`, or `catalog.database.table`, with optional
/// `$audit_log` suffix on the object name.
pub(crate) fn parse_incremental_table_ref(
    function_name: &str,
    name: &str,
    default_database: &str,
) -> DFResult<IncrementalTableRef> {
    let parts: Vec<&str> = name.split('.').collect();
    let (database, object_name) = match parts.len() {
        1 => (default_database, parts[0]),
        2 => (parts[0], parts[1]),
        3 => (parts[1], parts[2]),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{function_name}: invalid table name '{name}', expected 'table', 'database.table', or 'catalog.database.table'"
            )));
        }
    };

    let mut object_parts = object_name.splitn(2, SYSTEM_TABLE_SPLITTER);
    let base = object_parts.next().unwrap_or(object_name);
    let suffix = object_parts.next();
    let audit_log = match suffix {
        None => false,
        Some("audit_log") => true,
        Some(other) => {
            return Err(DataFusionError::Plan(format!(
                "{function_name}: unsupported system table suffix '${other}' in '{name}'"
            )));
        }
    };

    Ok(IncrementalTableRef {
        identifier: Identifier::new(database.to_string(), base.to_string()),
        audit_log,
    })
}

/// Accept integer literals or decimal strings (Spark `'0'`, `'1'` compatibility).
pub(crate) fn extract_snapshot_bound_literal(
    function_name: &str,
    expr: &Expr,
    name: &str,
) -> DFResult<i64> {
    match expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int8(Some(v)) => Ok(*v as i64),
            ScalarValue::Int16(Some(v)) => Ok(*v as i64),
            ScalarValue::Int32(Some(v)) => Ok(*v as i64),
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
                DataFusionError::Plan(format!(
                    "{function_name}: {name} value {v} exceeds i64 range"
                ))
            }),
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                s.parse::<i64>().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "{function_name}: {name} must be an integer snapshot id, got string '{s}'"
                    ))
                })
            }
            _ => Err(DataFusionError::Plan(format!(
                "{function_name}: {name} must be an integer or numeric string literal, got: {expr}"
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "{function_name}: {name} must be a literal, got: {expr}"
        ))),
    }
}

/// Parse optional TVF 4th argument; aligns with Java `incremental-between-scan-mode`.
pub(crate) fn parse_incremental_scan_mode(
    function_name: &str,
    expr: &Expr,
) -> DFResult<IncrementalScanMode> {
    let mode = extract_string_literal(function_name, expr, "scan_mode")?;
    match mode.to_ascii_lowercase().as_str() {
        "auto" => Ok(IncrementalScanMode::Auto),
        "delta" => Ok(IncrementalScanMode::Delta),
        "changelog" => Ok(IncrementalScanMode::Changelog),
        "diff" => Ok(IncrementalScanMode::Diff),
        other => Err(DataFusionError::Plan(format!(
            "{function_name}: unsupported scan_mode '{other}', expected auto, delta, changelog, or diff"
        ))),
    }
}

pub(crate) fn extract_string_literal(
    function_name: &str,
    expr: &Expr,
    name: &str,
) -> DFResult<String> {
    match expr {
        Expr::Literal(scalar, _) => {
            let s = scalar.try_as_str().flatten().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{function_name}: {name} must be a string literal, got: {expr}"
                ))
            })?;
            Ok(s.to_string())
        }
        _ => Err(DataFusionError::Plan(format!(
            "{function_name}: {name} must be a literal, got: {expr}"
        ))),
    }
}

pub(crate) fn extract_int_literal(function_name: &str, expr: &Expr, name: &str) -> DFResult<i64> {
    match expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int8(Some(v)) => Ok(*v as i64),
            ScalarValue::Int16(Some(v)) => Ok(*v as i64),
            ScalarValue::Int32(Some(v)) => Ok(*v as i64),
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
                DataFusionError::Plan(format!(
                    "{function_name}: {name} value {v} exceeds i64 range"
                ))
            }),
            _ => Err(DataFusionError::Plan(format!(
                "{function_name}: {name} must be an integer literal, got: {expr}"
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "{function_name}: {name} must be a literal, got: {expr}"
        ))),
    }
}

pub(crate) fn parse_table_identifier(
    function_name: &str,
    name: &str,
    default_database: &str,
) -> DFResult<Identifier> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        1 => Ok(Identifier::new(default_database, parts[0])),
        2 => Ok(Identifier::new(parts[0], parts[1])),
        3 => Ok(Identifier::new(parts[1], parts[2])),
        _ => Err(DataFusionError::Plan(format!(
            "{function_name}: invalid table name '{name}', expected 'table', 'database.table', or 'catalog.database.table'"
        ))),
    }
}

#[cfg(test)]
mod incremental_query_args_tests {
    use super::*;

    #[test]
    fn parse_incremental_table_ref_plain_name() {
        let parsed =
            parse_incremental_table_ref("paimon_incremental_query", "orders", "default").unwrap();
        assert_eq!(parsed.identifier.database(), "default");
        assert_eq!(parsed.identifier.object(), "orders");
        assert!(!parsed.audit_log);
    }

    #[test]
    fn parse_incremental_table_ref_with_audit_log_suffix() {
        let parsed = parse_incremental_table_ref(
            "paimon_incremental_query",
            "paimon.test_db.orders$audit_log",
            "default",
        )
        .unwrap();
        assert_eq!(parsed.identifier.database(), "test_db");
        assert_eq!(parsed.identifier.object(), "orders");
        assert!(parsed.audit_log);
    }

    #[test]
    fn extract_snapshot_bound_accepts_int_and_numeric_string() {
        let int_expr = Expr::Literal(ScalarValue::Int64(Some(2)), None);
        let str_expr = Expr::Literal(ScalarValue::Utf8(Some("2".to_string())), None);
        assert_eq!(
            extract_snapshot_bound_literal("paimon_incremental_query", &int_expr, "end").unwrap(),
            2
        );
        assert_eq!(
            extract_snapshot_bound_literal("paimon_incremental_query", &str_expr, "end").unwrap(),
            2
        );
    }

    #[test]
    fn parse_incremental_table_ref_rejects_unknown_system_suffix() {
        let err =
            parse_incremental_table_ref("paimon_incremental_query", "orders$snapshots", "default")
                .unwrap_err()
                .to_string();
        assert!(err.contains("snapshots"), "unexpected error: {err}");
    }

    #[test]
    fn parse_incremental_scan_mode_accepts_aliases() {
        let lit = |s: &str| Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
        assert_eq!(
            parse_incremental_scan_mode("paimon_incremental_query", &lit("auto")).unwrap(),
            IncrementalScanMode::Auto
        );
        assert_eq!(
            parse_incremental_scan_mode("paimon_incremental_query", &lit("DIFF")).unwrap(),
            IncrementalScanMode::Diff
        );
        assert_eq!(
            parse_incremental_scan_mode("paimon_incremental_query", &lit("changelog")).unwrap(),
            IncrementalScanMode::Changelog
        );
    }

    #[test]
    fn parse_incremental_scan_mode_rejects_unknown() {
        let lit = Expr::Literal(ScalarValue::Utf8(Some("nope".to_string())), None);
        let err = parse_incremental_scan_mode("paimon_incremental_query", &lit)
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope"), "unexpected error: {err}");
    }
}
