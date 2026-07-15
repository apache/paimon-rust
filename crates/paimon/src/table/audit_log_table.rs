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

use super::incremental_scan::{IncrementalPlan, IncrementalScan, IncrementalScanMode};
use super::{ArrowRecordBatchStream, Table};
use crate::spec::{
    BigIntType, DataField, DataType, VarCharType, ROW_KIND_FIELD_ID, ROW_KIND_FIELD_NAME,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME,
};

/// Wrapper that exposes table rows with a leading `rowkind` audit column.
///
/// Incremental reads produce:
/// - Delta: primary-key rows use physical `_VALUE_KIND`; append rows are `+I`
/// - Changelog: kinds come from physical `_VALUE_KIND` (`+I`/`-U`/`+U`/`-D`)
/// - Diff: not implemented in this release
#[derive(Debug, Clone)]
pub struct AuditLogTable {
    wrapped: Table,
}

const TABLE_READ_SEQUENCE_NUMBER_ENABLED: &str = "table-read.sequence-number.enabled";

impl AuditLogTable {
    pub fn new(wrapped: Table) -> Self {
        Self { wrapped }
    }

    pub fn wrapped(&self) -> &Table {
        &self.wrapped
    }

    /// Logical fields: `rowkind` (+ optional `_SEQUENCE_NUMBER`) then table fields.
    pub fn fields(&self) -> crate::Result<Vec<DataField>> {
        let mut fields = Vec::with_capacity(self.wrapped.schema().fields().len() + 2);
        fields.push(DataField::new(
            ROW_KIND_FIELD_ID,
            ROW_KIND_FIELD_NAME.to_string(),
            DataType::VarChar(VarCharType::string_type()),
        ));
        if self.sequence_number_enabled() {
            fields.push(DataField::new(
                SEQUENCE_NUMBER_FIELD_ID,
                SEQUENCE_NUMBER_FIELD_NAME.to_string(),
                DataType::BigInt(BigIntType::new()),
            ));
        }
        fields.extend(self.wrapped.schema().fields().iter().cloned());
        Ok(fields)
    }

    fn sequence_number_enabled(&self) -> bool {
        self.wrapped
            .schema()
            .options()
            .get(TABLE_READ_SEQUENCE_NUMBER_ENABLED)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    pub fn new_incremental_scan(
        &self,
        mode: IncrementalScanMode,
        start_exclusive: i64,
        end_inclusive: i64,
    ) -> IncrementalScan<'_> {
        IncrementalScan::for_table(&self.wrapped, mode, start_exclusive, end_inclusive)
    }

    pub fn to_arrow(&self, plan: &IncrementalPlan) -> crate::Result<ArrowRecordBatchStream> {
        let read = self.wrapped.new_read_builder().new_read()?;
        read.to_audit_log_arrow(plan)
    }
}
