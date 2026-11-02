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

//! Build-option, field-type, and row-range validation.

#[cfg(test)]
use crate::spec::DataField;
pub(super) use crate::table::index_build::fields::*;
#[cfg(test)]
use crate::table::Table;
#[cfg(test)]
use crate::{Error, Result};

#[cfg(test)]
pub(super) fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == column)
        .ok_or_else(|| Error::ColumnNotExist {
            full_name: table.identifier().full_name(),
            column: column.to_string(),
        })
}

pub(super) fn ranges_overlap(
    left_start: i64,
    left_end: i64,
    right_start: i64,
    right_end: i64,
) -> bool {
    left_start <= right_end && right_start <= left_end
}
