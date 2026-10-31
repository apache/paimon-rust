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

use crate::spec::DataField;
use crate::table::global_index_build_common::vector::{
    find_index_field as find_vector_index_field,
    validate_vector_field as validate_common_vector_field,
};
use crate::table::Table;
use crate::Result;

pub(super) fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    find_vector_index_field(table, column)
}

pub(super) fn validate_vector_field(field: &DataField) -> Result<()> {
    validate_common_vector_field(field, "vindex")
}

pub(super) use crate::table::index_build::vector_validation::*;
