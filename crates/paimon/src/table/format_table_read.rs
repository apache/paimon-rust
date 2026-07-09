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

//! Read implementation for Java-compatible `type=format-table` metadata.

use super::data_file_reader::DataFileReader;
use super::{ArrowRecordBatchStream, Table};
use crate::spec::{CoreOptions, DataField, Predicate};
use crate::DataSplit;

#[derive(Debug, Clone)]
pub(crate) struct FormatTableRead<'a> {
    table: &'a Table,
    read_type: Vec<DataField>,
    data_predicates: Vec<Predicate>,
}

impl<'a> FormatTableRead<'a> {
    pub(crate) fn new(
        table: &'a Table,
        read_type: Vec<DataField>,
        data_predicates: Vec<Predicate>,
    ) -> Self {
        Self {
            table,
            read_type,
            data_predicates,
        }
    }

    pub(crate) fn read_type(&self) -> &[DataField] {
        &self.read_type
    }

    pub(crate) fn data_predicates(&self) -> &[Predicate] {
        &self.data_predicates
    }

    pub(crate) fn table(&self) -> &Table {
        self.table
    }

    pub(crate) fn with_filter(mut self, filter: Predicate) -> Self {
        self.data_predicates = filter.split_and();
        self
    }

    pub(crate) fn to_arrow(
        &self,
        data_splits: &[DataSplit],
    ) -> crate::Result<ArrowRecordBatchStream> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            self.read_type.clone(),
            self.data_predicates.clone(),
        )
        .read(data_splits)
    }
}
