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

use std::sync::Arc;

use crate::{io::InputFile, options::Options, spec::DataType};

use super::{load_factory, FileIndexReader, FileIndexWriter};

pub trait FileIndexer {
    fn create_writer(&self) -> Arc<dyn FileIndexWriter>;
    fn create_reader(
        &self,
        input_file: InputFile,
        start: usize,
        length: usize,
    ) -> Box<dyn FileIndexReader>;
}

pub async fn create_file_indexer(
    typ: &str,
    data_type: DataType,
    options: Options,
) -> crate::Result<Box<dyn FileIndexer>> {
    let factory = load_factory(typ)?;
    factory.create(data_type, options)
}
