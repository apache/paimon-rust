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

//! REST catalog implementation for Apache Paimon.
//!
//! This module provides a REST-based catalog that communicates with
//! a Paimon REST catalog server for metadata operations.

mod rest_catalog;
mod rest_token;
mod rest_token_file_io;

pub use rest_catalog::*;
pub use rest_token::*;
pub use rest_token_file_io::*;
