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

use snafu::prelude::*;

/// Result type used in paimon.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for paimon.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Paimon data invalid for {}: {:?}", message, source))]
    DataInvalid {
        message: String,
        #[snafu(backtrace)]
        source: snafu::Whatever,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected error {}: {:?}", message, source)
    )]
    IoUnexpected {
        message: String,
        source: opendal::Error,
    },
}
