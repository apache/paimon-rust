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

#[derive(Debug, Snafu)]
pub enum InvalidBinaryType {
    #[snafu(
        visibility(pub(crate)),
        display("Binary string length must be at least 1.")
    )]
    LengthTooSmall,
}

#[derive(Debug, Snafu)]
pub enum InvalidCharType {
    #[snafu(
        visibility(pub(crate)),
        display("Character string length must be between 1 and 255 (both inclusive).")
    )]
    LengthOutOfRange,
}

#[derive(Debug, Snafu)]
pub enum InvalidDecimalType {
    #[snafu(
        visibility(pub(crate)),
        display(
            "Decimal precision must be between {} and {} (both inclusive).",
            min,
            max
        )
    )]
    PrecisionOutOfRange { min: u32, max: u32 },

    #[snafu(
        visibility(pub(crate)),
        display("Decimal scale must be between {} and {} (both inclusive).", min, max)
    )]
    ScaleOutOfRange { min: u32, max: u32 },
}

#[derive(Debug, Snafu)]
pub enum InvalidLocalZonedTimestampType {
    #[snafu(
        visibility(pub(crate)),
        display(
            "Local zoned timestamp precision must be between {} and {} (both inclusive).",
            min,
            max
        )
    )]
    LocalZonedTimestampPrecisionOutOfRange { min: u32, max: u32 },
}

#[derive(Debug, Snafu)]
pub enum InvalidTimeType {
    #[snafu(
        visibility(pub(crate)),
        display("Time precision must be between {} and {} (both inclusive).", min, max)
    )]
    TimePrecisionOutOfRange { min: u32, max: u32 },
}

#[derive(Debug, Snafu)]
pub enum InvalidTimestampType {
    #[snafu(
        visibility(pub(crate)),
        display(
            "Timestamp precision must be between {} and {} (both inclusive).",
            min,
            max
        )
    )]
    TimestampPrecisionOutOfRange { min: u32, max: u32 },
}

#[derive(Debug, Snafu)]
pub enum InvalidVarBinaryType {
    #[snafu(
        visibility(pub(crate)),
        display("VarBinary string length must be at least 1.")
    )]
    VarBinaryLengthTooSmall,
}

#[derive(Debug, Snafu)]
pub enum InvalidVarCharType {
    #[snafu(
        visibility(pub(crate)),
        display(
            "Character string length must be between {} and {} (both inclusive).",
            min,
            max
        )
    )]
    VarCharLengthOutOfRange { min: u32, max: u32 },
}
