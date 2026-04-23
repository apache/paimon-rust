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

use super::cursor::AvroCursor;
use super::schema::WriterSchema;

/// Trait for types that can be decoded directly from Avro binary data.
pub trait AvroRecordDecode: Sized {
    fn decode(cursor: &mut AvroCursor, writer_schema: &WriterSchema) -> crate::Result<Self>;
}

/// Safely negate a negative Avro block count to usize.
/// Avro uses negative counts to indicate that a block-size-in-bytes follows.
#[inline]
pub(crate) fn neg_count_to_usize(count: i64) -> crate::Result<usize> {
    count
        .checked_neg()
        .map(|v| v as usize)
        .ok_or_else(|| crate::Error::UnexpectedError {
            message: format!("avro decode: block count overflow: {count}"),
            source: None,
        })
}
