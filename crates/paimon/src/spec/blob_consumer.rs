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

use super::BlobDescriptor;
use crate::Result;

/// Receive the physical payload range of each written BLOB, as Java's
/// `BlobConsumer` does. A NULL field reports `None`; NULL elements inside an
/// ARRAY or MAP do not invoke the consumer.
///
/// Returning `true` requests an output flush after the complete record is
/// written. Descriptors refer to uncommitted files and do not publish a table
/// snapshot. A failed callback fails the write and is never retried.
/// Writers retain their BLOB files on automatic cleanup whenever a consumer
/// is installed, since an exposed descriptor may already be used elsewhere.
pub trait BlobConsumer: Send + Sync {
    fn accept(&self, field_name: &str, descriptor: Option<&BlobDescriptor>) -> Result<bool>;
}

impl<F> BlobConsumer for F
where
    F: Fn(&str, Option<&BlobDescriptor>) -> Result<bool> + Send + Sync,
{
    fn accept(&self, field_name: &str, descriptor: Option<&BlobDescriptor>) -> Result<bool> {
        self(field_name, descriptor)
    }
}
