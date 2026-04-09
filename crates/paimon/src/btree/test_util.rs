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

//! Test utilities for btree module.

use crate::io::{FileRead, FileWrite};
use bytes::Bytes;
use std::sync::{Arc, Mutex};

/// In-memory FileWrite for tests. Collects all written bytes.
#[derive(Clone)]
pub struct VecFileWrite {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl VecFileWrite {
    pub fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match Arc::try_unwrap(self.buf) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.buf.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl FileWrite for VecFileWrite {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        self.buf.lock().unwrap().extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> crate::Result<()> {
        Ok(())
    }
}

/// In-memory FileRead backed by Bytes. For tests.
pub struct BytesFileRead(pub Bytes);

#[async_trait::async_trait]
impl FileRead for BytesFileRead {
    async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
        Ok(self.0.slice(range.start as usize..range.end as usize))
    }
}
