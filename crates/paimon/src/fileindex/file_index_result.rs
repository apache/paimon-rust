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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileIndexResult {
    Remain,
    Skip,
}

impl FileIndexResult {
    pub fn remain(&self) -> bool {
        matches!(self, FileIndexResult::Remain)
    }

    pub fn and(&self, other: FileIndexResult) -> FileIndexResult {
        if self.remain() && other.remain() {
            FileIndexResult::Remain
        } else {
            FileIndexResult::Skip
        }
    }

    pub fn or(&self, other: FileIndexResult) -> FileIndexResult {
        if self.remain() || other.remain() {
            FileIndexResult::Remain
        } else {
            FileIndexResult::Skip
        }
    }
}

lazy_static::lazy_static! {
    pub static ref REMAIN: Arc<FileIndexResult> = Arc::new(FileIndexResult::Remain);
    pub static ref SKIP: Arc<FileIndexResult> = Arc::new(FileIndexResult::Skip);
}
