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

pub(crate) const BTREE_GLOBAL_INDEX_TYPE: &str = "btree";
pub(crate) const BITMAP_GLOBAL_INDEX_TYPE: &str = "bitmap";

pub(crate) fn normalize_sorted_global_index_type(index_type: &str) -> Option<&'static str> {
    if index_type.eq_ignore_ascii_case(BTREE_GLOBAL_INDEX_TYPE) {
        Some(BTREE_GLOBAL_INDEX_TYPE)
    } else if index_type.eq_ignore_ascii_case(BITMAP_GLOBAL_INDEX_TYPE) {
        Some(BITMAP_GLOBAL_INDEX_TYPE)
    } else {
        None
    }
}
