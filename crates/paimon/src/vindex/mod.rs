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

pub mod reader;

pub const IVF_FLAT_IDENTIFIER: &str = "ivf-flat";
pub const IVF_PQ_IDENTIFIER: &str = "ivf-pq";
pub const IVF_HNSW_FLAT_IDENTIFIER: &str = "ivf-hnsw-flat";
pub const IVF_HNSW_SQ_IDENTIFIER: &str = "ivf-hnsw-sq";

pub fn is_vindex_index_type(index_type: &str) -> bool {
    matches!(
        index_type,
        IVF_FLAT_IDENTIFIER | IVF_PQ_IDENTIFIER | IVF_HNSW_FLAT_IDENTIFIER | IVF_HNSW_SQ_IDENTIFIER
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vindex_index_type_identifier_helper() {
        assert!(is_vindex_index_type(IVF_FLAT_IDENTIFIER));
        assert!(is_vindex_index_type(IVF_PQ_IDENTIFIER));
        assert!(is_vindex_index_type(IVF_HNSW_FLAT_IDENTIFIER));
        assert!(is_vindex_index_type(IVF_HNSW_SQ_IDENTIFIER));
        assert!(!is_vindex_index_type(""));
        assert!(!is_vindex_index_type("btree"));
        assert!(!is_vindex_index_type("lumina"));
        assert!(!is_vindex_index_type("IVF-FLAT"));
    }
}
