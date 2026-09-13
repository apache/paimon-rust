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

use super::*;
use crate::spec::{ArrayType, FloatType};

#[test]
fn vindex_array_dimension_accepts_diskann_search_options() {
    let field = DataField::new(
        1,
        "embedding".to_string(),
        DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
    );
    let query_options = HashMap::from([
        ("diskann.dimension".to_string(), "8".to_string()),
        ("diskann.l_search".to_string(), "64".to_string()),
        (
            "vindex.reader.memory-budget-bytes".to_string(),
            "1048576".to_string(),
        ),
    ]);

    assert_eq!(
        pk_vector_query_dimension(&HashMap::new(), &query_options, "diskann", &field).unwrap(),
        Some(8)
    );
}
