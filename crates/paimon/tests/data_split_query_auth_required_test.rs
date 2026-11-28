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

use paimon::spec::BinaryRow;
use paimon::table::{DataSplit, DataSplitBuilder};

fn empty_split() -> DataSplit {
    DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::from_bytes(
            0,
            vec![0u8; BinaryRow::cal_fix_part_size_in_bytes(0) as usize],
        ))
        .with_bucket(0)
        .with_bucket_path("p".to_string())
        .with_total_buckets(1)
        .with_data_files(vec![])
        .with_raw_convertible(false)
        .build()
        .unwrap()
}

/// Build a transported split: serialize a fixture, set the
/// `query_auth_required` and `query_auth_response` fields, and deserialize.
fn transport(split: &DataSplit, required: bool, response: serde_json::Value) -> DataSplit {
    let mut json = serde_json::to_value(split).unwrap();
    json["query_auth_required"] = serde_json::json!(required);
    json["query_auth_response"] = response;
    serde_json::from_value(json).unwrap()
}

/// Issue 1463: `has_query_auth_rules()` reports row filters or column masks.
/// An unrestricted response can still require authorization, so a custom
/// reader must consult `query_auth_required()` on the transported split.
#[test]
fn transported_unrestricted_response_still_requires_authorization() {
    let split = empty_split();
    assert!(!split.query_auth_required());
    assert!(!split.has_query_auth_rules());

    let unrestricted = transport(&split, true, serde_json::json!({}));
    assert!(unrestricted.query_auth_required());
    assert!(!unrestricted.has_query_auth_rules());

    let blank = transport(
        &split,
        true,
        serde_json::json!({"filter":[],"columnMasking":{}}),
    );
    assert!(blank.query_auth_required());
    assert!(!blank.has_query_auth_rules());

    // The flag is independent of whether a response is present.
    let no_response = transport(&split, true, serde_json::Value::Null);
    assert!(no_response.query_auth_required());
    assert!(!no_response.has_query_auth_rules());

    let filtered = transport(&split, true, serde_json::json!({"filter":["id > 0"]}));
    assert!(filtered.query_auth_required());
    assert!(filtered.has_query_auth_rules());

    let masked = transport(
        &split,
        true,
        serde_json::json!({"columnMasking":{"ssn":"m0"}}),
    );
    assert!(masked.query_auth_required());
    assert!(masked.has_query_auth_rules());
}
