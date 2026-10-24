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

//! Mirrors Java [ConsumersTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/ConsumersTable.java).

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use crate::table::Table;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::Result;

pub(super) fn consumers_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("consumer_id", DataType::Utf8, false),
                Field::new("next_snapshot_id", DataType::Int64, false),
            ]))
        })
        .clone()
}

pub(super) async fn read(
    table: &Table,
    _projection: Option<&[usize]>,
    predicate: Option<&crate::spec::Predicate>,
) -> Result<RecordBatch> {
    let manager = table.consumer_manager();
    let consumers = match predicate.and_then(consumer_ids) {
        Some(ids) => {
            manager
                .list_by_ids(&ids.into_iter().collect::<Vec<_>>())
                .await?
        }
        None => manager.list_all().await?,
    };

    let schema = consumers_schema();
    let batch = super::record_batch(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                consumers.iter().map(|(id, _)| id.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                consumers.iter().map(|(_, next_snapshot)| *next_snapshot),
            )),
        ],
    )?;

    Ok(batch)
}

fn consumer_ids(predicate: &crate::spec::Predicate) -> Option<HashSet<String>> {
    use crate::spec::{Datum, Predicate, PredicateOperator};
    match predicate {
        Predicate::Leaf {
            column,
            op,
            literals,
            ..
        } if column == "consumer_id"
            && matches!(op, PredicateOperator::Eq | PredicateOperator::In) =>
        {
            literals
                .iter()
                .map(|value| {
                    if let Datum::String(value) = value {
                        Some(value.clone())
                    } else {
                        None
                    }
                })
                .collect()
        }
        Predicate::And(children) => children.iter().filter_map(consumer_ids).reduce(|mut a, b| {
            a.retain(|id| b.contains(id));
            a
        }),
        Predicate::Or(children) => children
            .iter()
            .map(consumer_ids)
            .collect::<Option<Vec<_>>>()
            .map(|sets| sets.into_iter().flatten().collect()),
        _ => None,
    }
}
