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

use super::{
    evaluate_merge_actions, MergeActionOutput, MergeAssignment, MergeCondition, UpdateAssignment,
    WhenMatched, WhenNotMatched,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn target_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("other", DataType::Int32, true),
    ]))
}

fn joined(ids: Vec<i64>, source: Vec<Option<i64>>) -> RecordBatch {
    let values = ids.iter().map(|id| *id as i32 * 10).collect::<Vec<_>>();
    let other = ids.iter().map(|id| *id as i32 * 100).collect::<Vec<_>>();
    RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(ids)) as ArrayRef),
        ("t.value", Arc::new(Int32Array::from(values)) as ArrayRef),
        ("t.other", Arc::new(Int32Array::from(other)) as ArrayRef),
        ("s.value", Arc::new(Int64Array::from(source)) as ArrayRef),
    ])
    .unwrap()
}

fn source(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_from_iter([("s.value", Arc::new(Int64Array::from(values)) as ArrayRef)])
        .unwrap()
}

fn greater_than(bound: i64) -> MergeCondition {
    MergeCondition {
        target_columns: Vec::new(),
        source_columns: vec!["value".into()],
        evaluate: Arc::new(move |batch| {
            Box::pin(async move {
                let values = batch
                    .column_by_name("s.value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                Ok(BooleanArray::from(
                    values
                        .iter()
                        .map(|value| value.map(|value| value > bound))
                        .collect::<Vec<_>>(),
                ))
            })
        }),
    }
}

fn int_values(batches: &[RecordBatch], name: &str) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn row_ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("_ROW_ID")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn prejoined_matched_actions_preserve_clause_order_null_fallthrough_and_target_values() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let assignment = UpdateAssignment::Function(Arc::new(move |batches| {
        observed.fetch_add(1, Ordering::Relaxed);
        assert_eq!(row_ids(batches), vec![1, 2]);
        // The function returns different chunk boundaries from the selected input.
        Ok(vec![Arc::new(Int64Array::from(vec![-1, -2])) as ArrayRef])
    }));
    let actions = evaluate_merge_actions(
        target_schema(),
        vec![
            joined(vec![1, 3], vec![None, Some(15)]),
            joined(vec![2, 4, 5], vec![Some(5), Some(25), Some(35)]),
        ],
        Vec::new(),
        &[
            WhenMatched {
                condition: Some(greater_than(20)),
                delete: true,
                assignments: Vec::new(),
            },
            WhenMatched {
                condition: Some(greater_than(10)),
                delete: false,
                assignments: vec![(
                    "value".into(),
                    MergeAssignment::SourceColumn("value".into()),
                )],
            },
            WhenMatched {
                condition: None,
                delete: false,
                assignments: vec![("other".into(), MergeAssignment::Value(assignment))],
            },
        ],
        &[],
        MergeActionOutput::All,
    )
    .await
    .unwrap();
    assert_eq!(actions.update_columns, vec!["value", "other"]);
    assert_eq!(row_ids(&actions.updates), vec![3, 1, 2]);
    assert_eq!(
        int_values(&actions.updates, "value"),
        vec![Some(15), Some(10), Some(20)]
    );
    assert_eq!(
        int_values(&actions.updates, "other"),
        vec![Some(300), Some(-1), Some(-2)]
    );
    assert_eq!(actions.deletes, vec![4, 5]);
    assert!(actions.inserts.is_empty());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn prejoined_insert_actions_fill_typed_nulls_and_select_first_match() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let literal = |value| {
        vec![(
            "value".into(),
            MergeAssignment::Value(UpdateAssignment::Scalar(Arc::new(Int64Array::from(vec![
                value,
            ])))),
        )]
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let fallback = UpdateAssignment::Function(Arc::new(move |batches| {
        observed.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1, 1]
        );
        Ok(batches
            .iter()
            .map(|batch| batch.column_by_name("s.value").unwrap().clone())
            .collect())
    }));
    let actions = evaluate_merge_actions(
        schema.clone(),
        Vec::new(),
        vec![
            source(vec![None, Some(15)]),
            source(vec![Some(5), Some(25)]),
        ],
        &[],
        &[
            WhenNotMatched {
                condition: Some(greater_than(20)),
                assignments: literal(20),
            },
            WhenNotMatched {
                condition: Some(greater_than(10)),
                assignments: literal(10),
            },
            WhenNotMatched {
                condition: None,
                assignments: vec![("value".into(), MergeAssignment::Value(fallback))],
            },
        ],
        MergeActionOutput::Inserts,
    )
    .await
    .unwrap();
    assert_eq!(
        int_values(&actions.inserts, "value"),
        vec![Some(20), Some(10), None, Some(5)]
    );
    for batch in &actions.inserts {
        assert_eq!(batch.schema(), schema);
        let labels = batch
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(labels.null_count(), batch.num_rows());
    }
    assert!(actions.updates.is_empty());
    assert!(actions.deletes.is_empty());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn prejoined_delete_output_consumes_updates_without_evaluating_values() {
    let clauses = [
        WhenMatched {
            condition: Some(greater_than(10)),
            delete: false,
            assignments: vec![(
                "value".into(),
                MergeAssignment::Value(UpdateAssignment::Function(Arc::new(|_| {
                    panic!("delete output evaluated UPDATE")
                }))),
            )],
        },
        WhenMatched {
            condition: None,
            delete: true,
            assignments: Vec::new(),
        },
    ];
    // A delete-only caller needs no target output fields.
    let actions = evaluate_merge_actions(
        Arc::new(Schema::empty()),
        vec![joined(vec![1, 2], vec![None, Some(15)])],
        Vec::new(),
        &clauses,
        &[],
        MergeActionOutput::Deletes,
    )
    .await
    .unwrap();
    assert_eq!(actions.deletes, vec![1]);
    assert!(actions.updates.is_empty());
}

#[tokio::test]
async fn prejoined_updates_consume_deletes_and_keep_empty_selections_lazy() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let clauses = [
        WhenMatched {
            condition: Some(greater_than(10)),
            delete: true,
            assignments: Vec::new(),
        },
        WhenMatched {
            condition: None,
            delete: false,
            assignments: vec![(
                "value".into(),
                MergeAssignment::Value(UpdateAssignment::Function(Arc::new(move |batches| {
                    observed.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(row_ids(batches), vec![1]);
                    Ok(vec![Arc::new(Int32Array::from(vec![7])) as ArrayRef])
                }))),
            )],
        },
    ];
    let actions = evaluate_merge_actions(
        target_schema(),
        vec![joined(vec![1, 2], vec![None, Some(15)])],
        Vec::new(),
        &clauses,
        &[],
        MergeActionOutput::Updates,
    )
    .await
    .unwrap();
    assert_eq!(row_ids(&actions.updates), vec![1]);
    assert_eq!(int_values(&actions.updates, "value"), vec![Some(7)]);
    assert!(actions.deletes.is_empty());
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    evaluate_merge_actions(
        target_schema(),
        vec![joined(Vec::new(), Vec::new())],
        Vec::new(),
        &clauses,
        &[],
        MergeActionOutput::Updates,
    )
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn prejoined_assignment_cast_errors_and_bad_condition_lengths_are_rejected() {
    let error = evaluate_merge_actions(
        target_schema(),
        vec![joined(vec![1], vec![Some(15)])],
        Vec::new(),
        &[WhenMatched {
            condition: None,
            delete: false,
            assignments: vec![(
                "value".into(),
                MergeAssignment::Value(UpdateAssignment::Scalar(Arc::new(StringArray::from(
                    vec!["invalid int"],
                )))),
            )],
        }],
        &[],
        MergeActionOutput::Updates,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("invalid int"), "{error}");

    let condition = MergeCondition {
        target_columns: Vec::new(),
        source_columns: Vec::new(),
        evaluate: Arc::new(|_| Box::pin(async { Ok(BooleanArray::from(vec![true, false])) })),
    };
    let error = evaluate_merge_actions(
        target_schema(),
        vec![joined(vec![1], vec![Some(15)])],
        Vec::new(),
        &[WhenMatched {
            condition: Some(condition),
            delete: true,
            assignments: Vec::new(),
        }],
        &[],
        MergeActionOutput::Deletes,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("condition length"), "{error}");
}

#[tokio::test]
async fn prejoined_matched_row_ids_are_validated_before_clause_callbacks() {
    let missing = joined(vec![1], vec![Some(15)]).project(&[1, 2, 3]).unwrap();
    let wrong_type =
        RecordBatch::try_from_iter([("_ROW_ID", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let null_id = RecordBatch::try_from_iter([(
        "_ROW_ID",
        Arc::new(Int64Array::from(vec![None])) as ArrayRef,
    )])
    .unwrap();
    let clauses = [WhenMatched {
        condition: Some(MergeCondition {
            target_columns: Vec::new(),
            source_columns: Vec::new(),
            evaluate: Arc::new(|_| panic!("invalid row IDs reached clause evaluation")),
        }),
        delete: true,
        assignments: Vec::new(),
    }];
    for batch in [missing, wrong_type, null_id] {
        let error = evaluate_merge_actions(
            target_schema(),
            vec![batch],
            Vec::new(),
            &clauses,
            &[],
            MergeActionOutput::Deletes,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("_ROW_ID"), "{error}");
    }
}
