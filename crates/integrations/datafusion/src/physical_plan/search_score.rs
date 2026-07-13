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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Int64Array, RecordBatch, RecordBatchOptions, UInt32Array,
};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{internal_err, DataFusionError, Statistics};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::StreamExt;

#[derive(Clone, Copy, Debug)]
pub(crate) enum SearchScoreOutputColumn {
    Input(usize),
    Score,
}

/// Filters Paimon rows by search result row ID and optionally exposes their scores.
#[derive(Debug, Clone)]
pub(crate) struct SearchScoreExec {
    input: Arc<dyn ExecutionPlan>,
    output_schema: ArrowSchemaRef,
    row_id_index: usize,
    output_columns: Arc<[SearchScoreOutputColumn]>,
    scores: Arc<HashMap<u64, f32>>,
    plan_properties: Arc<PlanProperties>,
}

impl SearchScoreExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: ArrowSchemaRef,
        row_id_index: usize,
        output_columns: Vec<SearchScoreOutputColumn>,
        scores: Arc<HashMap<u64, f32>>,
    ) -> Self {
        let partition_count = input.output_partitioning().partition_count();
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            output_schema,
            row_id_index,
            output_columns: output_columns.into(),
            scores,
            plan_properties,
        }
    }

    fn filter_and_project(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let row_ids = batch
            .column(self.row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "_ROW_ID must be Int64 when materializing hybrid search results".to_string(),
                )
            })?;

        let mut input_indices = Vec::with_capacity(batch.num_rows());
        let mut scores = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return internal_err!("_ROW_ID cannot be null in hybrid search results");
            }
            let row_id = u64::try_from(row_ids.value(row)).map_err(|_| {
                DataFusionError::Internal(format!(
                    "negative _ROW_ID {} in hybrid search results",
                    row_ids.value(row)
                ))
            })?;
            if let Some(score) = self.scores.get(&row_id) {
                input_indices.push(row as u32);
                scores.push(*score);
            }
        }
        let matched_row_count = input_indices.len();
        let scores: ArrayRef = Arc::new(Float32Array::from(scores));
        // Raw row-range scans may conservatively return non-matching rows from a
        // selected file. The scored row IDs are the authoritative result set.
        let input_indices =
            (input_indices.len() != batch.num_rows()).then(|| UInt32Array::from(input_indices));

        let columns = self
            .output_columns
            .iter()
            .map(|column| -> DFResult<ArrayRef> {
                match column {
                    SearchScoreOutputColumn::Input(index) => match &input_indices {
                        Some(indices) => Ok(arrow_select::take::take(
                            batch.column(*index).as_ref(),
                            indices,
                            None,
                        )?),
                        None => Ok(Arc::clone(batch.column(*index))),
                    },
                    SearchScoreOutputColumn::Score => Ok(Arc::clone(&scores)),
                }
            })
            .collect::<DFResult<Vec<_>>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(matched_row_count));
        RecordBatch::try_new_with_options(self.output_schema.clone(), columns, &options)
            .map_err(DataFusionError::from)
    }
}

impl DisplayAs for SearchScoreExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SearchScoreExec")
    }
}

impl ExecutionPlan for SearchScoreExec {
    fn name(&self) -> &str {
        "SearchScoreExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("SearchScoreExec expects one child");
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            Arc::clone(&self.output_schema),
            self.row_id_index,
            self.output_columns.to_vec(),
            Arc::clone(&self.scores),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let exec = self.clone();
        let stream = input.map(move |batch| batch.and_then(|batch| exec.filter_and_project(batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            Box::pin(stream),
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.output_schema),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    #[test]
    fn test_filter_and_project_by_row_id_filters_non_matches_and_reorders_columns() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("_ROW_ID", DataType::Int64, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("__paimon_search_score", DataType::Float32, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let exec = SearchScoreExec::new(
            Arc::new(EmptyExec::new(input_schema.clone())),
            output_schema,
            1,
            vec![
                SearchScoreOutputColumn::Score,
                SearchScoreOutputColumn::Input(0),
            ],
            Arc::new(HashMap::from([(7, 0.25), (3, 0.75)])),
        );
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![70, 40, 30])),
                Arc::new(Int64Array::from(vec![7, 4, 3])),
            ],
        )
        .expect("input batch");

        let output = exec.filter_and_project(batch).expect("filter and project");
        let scores = output
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("score column");
        let ids = output
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        assert_eq!(scores.values(), &[0.25, 0.75]);
        assert_eq!(ids.values(), &[70, 30]);
    }

    #[test]
    fn test_filter_and_project_preserves_row_count_without_output_columns() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "_ROW_ID",
            DataType::Int64,
            true,
        )]));
        let output_schema = Arc::new(Schema::empty());
        let exec = SearchScoreExec::new(
            Arc::new(EmptyExec::new(input_schema.clone())),
            output_schema,
            0,
            Vec::new(),
            Arc::new(HashMap::from([(7, 0.25), (3, 0.75)])),
        );
        let batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(Int64Array::from(vec![7, 4, 3]))],
        )
        .expect("input batch");

        let output = exec.filter_and_project(batch).expect("filter and project");
        assert_eq!(output.num_columns(), 0);
        assert_eq!(output.num_rows(), 2);
    }
}
