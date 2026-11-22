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

use std::collections::VecDeque;
use std::mem::size_of;

use datafusion::arrow::array::ArrayRef;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::Accumulator;

use super::PercentileAccumulator;

/// GK summaries have no inverse operation. Like Spark's moving aggregate
/// frames, retain the current frame and rebuild its summary when it changes.
/// This accumulator is only used for sliding windows; ordinary aggregation
/// and growing windows retain the compact summary instead of input rows.
#[derive(Debug)]
pub(super) struct SlidingPercentileAccumulator {
    template: PercentileAccumulator,
    values: VecDeque<ScalarValue>,
    value_bytes: usize,
    evaluated: Option<PercentileAccumulator>,
}

impl SlidingPercentileAccumulator {
    pub(super) fn new(template: PercentileAccumulator) -> Self {
        Self {
            template,
            values: VecDeque::new(),
            value_bytes: 0,
            evaluated: None,
        }
    }

    fn current(&mut self) -> Result<&mut PercentileAccumulator> {
        if self.evaluated.is_none() {
            let mut accumulator = self.template.empty_like();
            for value in &self.values {
                accumulator.insert_scalar(value.clone())?;
            }
            self.evaluated = Some(accumulator);
        }
        Ok(self.evaluated.as_mut().expect("initialized above"))
    }
}

impl Accumulator for SlidingPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for row in 0..values[0].len() {
            let value = ScalarValue::try_from_array(&values[0], row)?;
            self.value_bytes += value.size();
            self.values.push_back(value);
        }
        if !values[0].is_empty() {
            self.evaluated = None;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let length = values[0].len();
        if length > self.values.len() {
            return Err(DataFusionError::Internal(
                "Sliding percentile cannot retract more rows than its current frame".into(),
            ));
        }
        // DataFusion supplies the filtered outgoing rows in frame order. Keep
        // NULL rows too so entering and exiting row counts remain aligned.
        for _ in 0..length {
            if let Some(value) = self.values.pop_front() {
                self.value_bytes -= value.size();
            }
        }
        if length != 0 {
            self.evaluated = None;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.current()?.evaluate()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.current()?.state()
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        // Window execution updates/retracts the original frame and never merges
        // partial summaries. A compressed summary cannot reconstruct that frame.
        Err(DataFusionError::Internal(
            "Sliding percentile windows cannot merge partial aggregate states".into(),
        ))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.template.size()
            + self.value_bytes
            + (self.values.capacity() - self.values.len()) * size_of::<ScalarValue>()
            + self.evaluated.as_ref().map_or(0, Accumulator::size)
    }
}
