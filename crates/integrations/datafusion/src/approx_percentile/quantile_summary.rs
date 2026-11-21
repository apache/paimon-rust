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

//! Greenwald-Khanna summaries following Spark's `QuantileSummaries`.
//!
//! Keep the buffered insertion, compression thresholds and rank selection in sync
//! with Spark, including its ordinary IEEE comparisons inside sorted summaries.
//! Reference: Apache Spark v4.0.0, sql/catalyst/.../util/QuantileSummaries.scala.

use std::cmp::Ordering;
use std::mem::size_of;

use datafusion::common::{DataFusionError, Result};

const HEAD_SIZE: usize = 50_000;
const COMPRESS_THRESHOLD: usize = 10_000;
const MAGIC: &[u8; 4] = b"PGK1";
const HEADER_SIZE: usize = 24;
const SAMPLE_SIZE: usize = 24;

#[derive(Debug, Clone, Copy)]
struct Sample {
    value: f64,
    g: u64,
    delta: u64,
}

#[derive(Debug, Clone)]
pub(super) struct QuantileSummary {
    accuracy: u32,
    /// Count already inserted into `samples`, excluding `head`.
    count: u64,
    samples: Vec<Sample>,
    head: Vec<f64>,
    compressed: bool,
}

impl QuantileSummary {
    pub(super) fn new(accuracy: u32) -> Self {
        Self {
            accuracy,
            count: 0,
            samples: Vec::new(),
            head: Vec::new(),
            compressed: true,
        }
    }

    fn relative_error(&self) -> f64 {
        1.0 / f64::from(self.accuracy)
    }

    pub(super) fn count(&self) -> u64 {
        self.count.saturating_add(self.head.len() as u64)
    }

    pub(super) fn size(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.samples.capacity().saturating_mul(size_of::<Sample>()))
            .saturating_add(self.head.capacity().saturating_mul(size_of::<f64>()))
    }

    pub(super) fn insert(&mut self, value: f64) {
        self.head.push(value);
        self.compressed = false;
        if self.head.len() >= HEAD_SIZE {
            self.insert_head();
            if self.samples.len() >= COMPRESS_THRESHOLD {
                self.compress();
            }
        }
    }

    fn insert_head(&mut self) {
        if self.head.is_empty() {
            return;
        }
        let mut head = std::mem::take(&mut self.head);
        head.sort_by(double_order);
        let samples = std::mem::take(&mut self.samples);
        let mut merged = Vec::with_capacity(samples.len() + head.len());
        let mut sample_index = 0;
        let relative_error = self.relative_error();
        for (index, &value) in head.iter().enumerate() {
            // Spark deliberately uses ordinary <= here, not Double.compare.
            while sample_index < samples.len() && samples[sample_index].value <= value {
                merged.push(samples[sample_index]);
                sample_index += 1;
            }
            self.count = self.count.saturating_add(1);
            let delta = if merged.is_empty()
                || (sample_index == samples.len() && index + 1 == head.len())
            {
                0
            } else {
                (2.0 * relative_error * self.count as f64).floor() as u64
            };
            merged.push(Sample { value, g: 1, delta });
        }
        merged.extend_from_slice(&samples[sample_index..]);
        self.samples = merged;
    }

    pub(super) fn compress(&mut self) {
        if self.compressed {
            return;
        }
        self.insert_head();
        self.samples = compress_samples(
            &self.samples,
            2.0 * self.relative_error() * self.count as f64,
        );
        self.compressed = true;
    }

    pub(super) fn merge(&mut self, other: &Self) {
        self.compress();
        let mut other = other.clone();
        other.compress();
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other;
            return;
        }

        let additional_self_delta =
            (2.0 * other.relative_error() * other.count as f64).floor() as u64;
        let additional_other_delta =
            (2.0 * self.relative_error() * self.count as f64).floor() as u64;
        let mut merged = Vec::with_capacity(self.samples.len() + other.samples.len());
        let (mut self_index, mut other_index) = (0, 0);
        while self_index < self.samples.len() && other_index < other.samples.len() {
            let (mut sample, additional_delta) =
                if self.samples[self_index].value < other.samples[other_index].value {
                    let sample = self.samples[self_index];
                    self_index += 1;
                    (
                        sample,
                        if other_index > 0 {
                            additional_self_delta
                        } else {
                            0
                        },
                    )
                } else {
                    let sample = other.samples[other_index];
                    other_index += 1;
                    (
                        sample,
                        if self_index > 0 {
                            additional_other_delta
                        } else {
                            0
                        },
                    )
                };
            sample.delta = sample.delta.saturating_add(additional_delta);
            merged.push(sample);
        }
        merged.extend_from_slice(&self.samples[self_index..]);
        merged.extend_from_slice(&other.samples[other_index..]);
        self.count = self.count.saturating_add(other.count);
        self.accuracy = self.accuracy.min(other.accuracy);
        self.samples = compress_samples(&merged, 2.0 * self.relative_error() * self.count as f64);
        self.compressed = true;
    }

    pub(super) fn query(&mut self, percentiles: &[f64]) -> Vec<f64> {
        self.compress();
        if self.samples.is_empty() || percentiles.is_empty() {
            return Vec::new();
        }
        // Integer division is intentional: Spark computes the target error as Long.
        let target_error = self
            .samples
            .iter()
            .map(|sample| sample.g.saturating_add(sample.delta))
            .max()
            .unwrap_or(0)
            / 2;
        let mut sorted = percentiles.iter().copied().enumerate().collect::<Vec<_>>();
        sorted.sort_by(|(_, a), (_, b)| double_order(a, b));
        let mut results = vec![0.0; percentiles.len()];
        let mut index = 0;
        let mut min_rank = self.samples[0].g;
        let relative_error = self.relative_error();
        for (position, percentile) in sorted {
            // Check the lower endpoint first: with accuracy=1 even p=1 returns min.
            let value = if percentile <= relative_error {
                self.samples[0].value
            } else if percentile >= 1.0 - relative_error {
                self.samples[self.samples.len() - 1].value
            } else {
                let rank = (percentile * self.count as f64).ceil() as u64;
                while index + 1 < self.samples.len() {
                    let sample = self.samples[index];
                    let max_rank = min_rank.saturating_add(sample.delta);
                    if max_rank as f64 - target_error as f64 <= rank as f64
                        && rank as f64 <= min_rank as f64 + target_error as f64
                    {
                        break;
                    }
                    index += 1;
                    min_rank = min_rank.saturating_add(self.samples[index].g);
                }
                self.samples[index].value
            };
            results[position] = value;
        }
        results
    }

    pub(super) fn serialize(&mut self) -> Vec<u8> {
        self.compress();
        let mut bytes = Vec::with_capacity(HEADER_SIZE + self.samples.len() * SAMPLE_SIZE);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.accuracy.to_le_bytes());
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes.extend_from_slice(&(self.samples.len() as u64).to_le_bytes());
        for sample in &self.samples {
            bytes.extend_from_slice(&sample.value.to_bits().to_le_bytes());
            bytes.extend_from_slice(&sample.g.to_le_bytes());
            bytes.extend_from_slice(&sample.delta.to_le_bytes());
        }
        bytes
    }

    pub(super) fn from_bytes(bytes: &[u8], accuracy: u32) -> Result<Self> {
        if bytes.len() < HEADER_SIZE || &bytes[..4] != MAGIC {
            return Err(invalid_state());
        }
        let stored_accuracy =
            u32::from_le_bytes(bytes[4..8].try_into().map_err(|_| invalid_state())?);
        if stored_accuracy != accuracy || accuracy == 0 || accuracy > i32::MAX as u32 {
            return Err(invalid_state());
        }
        let count = read_word(&bytes[8..16])?;
        let length = usize::try_from(read_word(&bytes[16..24])?).map_err(|_| invalid_state())?;
        let expected_length = length
            .checked_mul(SAMPLE_SIZE)
            .and_then(|length| length.checked_add(HEADER_SIZE))
            .ok_or_else(invalid_state)?;
        if expected_length != bytes.len()
            || count > i64::MAX as u64
            || (count == 0) != (length == 0)
            || length as u64 > count
        {
            return Err(invalid_state());
        }
        let mut samples = Vec::with_capacity(length);
        let mut minimum_ranks = 0_u64;
        for entry in bytes[HEADER_SIZE..].as_chunks::<SAMPLE_SIZE>().0 {
            let sample = Sample {
                value: f64::from_bits(read_word(&entry[..8])?),
                g: read_word(&entry[8..16])?,
                delta: read_word(&entry[16..24])?,
            };
            minimum_ranks = minimum_ranks
                .checked_add(sample.g)
                .ok_or_else(invalid_state)?;
            if sample.g == 0 || minimum_ranks > count || sample.delta > count.saturating_mul(2) {
                return Err(invalid_state());
            }
            samples.push(sample);
        }
        Ok(Self {
            accuracy,
            count,
            samples,
            head: Vec::new(),
            compressed: true,
        })
    }
}

fn compress_samples(samples: &[Sample], threshold: f64) -> Vec<Sample> {
    let Some(mut head) = samples.last().copied() else {
        return Vec::new();
    };
    if samples.len() == 1 {
        return samples.to_vec();
    }
    let mut compressed = Vec::new();
    for sample in samples[1..samples.len().saturating_sub(1)].iter().rev() {
        let combined_rank = sample.g.saturating_add(head.g).saturating_add(head.delta);
        if (combined_rank as f64) < threshold {
            head.g = head.g.saturating_add(sample.g);
        } else {
            compressed.push(head);
            head = *sample;
        }
    }
    compressed.push(head);
    // Preserve Spark's ordinary <= check, including its two-element NaN behavior.
    if samples.len() > 1 && samples[0].value <= head.value {
        compressed.push(samples[0]);
    }
    compressed.reverse();
    compressed.shrink_to_fit();
    compressed
}

/// Scala's sorting uses Double.compare; the summary merge itself uses IEEE comparisons.
fn double_order(a: &f64, b: &f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.total_cmp(b),
    }
}

fn read_word(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes.try_into().map_err(|_| invalid_state())?,
    ))
}

fn invalid_state() -> DataFusionError {
    DataFusionError::Execution("Invalid approx_percentile summary state".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(values: impl IntoIterator<Item = f64>, accuracy: u32) -> QuantileSummary {
        let mut summary = QuantileSummary::new(accuracy);
        for value in values {
            summary.insert(value);
        }
        summary
    }

    #[test]
    fn matches_spark_discrete_ranks_array_order_and_accuracy_one() {
        let values = [0.0, 1.0, 2.0, 10.0];
        assert_eq!(
            summary(values, 100).query(&[0.5, 0.4, 0.1, 0.5]),
            [1.0, 1.0, 0.0, 1.0]
        );
        assert_eq!(summary(values, 1).query(&[0.0, 0.5, 1.0]), [0.0, 0.0, 0.0]);
        assert_eq!(summary([], 100).query(&[0.5]), Vec::<f64>::new());
        assert_eq!(summary(values, 100).query(&[]), Vec::<f64>::new());
    }

    #[test]
    fn preserves_spark_nan_and_signed_zero_behavior() {
        assert!(summary([1.0, f64::NAN], 100)
            .query(&[0.0, 0.5, 1.0])
            .iter()
            .all(|v| v.is_nan()));
        let result = summary([1.0, 2.0, f64::NAN], 100).query(&[0.0, 0.5, 1.0]);
        assert_eq!(&result[..2], &[1.0, 2.0]);
        assert!(result[2].is_nan());
        let result = summary([-0.0, 0.0], 100).query(&[0.0, 0.5, 1.0]);
        assert_eq!(
            result.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            [
                (-0.0_f64).to_bits(),
                (-0.0_f64).to_bits(),
                0.0_f64.to_bits()
            ]
        );
    }

    #[test]
    fn matches_spark_oracle_after_multiple_flushes_and_interleaved_merge() {
        // Captured from Spark's QuantileSummaries with the same head/compress thresholds.
        let mut ordered = summary((0..125_001).map(f64::from), 100);
        assert_eq!(
            ordered.query(&[0.0, 0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999, 1.0]),
            [0.0, 0.0, 0.0, 12_037.0, 61_537.0, 111_438.0, 125_000.0, 125_000.0, 125_000.0]
        );
        assert_eq!(ordered.samples.len(), 228);
        let mut shuffled = summary(
            (0..125_001_u64).map(|i| ((i * 7_919) % 125_001) as f64),
            100,
        );
        assert_eq!(
            shuffled.query(&[0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0]),
            [0.0, 0.0, 11_634.0, 62_392.0, 112_127.0, 125_000.0, 125_000.0]
        );
        assert_eq!(shuffled.samples.len(), 84);
        let mut even = summary((0..100_000).step_by(2).map(f64::from), 100);
        let odd = summary((1..100_000).step_by(2).map(f64::from), 100);
        even.merge(&odd);
        assert_eq!(
            even.query(&[0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0]),
            [0.0, 0.0, 9_273.0, 49_892.0, 89_053.0, 99_999.0, 99_999.0]
        );
        assert_eq!(even.samples.len(), 400);
    }

    #[test]
    fn merged_and_serialized_summaries_preserve_rank_error_bounds() {
        let count = 100_000;
        let accuracy = 100;
        let mut merged = QuantileSummary::new(accuracy);
        for partition in 0..7 {
            let mut partial = summary((partition..count).step_by(7).map(f64::from), accuracy);
            let bytes = partial.serialize();
            let decoded = QuantileSummary::from_bytes(&bytes, accuracy).unwrap();
            merged.merge(&decoded);
        }
        assert_eq!(merged.count(), count as u64);
        let percentages = [0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0];
        let estimates = merged.query(&percentages);
        for (&percentage, &estimate) in percentages.iter().zip(&estimates) {
            let target_rank = (percentage * f64::from(count)).ceil();
            assert!(
                (estimate + 1.0 - target_rank).abs()
                    <= f64::from(count) / f64::from(accuracy) + 1.0,
                "p={percentage} estimate={estimate}"
            );
        }
        let bytes = merged.serialize();
        let mut restored = QuantileSummary::from_bytes(&bytes, accuracy).unwrap();
        assert_eq!(restored.query(&percentages), estimates);
    }

    #[test]
    fn multi_flush_streams_remain_compressed_and_memory_bounded() {
        let mut summary = QuantileSummary::new(100);
        for value in 0..1_000_000 {
            summary.insert(f64::from(value));
            assert!(
                summary.size() < 1_000_000,
                "memory footprint grew to {}",
                summary.size()
            );
        }
        assert_eq!(summary.count(), 1_000_000);
        summary.compress();
        assert!(summary.samples.len() < 1_000);
        assert!(summary.size() < 30_000);
        let result = summary.query(&[0.1, 0.5, 0.9]);
        for (&value, percentile) in result.iter().zip([0.1, 0.5, 0.9]) {
            assert!((value - 1_000_000.0 * percentile).abs() <= 10_000.0);
        }
    }

    #[test]
    fn malformed_state_is_rejected_without_panics() {
        let mut digest = summary([1.0, 2.0], 100);
        let valid = digest.serialize();
        for end in 0..valid.len() {
            assert!(QuantileSummary::from_bytes(&valid[..end], 100).is_err());
        }
        assert!(QuantileSummary::from_bytes(&valid, 99).is_err());
        for (offset, value) in [(8, 0), (16, u64::MAX), (32, 0), (40, u64::MAX)] {
            let mut invalid = valid.clone();
            invalid[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            assert!(QuantileSummary::from_bytes(&invalid, 100).is_err());
        }
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(QuantileSummary::from_bytes(&trailing, 100).is_err());
        let nan_state = summary([1.0, f64::NAN], 100).serialize();
        assert!(QuantileSummary::from_bytes(&nan_state, 100).is_ok());
    }
}
