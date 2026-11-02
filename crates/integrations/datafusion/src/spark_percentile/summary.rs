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

// Port of Spark's mergeable Greenwald–Khanna QuantileSummaries (v4.1.2).
// https://github.com/apache/spark/blob/v4.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/QuantileSummaries.scala
// Keep the insertion, strict compression threshold and integer rank-error calculation together.
use datafusion::common::{DataFusionError, Result};

const HEAD_SIZE: usize = 50_000;
const COMPRESS_THRESHOLD: usize = 10_000;

#[derive(Clone, Copy, Debug)]
pub(super) struct Sample {
    pub value: f64,
    pub g: u64,
    pub delta: u64,
}

#[derive(Debug)]
pub(super) struct Summary {
    pub error: f64,
    pub count: u64,
    pub samples: Vec<Sample>,
    head: Vec<f64>,
    compressed: bool,
}

impl Summary {
    pub fn new(error: f64) -> Self {
        Self {
            error,
            count: 0,
            samples: Vec::new(),
            head: Vec::new(),
            compressed: true,
        }
    }
    pub fn insert(&mut self, value: f64) -> Result<()> {
        if self.count >= i64::MAX as u64 - self.head.len() as u64 {
            return Err(DataFusionError::Execution(
                "approx_percentile row count exceeds BIGINT".into(),
            ));
        }
        self.head.push(value);
        self.compressed = false;
        if self.head.len() >= HEAD_SIZE {
            self.insert_head();
            if self.samples.len() >= COMPRESS_THRESHOLD {
                self.compress();
            }
        }
        Ok(())
    }
    fn insert_head(&mut self) {
        if self.head.is_empty() {
            return;
        }
        // Scala's Double ordering places NaN last and distinguishes signed zero.
        self.head.sort_unstable_by(|a, b| {
            if a.is_nan() {
                if b.is_nan() {
                    std::cmp::Ordering::Equal
                } else {
                    std::cmp::Ordering::Greater
                }
            } else if b.is_nan() {
                std::cmp::Ordering::Less
            } else {
                a.total_cmp(b)
            }
        });
        let mut merged = Vec::with_capacity(self.samples.len() + self.head.len());
        let mut index = 0;
        for (position, &value) in self.head.iter().enumerate() {
            while index < self.samples.len() && self.samples[index].value <= value {
                merged.push(self.samples[index]);
                index += 1;
            }
            self.count += 1;
            let delta = if merged.is_empty()
                || (index == self.samples.len() && position + 1 == self.head.len())
            {
                0
            } else {
                (2.0 * self.error * self.count as f64).floor() as u64
            };
            merged.push(Sample { value, g: 1, delta });
        }
        merged.extend_from_slice(&self.samples[index..]);
        self.samples = merged;
        self.head.clear();
    }
    pub fn compress(&mut self) {
        if self.compressed {
            return;
        }
        self.insert_head();
        self.samples = compress(&self.samples, 2.0 * self.error * self.count as f64);
        self.compressed = true;
    }
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        self.compress();
        other.compress();
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = other;
            return Ok(());
        }
        let count = self
            .count
            .checked_add(other.count)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| {
                DataFusionError::Execution("approx_percentile row count exceeds BIGINT".into())
            })?;
        let error = self.error.max(other.error);
        let self_delta = (2.0 * other.error * other.count as f64).floor() as u64;
        let other_delta = (2.0 * self.error * self.count as f64).floor() as u64;
        let mut merged = Vec::with_capacity(self.samples.len() + other.samples.len());
        let (mut left, mut right) = (0, 0);
        while left < self.samples.len() && right < other.samples.len() {
            let (mut next, delta) = if self.samples[left].value < other.samples[right].value {
                let next = self.samples[left];
                left += 1;
                (next, if right > 0 { self_delta } else { 0 })
            } else {
                let next = other.samples[right];
                right += 1;
                (next, if left > 0 { other_delta } else { 0 })
            };
            next.delta = next.delta.checked_add(delta).ok_or_else(|| {
                DataFusionError::Execution("approx_percentile rank overflow".into())
            })?;
            merged.push(next);
        }
        merged.extend_from_slice(&self.samples[left..]);
        merged.extend_from_slice(&other.samples[right..]);
        self.samples = compress(&merged, 2.0 * error * count as f64);
        self.count = count;
        self.error = error;
        self.compressed = true;
        Ok(())
    }
    pub fn query(&mut self, probabilities: &[f64]) -> Vec<f64> {
        self.compress();
        if self.samples.is_empty() {
            return Vec::new();
        }
        let target_error = self.samples.iter().map(|s| s.g + s.delta).max().unwrap() / 2;
        let mut ordered = probabilities
            .iter()
            .copied()
            .enumerate()
            .collect::<Vec<_>>();
        ordered.sort_by(|a, b| a.1.total_cmp(&b.1));
        let mut result = vec![0.0; probabilities.len()];
        let (mut index, mut min_rank) = (0, self.samples[0].g);
        for (position, probability) in ordered {
            result[position] = if probability <= self.error {
                self.samples[0].value
            } else if probability >= 1.0 - self.error {
                self.samples.last().unwrap().value
            } else {
                let rank = (probability * self.count as f64).ceil();
                while index + 1 < self.samples.len() {
                    let sample = self.samples[index];
                    let max_rank = min_rank + sample.delta;
                    if max_rank as f64 - target_error as f64 <= rank
                        && rank <= min_rank as f64 + target_error as f64
                    {
                        break;
                    }
                    index += 1;
                    min_rank += self.samples[index].g;
                }
                if index + 1 == self.samples.len() {
                    min_rank = 0;
                }
                self.samples[index].value
            };
        }
        result
    }
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + self.head.capacity() * size_of::<f64>()
            + self.samples.capacity() * size_of::<Sample>()
    }
    pub fn encode(&mut self, family: u8) -> Vec<u8> {
        self.compress();
        let mut bytes = Vec::with_capacity(9 + self.samples.len() * 24);
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes.push(family);
        for sample in &self.samples {
            bytes.extend_from_slice(&sample.value.to_le_bytes());
            bytes.extend_from_slice(&sample.g.to_le_bytes());
            bytes.extend_from_slice(&sample.delta.to_le_bytes());
        }
        bytes
    }
    pub fn decode(bytes: &[u8], error: f64) -> Result<(Self, u8)> {
        let invalid =
            || DataFusionError::Execution("Invalid approx_percentile intermediate state".into());
        if bytes.len() < 9 || !(bytes.len() - 9).is_multiple_of(24) {
            return Err(invalid());
        }
        let count = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        if count > i64::MAX as u64 || (count == 0) != (bytes.len() == 9) || bytes[8] > 2 {
            return Err(invalid());
        }
        let mut summary = Self::new(error);
        summary.count = count;
        for sample in bytes[9..].as_chunks::<24>().0 {
            let g = u64::from_le_bytes(sample[8..16].try_into().unwrap());
            let delta = u64::from_le_bytes(sample[16..24].try_into().unwrap());
            if g == 0 || g > count || g.checked_add(delta).is_none() {
                return Err(invalid());
            }
            summary.samples.push(Sample {
                value: f64::from_le_bytes(sample[..8].try_into().unwrap()),
                g,
                delta,
            });
        }
        Ok((summary, bytes[8]))
    }
}

fn compress(samples: &[Sample], threshold: f64) -> Vec<Sample> {
    let Some(&last) = samples.last() else {
        return Vec::new();
    };
    if samples.len() == 1 {
        return vec![last];
    }
    let mut result = Vec::with_capacity(samples.len());
    let mut head = last;
    for &sample in samples[1..samples.len().saturating_sub(1)].iter().rev() {
        if (sample.g as f64 + head.g as f64 + head.delta as f64) < threshold {
            head.g += sample.g;
        } else {
            result.push(head);
            head = sample;
        }
    }
    result.push(head);
    if samples.len() > 1 && samples[0].value <= head.value {
        result.push(samples[0]);
    }
    result.reverse();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compressed_partial_summaries_match_spark_quantiles() {
        for (accuracy, expected) in [
            (10, vec![0.0, 0.0, 57013.0, 100000.0, 100000.0]),
            (10000, vec![0.0, 9995.0, 49999.0, 89994.0, 100000.0]),
        ] {
            let mut left = Summary::new(1.0 / accuracy as f64);
            let mut right = Summary::new(1.0 / accuracy as f64);
            for value in 0..50000 {
                left.insert(value as f64).unwrap();
            }
            for value in 50000..100001 {
                right.insert(value as f64).unwrap();
            }
            let bytes = right.encode(0);
            let (right, _) = Summary::decode(&bytes, left.error).unwrap();
            left.merge(right).unwrap();
            assert_eq!(left.count, 100001);
            assert_eq!(left.query(&[0.0, 0.1, 0.5, 0.9, 1.0]), expected);
            // Independent rank bounds, not just matching one implementation's samples.
            for (probability, value) in [0.1, 0.5, 0.9]
                .into_iter()
                .zip(left.query(&[0.1, 0.5, 0.9]))
            {
                let rank = value + 1.0;
                let n = left.count as f64;
                assert!(
                    rank >= ((probability - left.error) * n).floor()
                        && rank <= ((probability + left.error) * n).ceil()
                );
            }
        }
    }
    #[test]
    fn summary_state_handles_empty_singleton_and_nan() {
        let mut empty = Summary::new(0.0001);
        assert!(empty.query(&[0.5]).is_empty());
        for bytes in [vec![], vec![0; 8], vec![0; 10]] {
            assert!(Summary::decode(&bytes, 0.0001).is_err());
        }
        empty.insert(1.0).unwrap();
        let (mut copy, family) = Summary::decode(&empty.encode(2), empty.error).unwrap();
        assert_eq!(family, 2);
        assert_eq!(copy.query(&[0.0, 0.5, 1.0]), vec![1.0; 3]);
        let mut nan = Summary::new(0.0001);
        for value in [f64::NAN, 1.0, 2.0] {
            nan.insert(value).unwrap();
        }
        let result = nan.query(&[0.0, 0.5, 1.0]);
        assert_eq!(&result[..2], &[1.0, 2.0]);
        assert!(result[2].is_nan());
    }
}
