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

const CANDIDATES_FACTOR: &str = "vector-index.post-filter.candidates-factor";

pub(super) fn candidates_factor(options: &HashMap<String, String>) -> crate::Result<usize> {
    match options.get(CANDIDATES_FACTOR) {
        None => Ok(10),
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0 && *n <= i32::MAX as usize)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!(
                    "{CANDIDATES_FACTOR} must be between 1 and {}, got {value}",
                    i32::MAX
                ),
            }),
    }
}

pub(super) fn candidate_limit(
    top_k: usize,
    factor: usize,
    required: usize,
    shard_size: u64,
) -> usize {
    top_k
        .saturating_mul(factor)
        .max(required)
        .min(usize::try_from(shard_size).unwrap_or(usize::MAX))
        .min(i32::MAX as usize)
}

// Bound retained candidate maps as well as native scratch allocations.
const MAX_BATCH_CANDIDATES: usize = 1 << 20;

pub(super) fn batch_size(candidate_limit: usize) -> usize {
    (MAX_BATCH_CANDIDATES / candidate_limit.max(1)).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_bound_candidate_maps_without_reducing_query_limits() {
        assert_eq!(batch_size(1000), 1048);
        assert_eq!(batch_size(usize::MAX), 1);
        assert_eq!(batch_size(1), MAX_BATCH_CANDIDATES);
    }

    #[test]
    fn count_uses_original_top_k_and_preserves_required_allowance() {
        assert_eq!(candidates_factor(&HashMap::new()).unwrap(), 10);
        assert_eq!(candidate_limit(2, 10, 6, 1000), 20);
        assert_eq!(candidate_limit(2, 1, 6, 1000), 6);
        assert_eq!(candidate_limit(2, 10, 6, 12), 12);
        assert_eq!(
            candidate_limit(usize::MAX, 10, 6, u64::MAX),
            i32::MAX as usize
        );
        assert_eq!(candidate_limit(2, 10, 6, 0), 0);
        for value in ["0", "-1", "abc", "2147483648"] {
            assert!(
                candidates_factor(&HashMap::from([(CANDIDATES_FACTOR.into(), value.into())]))
                    .is_err()
            );
        }
    }
    #[test]
    fn task_literal_transport_preserves_nan_signed_zero_and_decimal() {
        use crate::spec::Datum;
        for bits in [f32::NAN.to_bits(), (-0.0f32).to_bits()] {
            let json = serde_json::to_value(Datum::Float(f32::from_bits(bits))).unwrap();
            let Datum::Float(value) = serde_json::from_value(json).unwrap() else {
                panic!()
            };
            assert_eq!(value.to_bits(), bits);
        }
        for bits in [f64::NAN.to_bits(), (-0.0f64).to_bits()] {
            let json = serde_json::to_value(Datum::Double(f64::from_bits(bits))).unwrap();
            let Datum::Double(value) = serde_json::from_value(json).unwrap() else {
                panic!()
            };
            assert_eq!(value.to_bits(), bits);
        }
        let decimal = Datum::Decimal {
            unscaled: i128::MAX,
            precision: 38,
            scale: 2,
        };
        let encoded = serde_json::to_value(&decimal).unwrap();
        assert_eq!(serde_json::from_value::<Datum>(encoded).unwrap(), decimal);
    }
}
