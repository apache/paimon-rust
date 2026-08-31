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

//! Bounded-memory SA-IS suffix-array construction used by the FM index.

use std::io;

pub(crate) fn build(text: &[u16], upper: usize) -> io::Result<Vec<usize>> {
    if text.is_empty() || text.iter().any(|value| usize::from(*value) > upper) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "FM index text is empty or contains a symbol outside the alphabet",
        ));
    }
    sa_is(text, upper)
}

fn sa_is<T>(text: &[T], upper: usize) -> io::Result<Vec<usize>>
where
    T: Copy + Into<usize>,
{
    let len = text.len();
    if len == 1 {
        return Ok(vec![0]);
    }
    if len == 2 {
        return Ok(if text[0].into() < text[1].into() {
            vec![0, 1]
        } else {
            vec![1, 0]
        });
    }

    let mut s_type = vec![false; len];
    for i in (0..len - 1).rev() {
        let symbol = text[i].into();
        let next = text[i + 1].into();
        s_type[i] = if symbol == next {
            s_type[i + 1]
        } else {
            symbol < next
        };
    }

    let mut bucket_ends = vec![0usize; upper + 1];
    let mut bucket_starts = vec![0usize; upper + 1];
    for (i, value) in text.iter().enumerate() {
        let symbol = (*value).into();
        if s_type[i] {
            if symbol == upper {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "FM SA-IS S-type symbol exceeds its bucket range",
                ));
            }
            bucket_starts[symbol + 1] += 1;
        } else {
            bucket_ends[symbol] += 1;
        }
    }
    for symbol in 0..=upper {
        bucket_ends[symbol] += bucket_starts[symbol];
        if symbol < upper {
            bucket_starts[symbol + 1] += bucket_ends[symbol];
        }
    }

    let lms_index = LmsIndex::create(&s_type);
    let mut lms = Vec::with_capacity(lms_index.count);
    for i in 1..len {
        if lms_index.ordinal(i).is_some() {
            lms.push(i);
        }
    }

    let mut suffix_array = induce(text, &s_type, &bucket_starts, &bucket_ends, &lms)?;
    if lms.is_empty() {
        return suffix_array
            .into_iter()
            .map(|suffix| {
                suffix.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "FM SA-IS left an empty suffix slot",
                    )
                })
            })
            .collect();
    }

    let sorted_lms = suffix_array
        .iter()
        .filter_map(|suffix| suffix.and_then(|suffix| lms_index.ordinal(suffix).map(|_| suffix)))
        .collect::<Vec<_>>();
    if sorted_lms.len() != lms.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FM SA-IS lost an LMS suffix",
        ));
    }

    let mut reduced_text = vec![0usize; lms.len()];
    let mut reduced_upper = 0usize;
    let first_ordinal = lms_index
        .ordinal(sorted_lms[0])
        .expect("sorted LMS is an LMS");
    reduced_text[first_ordinal] = 0;
    for pair in sorted_lms.windows(2) {
        let mut previous = pair[0];
        let mut current = pair[1];
        let previous_ordinal = lms_index.ordinal(previous).expect("sorted LMS is an LMS");
        let current_ordinal = lms_index.ordinal(current).expect("sorted LMS is an LMS");
        let previous_end = if previous_ordinal + 1 < lms.len() {
            lms[previous_ordinal + 1] + 1
        } else {
            len
        };
        let current_end = if current_ordinal + 1 < lms.len() {
            lms[current_ordinal + 1] + 1
        } else {
            len
        };
        let mut same = previous_end - previous == current_end - current;
        while same && previous < previous_end {
            if text[previous].into() != text[current].into() || s_type[previous] != s_type[current]
            {
                same = false;
            }
            previous += 1;
            current += 1;
        }
        if !same {
            reduced_upper += 1;
        }
        reduced_text[current_ordinal] = reduced_upper;
    }

    let reduced_sa = if reduced_upper + 1 == lms.len() {
        let mut result = vec![0usize; lms.len()];
        for (ordinal, symbol) in reduced_text.iter().enumerate() {
            result[*symbol] = ordinal;
        }
        result
    } else {
        sa_is(&reduced_text, reduced_upper)?
    };
    let sorted_lms = reduced_sa
        .into_iter()
        .map(|ordinal| lms[ordinal])
        .collect::<Vec<_>>();
    suffix_array = induce(text, &s_type, &bucket_starts, &bucket_ends, &sorted_lms)?;
    suffix_array
        .into_iter()
        .map(|suffix| {
            suffix.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "FM SA-IS left an empty suffix slot",
                )
            })
        })
        .collect()
}

fn induce<T>(
    text: &[T],
    s_type: &[bool],
    bucket_starts: &[usize],
    bucket_ends: &[usize],
    lms: &[usize],
) -> io::Result<Vec<Option<usize>>>
where
    T: Copy + Into<usize>,
{
    let mut suffix_array = vec![None; text.len()];
    let mut buffer = bucket_ends.to_vec();
    for &suffix in lms {
        let symbol = text[suffix].into();
        let position = buffer[symbol];
        if position >= suffix_array.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FM SA-IS LMS bucket overflow",
            ));
        }
        suffix_array[position] = Some(suffix);
        buffer[symbol] += 1;
    }

    buffer.clone_from_slice(bucket_starts);
    let last = text.len() - 1;
    let last_symbol = text[last].into();
    suffix_array[buffer[last_symbol]] = Some(last);
    buffer[last_symbol] += 1;
    for i in 0..suffix_array.len() {
        if let Some(suffix) = suffix_array[i] {
            if suffix >= 1 && !s_type[suffix - 1] {
                let symbol = text[suffix - 1].into();
                let position = buffer[symbol];
                suffix_array[position] = Some(suffix - 1);
                buffer[symbol] += 1;
            }
        }
    }

    buffer.clone_from_slice(bucket_starts);
    for i in (0..suffix_array.len()).rev() {
        if let Some(suffix) = suffix_array[i] {
            if suffix >= 1 && s_type[suffix - 1] {
                let symbol = text[suffix - 1].into();
                let next_bucket = symbol.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "FM SA-IS bucket overflow")
                })?;
                if next_bucket >= buffer.len() || buffer[next_bucket] == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "FM SA-IS S-type bucket underflow",
                    ));
                }
                buffer[next_bucket] -= 1;
                suffix_array[buffer[next_bucket]] = Some(suffix - 1);
            }
        }
    }
    Ok(suffix_array)
}

struct LmsIndex {
    words: Vec<u64>,
    word_prefixes: Vec<usize>,
    count: usize,
}

impl LmsIndex {
    fn create(s_type: &[bool]) -> Self {
        let mut words = vec![0u64; s_type.len().div_ceil(64)];
        for i in 1..s_type.len() {
            if !s_type[i - 1] && s_type[i] {
                words[i >> 6] |= 1u64 << (i & 63);
            }
        }
        let mut word_prefixes = Vec::with_capacity(words.len());
        let mut count = 0usize;
        for word in &words {
            word_prefixes.push(count);
            count += word.count_ones() as usize;
        }
        Self {
            words,
            word_prefixes,
            count,
        }
    }

    fn ordinal(&self, position: usize) -> Option<usize> {
        let word_index = position >> 6;
        let word = *self.words.get(word_index)?;
        let bit = 1u64 << (position & 63);
        if word & bit == 0 {
            return None;
        }
        Some(self.word_prefixes[word_index] + (word & (bit - 1)).count_ones() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    fn naive(text: &[u16]) -> Vec<usize> {
        let mut suffixes = (0..text.len()).collect::<Vec<_>>();
        suffixes.sort_by(|left, right| text[*left..].cmp(&text[*right..]));
        suffixes
    }

    #[test]
    fn known_and_random_suffix_arrays_match_naive_sort() {
        let banana = [2, 1, 3, 1, 3, 1, 0];
        assert_eq!(build(&banana, 3).unwrap(), vec![6, 5, 3, 1, 0, 4, 2]);

        let mut random = StdRng::seed_from_u64(1847);
        for length in 1..=80 {
            for _ in 0..30 {
                let alphabet = random.gen_range(1..=12);
                let mut text = (0..length)
                    .map(|_| random.gen_range(1..=alphabet) as u16)
                    .collect::<Vec<_>>();
                text.push(0);
                assert_eq!(build(&text, alphabet).unwrap(), naive(&text));
            }
        }
    }
}
