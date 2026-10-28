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

//! Batch temporal matching over sorted, non-null int64 keys.

use arrow::array::{Array, ArrayData, Int64Array};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::sync::Arc;

fn lower(times: &[i64], target: i128, mut start: usize, mut end: usize) -> usize {
    while start < end {
        let middle = start + (end - start) / 2;
        if (times[middle] as i128) < target {
            start = middle + 1;
        } else {
            end = middle;
        }
    }
    start
}

fn upper(times: &[i64], target: i128, mut start: usize, mut end: usize) -> usize {
    while start < end {
        let middle = start + (end - start) / 2;
        if (times[middle] as i128) <= target {
            start = middle + 1;
        } else {
            end = middle;
        }
    }
    start
}

#[derive(Clone, Copy)]
enum Mode {
    Nearest,
    Backward,
    Forward,
    Interpolate,
    Window,
}

struct Options {
    mode: Mode,
    exact: bool,
    tolerance: Option<i64>,
    preceding: i64,
    following: i64,
    left_closed: bool,
    right_closed: bool,
}

fn match_one(
    times: &[i64],
    target: i64,
    start: usize,
    end: usize,
    options: &Options,
) -> (i64, i64) {
    if start == end {
        return (-1, -1);
    }
    let target = target as i128;
    if matches!(options.mode, Mode::Window) {
        let left = target - options.preceding as i128;
        let right = target + options.following as i128;
        let first = if options.left_closed {
            lower(times, left, start, end)
        } else {
            upper(times, left, start, end)
        };
        let last = if options.right_closed {
            upper(times, right, start, end)
        } else {
            lower(times, right, start, end)
        };
        return if last > first {
            (first as i64, last as i64)
        } else {
            (0, 0)
        };
    }
    let left = lower(times, target, start, end);
    let right = upper(times, target, left, end);
    if matches!(options.mode, Mode::Interpolate) {
        if left < end && times[left] as i128 == target {
            return ((right - 1) as i64, (right - 1) as i64);
        }
        if left > start
            && left < end
            && options.tolerance.is_none_or(|tolerance| {
                (target - times[left - 1] as i128).max(times[left] as i128 - target)
                    <= tolerance as i128
            })
        {
            return ((left - 1) as i64, left as i64);
        }
        return (-1, -1);
    }
    let before = if options.exact { right } else { left };
    let after = if options.exact { left } else { right };
    let position = match options.mode {
        Mode::Backward => (before > start).then(|| before - 1),
        Mode::Forward => (after < end).then_some(after),
        _ if options.exact && left < end && times[left] as i128 == target => Some(right - 1),
        _ if left == start => (after < end).then_some(after),
        _ if after == end || target - times[left - 1] as i128 <= times[after] as i128 - target => {
            Some(left - 1)
        }
        _ => Some(after),
    };
    match position {
        Some(position)
            if options.tolerance.is_none_or(|tolerance| {
                (times[position] as i128 - target).abs() <= tolerance as i128
            }) =>
        {
            (position as i64, -1)
        }
        _ => (-1, -1),
    }
}

fn int64_array(value: &Bound<'_, PyAny>) -> PyResult<Int64Array> {
    let data = ArrayData::from_pyarrow_bound(value)?;
    if data.data_type() != &arrow::datatypes::DataType::Int64 || data.null_count() != 0 {
        return Err(PyValueError::new_err(
            "Expected a non-null int64 Arrow array",
        ));
    }
    Ok(Int64Array::from(data))
}

/// Match each target within its [start, end) group of sorted times.
/// Returns indices, not row IDs; -1 denotes a missing match. Window end indices
/// are exclusive. Equal nearest/interpolation keys select the last duplicate;
/// nearest ties select the earlier key. Arithmetic uses i128 at int64 limits.
#[pyfunction]
#[pyo3(signature = (times, targets, starts, ends, mode, exact=true, tolerance=None, preceding=0, following=0, left_closed=true, right_closed=true))]
#[allow(clippy::too_many_arguments)]
pub fn temporal_match(
    py: Python<'_>,
    times: &Bound<'_, PyAny>,
    targets: &Bound<'_, PyAny>,
    starts: &Bound<'_, PyAny>,
    ends: &Bound<'_, PyAny>,
    mode: &str,
    exact: bool,
    tolerance: Option<i64>,
    preceding: i64,
    following: i64,
    left_closed: bool,
    right_closed: bool,
) -> PyResult<Py<PyAny>> {
    let times = int64_array(times)?;
    let targets = int64_array(targets)?;
    let starts = int64_array(starts)?;
    let ends = int64_array(ends)?;
    let mode = match mode {
        "nearest" => Mode::Nearest,
        "backward" => Mode::Backward,
        "forward" => Mode::Forward,
        "interpolate" => Mode::Interpolate,
        "window" => Mode::Window,
        _ => return Err(PyValueError::new_err("Unknown temporal match mode")),
    };
    if targets.len() != starts.len() || targets.len() != ends.len() {
        return Err(PyValueError::new_err(
            "Target and group-bound arrays must have equal lengths",
        ));
    }
    if tolerance.is_some_and(|value| value < 0) || preceding < 0 || following < 0 {
        return Err(PyValueError::new_err(
            "Temporal distances must be non-negative",
        ));
    }
    for (&start, &end) in starts.values().iter().zip(ends.values()) {
        if start < 0 || end < start || end as u64 > times.len() as u64 {
            return Err(PyValueError::new_err("Invalid temporal group bounds"));
        }
    }
    let options = Options {
        mode,
        exact,
        tolerance,
        preceding,
        following,
        left_closed,
        right_closed,
    };
    let (first, second): (Vec<i64>, Vec<i64>) = py.detach(|| {
        targets
            .values()
            .iter()
            .zip(starts.values())
            .zip(ends.values())
            .map(|((&target, &start), &end)| {
                match_one(
                    times.values(),
                    target,
                    start as usize,
                    end as usize,
                    &options,
                )
            })
            .unzip()
    });
    let batch = RecordBatch::try_from_iter(vec![
        ("first", Arc::new(Int64Array::from(first)) as Arc<dyn Array>),
        (
            "second",
            Arc::new(Int64Array::from(second)) as Arc<dyn Array>,
        ),
    ])
    .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(batch.to_pyarrow(py)?.unbind())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn options(mode: Mode) -> Options {
        Options {
            mode,
            exact: true,
            tolerance: None,
            preceding: 0,
            following: 0,
            left_closed: true,
            right_closed: true,
        }
    }
    #[test]
    fn duplicate_and_tie_policy() {
        let times = [0, 10, 10, 20];
        assert_eq!(
            match_one(&times, 10, 0, 4, &options(Mode::Nearest)),
            (2, -1)
        );
        assert_eq!(
            match_one(&times, 10, 0, 4, &options(Mode::Forward)),
            (1, -1)
        );
        assert_eq!(
            match_one(&times, 15, 0, 4, &options(Mode::Nearest)),
            (2, -1)
        );
        assert_eq!(
            match_one(&times, 10, 0, 4, &options(Mode::Interpolate)),
            (2, 2)
        );
        let mut option = options(Mode::Nearest);
        option.exact = false;
        assert_eq!(match_one(&times, 10, 0, 4, &option), (0, -1));
    }
    #[test]
    fn limits_and_tolerance() {
        let times = [i64::MIN, i64::MAX];
        assert_eq!(match_one(&times, 0, 0, 2, &options(Mode::Nearest)), (1, -1));
        let mut option = options(Mode::Interpolate);
        option.tolerance = Some(i64::MAX);
        assert_eq!(match_one(&times, 0, 0, 2, &option), (-1, -1));
        assert_eq!(match_one(&times, 0, 1, 1, &option), (-1, -1));
        let mut option = options(Mode::Window);
        option.preceding = i64::MAX;
        option.following = i64::MAX;
        assert_eq!(match_one(&times, i64::MIN, 0, 2, &option), (0, 1));
        assert_eq!(match_one(&times, i64::MAX, 0, 2, &option), (1, 2));
    }
    #[test]
    fn window_closure_and_groups() {
        let times = [0, 10, 10, 20, 0, 5];
        let mut option = options(Mode::Window);
        assert_eq!(match_one(&times, 10, 0, 4, &option), (1, 3));
        option.left_closed = false;
        assert_eq!(match_one(&times, 10, 0, 4, &option), (0, 0));
        assert_eq!(match_one(&times, 4, 4, 6, &options(Mode::Nearest)), (5, -1));
    }
}
