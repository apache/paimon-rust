# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import bisect
import random

import pyarrow as pa
import pytest
from pypaimon_rust import temporal_match


def call(times, targets, mode="nearest", starts=None, ends=None, **kwargs):
    arrays = [times, targets, starts if starts is not None else [0] * len(targets),
              ends if ends is not None else [len(times)] * len(targets)]
    return temporal_match(*(pa.array(values, type=pa.int64()) for values in arrays),
                          mode, **kwargs).to_pydict()


@pytest.mark.parametrize("mode", ["nearest", "backward", "forward", "interpolate", "window"])
def test_random_matching(mode):
    rng = random.Random(42)
    for _ in range(30):
        times = sorted(rng.randrange(-20, 20) for _ in range(40))
        targets = [rng.randrange(-30, 30) for _ in range(50)]
        for exact in (False, True):
            for tolerance in (None, 0, 3):
                actual = call(times, targets, mode, exact=exact, tolerance=tolerance,
                              preceding=3, following=5)
                for i, target in enumerate(targets):
                    left, right = bisect.bisect_left(times, target), bisect.bisect_right(times, target)
                    if mode == "window":
                        expected = (bisect.bisect_left(times, target - 3),
                                    bisect.bisect_right(times, target + 5))
                        if expected[0] == expected[1]:
                            expected = (0, 0)
                    elif mode == "interpolate":
                        if left < right:
                            expected = (right - 1, right - 1)
                        elif left == 0 or left == len(times):
                            expected = (-1, -1)
                        elif tolerance is not None and max(target-times[left-1], times[left]-target) > tolerance:
                            expected = (-1, -1)
                        else:
                            expected = (left - 1, left)
                    else:
                        candidates = [(j, value) for j, value in enumerate(times)
                                      if (exact or value != target)
                                      and (mode != "backward" or value <= target)
                                      and (mode != "forward" or value >= target)
                                      and (tolerance is None or abs(value-target) <= tolerance)]
                        if not candidates:
                            expected = (-1, -1)
                        elif mode == "backward":
                            expected = (candidates[-1][0], -1)
                        elif mode == "forward":
                            expected = (candidates[0][0], -1)
                        else:
                            value = min((value for _, value in candidates), key=lambda v: (abs(v-target), v))
                            indices = [j for j, v in candidates if v == value]
                            expected = (indices[-1] if value <= target else indices[0], -1)
                    assert (actual["first"][i], actual["second"][i]) == expected


@pytest.mark.parametrize("left", [False, True])
@pytest.mark.parametrize("right", [False, True])
def test_window_closure_and_limits(left, right):
    low, high = -(1 << 63), (1 << 63) - 1
    times = [low, -1, 0, 0, 1, high]
    for target in times:
        result = call(times, [target], "window", preceding=high, following=high,
                      left_closed=left, right_closed=right)
        positions = [i for i, value in enumerate(times)
                     if (value >= target-high if left else value > target-high)
                     and (value <= target+high if right else value < target+high)]
        assert (result["first"][0], result["second"][0]) == (
            (positions[0], positions[-1]+1) if positions else (0, 0))


def test_sliced_arrays_and_group_bounds():
    times = pa.array([999, 1, 3, 0, 2, 999], type=pa.int64()).slice(1, 4)
    targets = pa.array([2, 1, 1], type=pa.int64())
    starts = pa.array([0, 2, 0], type=pa.int64())
    ends = pa.array([2, 4, 0], type=pa.int64())
    assert temporal_match(times, targets, starts, ends, "nearest").column(0).to_pylist() == [0, 2, -1]
    assert call([], [], "nearest") == {"first": [], "second": []}


@pytest.mark.parametrize("kwargs", [dict(starts=[-1]), dict(ends=[2]), dict(starts=[1], ends=[0]),
                                     dict(starts=[]), dict(tolerance=-1), dict(preceding=-1),
                                     dict(mode="invalid")])
def test_invalid_arguments(kwargs):
    with pytest.raises(ValueError):
        call([0], [0], **kwargs)


def test_invalid_arrow_keys():
    good = pa.array([0], type=pa.int64())
    for invalid in (pa.array([None], type=pa.int64()), pa.array([0.0])):
        with pytest.raises(ValueError):
            temporal_match(invalid, good, good, good, "nearest")
