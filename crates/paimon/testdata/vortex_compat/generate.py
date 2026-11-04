#!/usr/bin/env python3
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

"""Generate with Python 3.11, vortex-data==0.75.0 and pyarrow==23.0.1."""
from pathlib import Path
import pyarrow as pa
import vortex
import vortex.io

assert vortex.__version__ == "0.75.0"
profile_type = pa.struct([pa.field("detail", pa.struct([pa.field("keep", pa.int32())]))])
profiles = [{"detail": {"keep": 7}}, {"detail": None}, None, {"detail": {"keep": 9}}] * 128
table = pa.table({
    "id": pa.array(range(512), type=pa.int32()),
    "name": pa.array(["one", None, "three", "four"] * 128),
    "profile": pa.array(profiles, type=profile_type),
})
vortex.io.write(vortex.array(table), str(Path(__file__).with_name("vortex_0_75.vortex")))
