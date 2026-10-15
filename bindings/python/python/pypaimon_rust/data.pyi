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

from typing import Sequence

import pyarrow

def read_arrow_schema(fields: str) -> pyarrow.Schema:
    """Build the runtime read schema from Paimon fields (JSON)."""
    ...

def normalize_array_for_write(column: pyarrow.Array, target_type: pyarrow.DataType) -> pyarrow.Array:
    """Normalize a write layout and validate visible NOT NULL children in core."""
    ...

def cast_array_for_schema_evolution(
    column: pyarrow.Array, source_type: str, target_type: str
) -> pyarrow.Array:
    """Cast historical Paimon types (JSON) using the core table-reader rules."""
    ...

def variant_get_numeric_fields(
    column: pyarrow.Array, fields: Sequence[str]
) -> pyarrow.FixedSizeListArray:
    """Extract literal top-level numeric fields as float32 in the requested order."""
    ...
