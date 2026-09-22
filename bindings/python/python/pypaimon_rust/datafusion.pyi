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

from os import PathLike
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, TypeAlias, Union

import pyarrow

ArrowTypeLike: TypeAlias = Union[pyarrow.DataType, pyarrow.Field, str]
InputFieldsLike: TypeAlias = Union[ArrowTypeLike, Sequence[ArrowTypeLike]]
VolatilityLike: TypeAlias = Union[str, Any]

class DataField:
    def name(self) -> str: ...
    def field_type(self) -> str: ...
    def is_nullable(self) -> bool: ...
    def description(self) -> Optional[str]: ...

class TableSchema:
    def fields(self) -> List[DataField]: ...
    def partition_keys(self) -> List[str]: ...
    def primary_keys(self) -> List[str]: ...
    def options(self) -> Dict[str, str]: ...
    def comment(self) -> Optional[str]: ...

class Split:
    def __init__(self, state: bytes) -> None: ...
    @staticmethod
    def deserialize(state: bytes) -> "Split":
        """Decode a Java-compatible SplitSerializer v1 frame."""
        ...
    def row_count(self) -> int: ...
    def is_streaming(self) -> bool: ...
    # Java SplitSerializer v1 binary: DataSplit v8/v9, or score-free
    # IndexedSplit when the native split has row ranges.
    def serialize(self) -> bytes: ...

class Plan:
    def snapshot_id(self) -> Optional[int]:
        """Selected snapshot, preserved for empty plans; None if no snapshot was selected."""
        ...
    def splits(self) -> List[Split]: ...
    def __len__(self) -> int: ...

class TableScan:
    def with_row_position_slice(self, start: int, end: int) -> "TableScan": ...
    def with_row_position_shard(self, index: int, count: int) -> "TableScan": ...
    def plan(self) -> Plan: ...

class RecordBatchReader:
    def __iter__(self) -> "RecordBatchReader": ...
    def __next__(self) -> pyarrow.RecordBatch: ...
    def read_next_batch(self) -> Optional[pyarrow.RecordBatch]: ...
    def close(self) -> None:
        """Stop an in-flight read and release the underlying native stream."""
        ...

class TableRead:
    def read_arrow(self, splits: Sequence[Split]) -> RecordBatchReader:
        """Lazily read splits as PyArrow RecordBatches."""
        ...
    def read(self, splits: Sequence[Split]) -> List[pyarrow.RecordBatch]: ...

class ReadBuilder:
    def with_projection(self, columns: List[str]) -> "ReadBuilder": ...
    def with_case_sensitive(self, case_sensitive: bool) -> "ReadBuilder":
        """
        Set whether column-name matching (projection and predicate column
        resolution) is case-sensitive. Defaults to ``True`` (exact match).

        Projection resolution is lazy, so this is order-independent with
        ``with_projection``. Predicates built via ``with_filter`` capture case
        sensitivity when they are constructed, so ``with_case_sensitive`` must be
        set before ``with_filter`` for the filter to honor it.
        """
        ...
    def with_limit(self, limit: int) -> "ReadBuilder":
        """Set a scan-planning hint; data-evolution reads also stop at this
        limit before resolving BLOB payloads. Other reads still need an
        application-level limit for an exact bound."""
        ...
    def with_include_row_kind(self, include: bool) -> "ReadBuilder":
        """Include a leading ``rowkind`` string column in native read results."""
        ...
    def with_blob_parallelism(self, blob_parallelism: int) -> "ReadBuilder":
        """Set the maximum number of concurrent BLOB range reads. Must be positive."""
        ...
    def with_filter(self, predicate: dict) -> "ReadBuilder": ...
    def with_row_ranges(self, ranges: Sequence[tuple[int, int]]) -> "ReadBuilder":
        """Set Data Evolution row ranges. Empty selects no rows; format tables are unsupported."""
        ...
    def new_scan(self) -> TableScan: ...
    def new_incremental_scan(
        self,
        start_snapshot_id: int,
        end_snapshot_id: int,
        mode: str = "delta",
    ) -> TableScan:
        """Plan incremental files in (start, end] as one native plan.

        ``mode`` accepts ``delta``, ``changelog`` or ``auto``. Delta reads APPEND
        manifests; changelog reads physical changelog manifests; auto follows the
        table's changelog-producer option. Diff is not representable as one
        split list and is rejected. Snapshot IDs are used, not timestamps. The
        end snapshot must exist. Row-position slicing and sharding use the
        combined delta batch as their position space.
        """
        ...
    def new_read(self) -> "TableRead": ...

# ---- #285: observability ----
class Snapshot:
    def id(self) -> int: ...
    def commit_time_ms(self) -> int: ...
    def total_record_count(self) -> Optional[int]: ...
    def delta_record_count(self) -> Optional[int]: ...
    def commit_kind(self) -> str: ...

class Tag:
    def name(self) -> str: ...
    def snapshot_id(self) -> int: ...

class PartitionStat:
    def partition(self) -> Dict[str, str]: ...
    def record_count(self) -> int: ...
    def file_count(self) -> int: ...
    def total_size_bytes(self) -> int: ...

class Table:
    @staticmethod
    def from_resolved_schema(
        location: str, schema_json: str, *, database: str = "default",
        table: str = "table", branch: str = "main",
        options: Optional[Dict[str, str]] = None,
    ) -> "Table":
        """Preserve a resolved Java-format TableSchema; options configures FileIO.

        No catalog lookup or schema time-travel resolution is performed. Snapshot
        selection still uses the supplied schema's options. Use a catalog when
        REST authorization or credential refresh is required.
        """
        ...
    def copy_with_resolved_schema(self, schema_json: str, *, branch: Optional[str] = None) -> "Table":
        """Replace all fields/options, preserving FileIO, REST context and branch."""
        ...

    def identifier(self) -> str: ...
    def branch(self) -> str: ...
    def location(self) -> str: ...
    def schema(self) -> TableSchema: ...
    def new_read_builder(self, options: Optional[Dict[str, str]] = None) -> ReadBuilder: ...
    def new_write_builder(
        self, commit_user: Optional[str] = None, *, overwrite: bool = False
    ) -> "WriteBuilder":
        """Share a commit user with writers; overwrite=True prepares overwrite writes."""
        ...
    def new_commit(self, commit_user: str) -> "TableCommit":
        """Create a committer with a stable job/attempt identity."""
        ...
    def latest_snapshot(self) -> Optional[Snapshot]:
        """
        Warning: This method blocks on a DataFusion runtime.
        Calling this from an active asyncio event loop will result in a panic.
        """
        ...
    def list_snapshots(self) -> List[Snapshot]:
        """
        Returns all snapshots ordered newest first (descending by ID).

        Warning: This method blocks on a DataFusion runtime.
        Calling this from an active asyncio event loop will result in a panic.
        """
        ...
    def list_tags(self) -> List[Tag]:
        """
        Warning: This method blocks on a DataFusion runtime.
        Calling this from an active asyncio event loop will result in a panic.
        """
        ...
    def list_partitions(self) -> List[Dict[str, str]]:
        """
        Warning: This method blocks on a DataFusion runtime.
        Calling this from an active asyncio event loop will result in a panic.
        """
        ...
    def partition_stats(self) -> List[PartitionStat]:
        """
        Warning: This method blocks on a DataFusion runtime.
        Calling this from an active asyncio event loop will result in a panic.
        """
        ...

class CommitMessage:
    def serialize(self) -> bytes:
        """Java CommitMessageSerializer v14 body, without a version header."""
        ...

class TableWrite:
    def write_arrow(self, batch: pyarrow.RecordBatch) -> None: ...
    def prepare_commit(self) -> List[CommitMessage]: ...

class TableCommit:
    def deserialize_commit_message(
        self, data: bytes, source_table_location: str, *, version: int = 14,
        overwrite: bool = False,
    ) -> CommitMessage:
        """Import an unframed Java body using trusted source table and write mode.

        Only v14 is supported. For fixed-bucket overwrite messages, overwrite=True
        restores operation context that Java does not store in the wire body.
        """
        ...
    def commit(
        self, messages: Sequence[CommitMessage], commit_identifier: Optional[int] = None
    ) -> None:
        """Commit once per identifier, increasing it monotonically per commit user.

        None uses the batch identifier. This does not filter prior identifiers;
        use filter_and_commit when retrying an uncertain result.
        """
        ...
    def filter_and_commit(
        self, messages: Sequence[CommitMessage], commit_identifier: int
    ) -> None:
        """Skip an already committed identifier before committing its messages."""
        ...
    def overwrite(
        self, messages: Sequence[CommitMessage],
        static_partitions: Optional[Dict[str, Any]] = None, *,
        commit_identifier: Optional[int] = None,
    ) -> None:
        """None replaces touched partitions; {} replaces the whole table.

        A nonempty spec replaces matching partitions, including partial specs.
        Values use the schema's Python types (int, str, date, Decimal, etc.);
        None and the configured default partition name denote null. With empty
        messages, a static spec truncates matching partitions; None is a no-op.
        An explicit identifier filters a previously committed operation on retry.
        """
        ...
    def truncate_partitions(
        self, partitions: Sequence[Dict[str, Any]], commit_identifier: Optional[int] = None
    ) -> None:
        """Truncate matching partitions using typed specs. An empty list is rejected.

        An explicit identifier filters a previously committed operation on retry.
        """
        ...
    def truncate_table(self, commit_identifier: Optional[int] = None) -> None:
        """Truncate all data, filtering retries when an identifier is supplied."""
        ...
    def abort(self, messages: Sequence[CommitMessage]) -> None:
        """Delete the files the messages refer to. Best-effort: missing files and
        storage errors are ignored. The messages must not be committed afterwards."""
        ...

class WriteBuilder:
    def deserialize_commit_message(
        self, data: bytes, source_table_location: str, *, version: int = 14,
        overwrite: bool = False,
    ) -> CommitMessage:
        """Import a Java v14 body with trusted source table and overwrite context."""
        ...
    def new_write(self) -> TableWrite: ...
    def new_commit(self) -> TableCommit: ...

class PaimonCatalog:
    def __init__(self, catalog_options: Dict[str, str]) -> None: ...
    def __datafusion_catalog_provider__(self, session: Any) -> object: ...
    def list_databases(self) -> List[str]: ...
    def list_tables(self, database_name: str) -> List[str]: ...
    def get_table(self, identifier: Union[str, Tuple[str, str]]) -> Table: ...

class PythonScalarUDF:
    def __init__(
        self,
        name: str,
        func: Callable[..., pyarrow.Array],
        input_fields: InputFieldsLike,
        return_field: ArrowTypeLike,
        volatility: VolatilityLike,
    ) -> None: ...
    @staticmethod
    def udf(
        func: Callable[..., pyarrow.Array],
        input_fields: InputFieldsLike,
        return_field: ArrowTypeLike,
        volatility: VolatilityLike,
        name: Optional[str] = None,
    ) -> "PythonScalarUDF": ...
    @property
    def name(self) -> str: ...

def udf(
    func: Callable[..., pyarrow.Array],
    input_fields: InputFieldsLike,
    return_field: ArrowTypeLike,
    volatility: VolatilityLike,
    name: Optional[str] = None,
) -> PythonScalarUDF:
    """
    Create a scalar UDF.

    This mirrors DataFusion Python's function-style API:
    ``udf(func, input_fields, return_field, volatility, name)``.
    ``input_fields`` and ``return_field`` accept PyArrow DataType or Field
    values. String type names remain accepted for compatibility.
    """
    ...

class SQLContext:
    def __init__(
        self,
        *,
        memory_pool_type: Optional[Literal["fair", "greedy"]] = None,
        memory_pool_bytes: Optional[int] = None,
        temp_directory: Optional[Union[str, PathLike[str]]] = None,
        max_temp_directory_size_bytes: Optional[int] = None,
    ) -> None: ...
    def register_catalog(
        self,
        catalog_name: str,
        catalog_options: Dict[str, str],
        default_database: Optional[str] = None,
    ) -> None:
        """Register a Paimon catalog. ``default_database``: omitted or ``None`` uses
        ``"default"``; ``""`` skips default-database init; a name uses that database."""
        ...
    def set_current_catalog(self, catalog_name: str) -> None: ...
    def set_current_database(self, database_name: str) -> None: ...
    def register_batch(self, name: str, batch: pyarrow.RecordBatch) -> None: ...
    def register_udf(self, udf: PythonScalarUDF) -> None: ...
    def sql(self, sql: str) -> List[pyarrow.RecordBatch]: ...
