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

import base64
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon_rust import datafusion
from pypaimon_rust.datafusion import CommitMessage, PaimonCatalog, SQLContext


def _table(path, partitioned=False, options=None, primary_key=False):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(path)})
    ctx.sql("CREATE SCHEMA paimon.db")
    ddl = "CREATE TABLE paimon.db.t (id INT, pt INT"
    ddl += ", PRIMARY KEY (id))" if primary_key else ")"
    if partitioned:
        ddl += " PARTITIONED BY (pt)"
    if options:
        ddl += " WITH (" + ", ".join(f"'{k}' = '{v}'" for k, v in options.items()) + ")"
    ctx.sql(ddl)
    return PaimonCatalog({"warehouse": str(path)}).get_table("db.t")


def _write(writer, ids, partitions):
    writer.write_arrow(pa.record_batch(
        [ids, partitions], schema=pa.schema([("id", pa.int32()), ("pt", pa.int32())])
    ))


def _prepare(builder, ids, partitions, identifier=None):
    writer = builder.new_write()
    _write(writer, ids, partitions)
    return writer.prepare_commit() if identifier is None else writer.prepare_commit(True, identifier)


def _append(table, ids, partitions):
    builder = table.new_batch_write_builder()
    builder.new_commit().commit(_prepare(builder, ids, partitions))


def _roundtrip(messages):
    return [CommitMessage.deserialize(message.serialize()) for message in messages]


def _rows(table):
    reader = table.new_read_builder()
    batches = reader.new_read().read(reader.new_scan().plan().splits())
    return sorted(row["id"] for batch in batches for row in batch.to_pylist())


def _snapshot(table):
    return json.loads((Path(table.location()) / "snapshot" /
                       f"snapshot-{table.latest_snapshot().id()}").read_text())


def test_public_api_separates_batch_and_stream(tmp_path):
    table = _table(tmp_path)
    assert not hasattr(table, "new_commit")
    assert not hasattr(table, "new_write_builder")
    for name in ("WriteBuilder", "TableWrite", "TableCommit"):
        assert not hasattr(datafusion, name)
    for factory in (table.new_batch_write_builder, table.new_stream_write_builder):
        for kwargs in ({"commit_user": "job"}, {"overwrite": True}):
            with pytest.raises(TypeError):
                factory(**kwargs)
    batch = table.new_batch_write_builder()
    stream = table.new_stream_write_builder()
    assert batch.with_commit_user("batch-job") is batch
    assert not hasattr(stream, "with_overwrite")
    for obj in (batch, stream, batch.new_commit(), stream.new_commit()):
        assert not hasattr(obj, "deserialize_commit_message")
    assert not hasattr(batch.new_commit(), "filter_and_commit")
    assert not hasattr(batch.new_commit(), "overwrite")
    for method in ("overwrite", "_overwrite", "truncate_table"):
        assert not hasattr(stream.new_commit(), method)
    with pytest.raises(TypeError):
        batch.new_commit().commit([], commit_identifier=1)
    with pytest.raises(TypeError):
        stream.new_commit().commit([])


def test_serialized_stream_commit_and_grouped_retry(tmp_path):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder()
    assert builder.with_commit_user("python-job") is builder
    assert builder.commit_user() == "python-job"
    writer = builder.new_write()
    commit = builder.new_commit()
    _write(writer, [1], [10])
    first = _roundtrip(writer.prepare_commit(True, 7))
    commit.commit(7, first)
    _write(writer, [2], [20])
    second = _roundtrip(writer.prepare_commit(False, 8))
    _write(writer, [3], [30])
    third = _roundtrip(writer.prepare_commit(True, 9))
    restored = table.new_stream_write_builder().with_commit_user("python-job").new_commit()
    # Input order differs from commit order; count groups after filtering.
    assert restored.filter_and_commit({9: third, 7: first, 8: second}) == 2
    assert _rows(table) == [1, 2, 3]
    snapshot = _snapshot(table)
    assert snapshot["commitUser"] == "python-job"
    assert snapshot["commitIdentifier"] == 9
    assert restored.filter_and_commit({7: first, 8: second, 9: third}) == 0
    assert table.latest_snapshot().id() == snapshot["id"]
    assert restored.filter_and_commit({}) == 0


@pytest.mark.parametrize("user", ["", "../job", "a/b"])
def test_invalid_stream_commit_user(tmp_path, user):
    builder = _table(tmp_path).new_stream_write_builder()
    original = builder.commit_user()
    with pytest.raises(ValueError):
        builder.with_commit_user(user)
    assert builder.commit_user() == original


def test_static_deserialize_checks_version_and_payload():
    # Java CommitMessageSerializer v14 fixture; no table or builder is needed.
    body = base64.b64decode(
        "AAAADAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABw=="
    )
    with pytest.raises(NotImplementedError, match="version"):
        CommitMessage.deserialize(body, version=13)
    for invalid in (b"", body[:-1], body + b"extra"):
        with pytest.raises(ValueError):
            CommitMessage.deserialize(invalid)
    assert CommitMessage.deserialize(body).serialize() == body
    assert CommitMessage.deserialize(body, version=14).serialize() == body


def test_deserialized_batch_commit_uses_target_builder(tmp_path):
    table = _table(tmp_path)
    messages = _roundtrip(_prepare(table.new_batch_write_builder(), [1], [10]))
    table.new_batch_write_builder().new_commit().commit(messages)
    assert _rows(table) == [1]


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("properties", [None, {}, {"source": "capture", "label": "中文"}])
def test_batch_snapshot_properties(tmp_path, overwrite, properties):
    table = _table(tmp_path)
    _append(table, [1], [10])
    builder = table.new_batch_write_builder()
    if overwrite:
        builder.with_overwrite()
    messages = _roundtrip(_prepare(builder, [2], [20]))
    committer = builder.new_commit()
    committer.commit(messages, snapshot_properties=properties)
    snapshot = _snapshot(table)
    assert snapshot.get("properties") == (properties or None)
    assert snapshot["commitKind"] == ("OVERWRITE" if overwrite else "APPEND")
    assert _rows(table) == ([2] if overwrite else [1, 2])
    with pytest.raises(RuntimeError, match="one-time"):
        committer.commit(messages, snapshot_properties={"source": "retry"})
    assert _snapshot(table)["id"] == snapshot["id"]


def test_stream_snapshot_properties_are_per_checkpoint(tmp_path):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder().with_commit_user("properties-job")
    committer = builder.new_commit()
    properties = {"checkpoint": "first"}
    for identifier, metadata in enumerate([properties, None, {}, {"checkpoint": "fourth"}], 1):
        messages = _prepare(builder, [identifier], [identifier * 10], identifier)
        committer.commit(identifier, messages, snapshot_properties=metadata)
        snapshot = _snapshot(table)
        assert snapshot["commitIdentifier"] == identifier
        assert snapshot.get("properties") == (metadata or None)
        if not metadata:
            assert "properties" not in snapshot
    properties["checkpoint"] = "changed"
    first = json.loads((Path(table.location()) / "snapshot" / "snapshot-1").read_text())
    assert first["properties"] == {"checkpoint": "first"}
    assert _rows(table) == [1, 2, 3, 4]


def test_empty_stream_snapshot_properties(tmp_path):
    table = _table(tmp_path)
    committer = table.new_stream_write_builder().with_commit_user("empty-job").new_commit()
    committer.commit(42, [], snapshot_properties={"source": "empty"})
    snapshot = _snapshot(table)
    assert snapshot["properties"] == {"source": "empty"}
    assert snapshot["totalRecordCount"] == 0
    assert snapshot["deltaRecordCount"] == 0
    committer.commit(43, [])
    assert "properties" not in _snapshot(table)


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("serialized", [False, True])
def test_public_batch_commit_user_survives_serialization(tmp_path, overwrite, serialized):
    # Inspect the data commit's identity rather than a subsequent COMPACT.
    table = _table(tmp_path, primary_key=True, options={"bucket": "1", "write-only": "true"})
    _append(table, [1], [10])
    builder = table.new_batch_write_builder()
    assert builder.with_commit_user("python-batch-job") is builder
    if overwrite:
        builder.with_overwrite()
    messages = _prepare(builder, [2], [20])
    if serialized:
        messages = _roundtrip(messages)
    builder.new_commit().commit(messages)
    assert _rows(table) == ([2] if overwrite else [1, 2])
    snapshot = _snapshot(table)
    assert snapshot["commitUser"] == "python-batch-job"
    assert snapshot["commitIdentifier"] == 2**63 - 1


@pytest.mark.parametrize("user", ["", "../job", "a/b"])
def test_invalid_public_batch_commit_user_preserves_identity(tmp_path, user):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder().with_commit_user("python-batch-job")
    with pytest.raises(ValueError):
        builder.with_commit_user(user)
    builder.new_commit().commit(_prepare(builder, [1], [10]))
    assert _snapshot(table)["commitUser"] == "python-batch-job"
    assert _rows(table) == [1]


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('primary_key', [False, True])
@pytest.mark.parametrize('serialized', [False, True])
def test_explicit_abort_deletes_known_uncommitted_files(tmp_path, stream, primary_key, serialized):
    table = _table(tmp_path, primary_key=primary_key, options={'bucket': '1'} if primary_key else None)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    messages = _prepare(builder, [1], [10], 7 if stream else None)
    if serialized:
        messages = _roundtrip(messages)
    files = list(tmp_path.rglob("data-*.parquet"))
    assert files
    assert table.latest_snapshot() is None
    commit = builder.new_commit()
    # These messages have never been submitted and will not be committed.
    commit.abort(messages)
    commit.abort(messages)
    assert not any(file.exists() for file in files)
    assert table.latest_snapshot() is None
    assert _rows(table) == []


def test_batch_writer_and_commit_are_one_shot(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    _write(writer, [1], [10])
    messages = writer.prepare_commit()
    with pytest.raises(RuntimeError, match="one-time"):
        writer.prepare_commit()
    commit = builder.new_commit()
    commit.commit(messages)
    with pytest.raises(RuntimeError, match="one-time"):
        commit.commit(messages)
    with pytest.raises(RuntimeError, match="one-time"):
        commit.truncate_table()
    assert _rows(table) == [1]
    truncate = builder.new_commit()
    truncate.truncate_table()
    with pytest.raises(RuntimeError, match="one-time"):
        truncate.commit([])
    assert _rows(table) == []


@pytest.mark.parametrize("ignore", ["true", "false", "False"])
def test_batch_empty_commit_honors_option(tmp_path, ignore):
    table = _table(tmp_path, options={"snapshot.ignore-empty-commit": ignore})
    commit = table.new_batch_write_builder().new_commit()
    commit.commit([])
    assert (table.latest_snapshot() is None) == (ignore.lower() == "true")
    with pytest.raises(RuntimeError, match="one-time"):
        commit.commit([])


def test_stream_empty_checkpoint_is_recorded(tmp_path):
    table = _table(tmp_path, options={"snapshot.ignore-empty-commit": "true"})
    commit = table.new_stream_write_builder().with_commit_user("job").new_commit()
    commit.commit(1, [])
    assert _snapshot(table)["commitIdentifier"] == 1
    assert commit.filter_and_commit({3: [], 1: [], 2: []}) == 2
    assert _snapshot(table)["commitIdentifier"] == 3
    assert table.latest_snapshot().id() == 3
    assert _rows(table) == []


@pytest.mark.parametrize("spec", [{}, {"pt": 999}])
def test_default_dynamic_overwrite_uses_touched_partitions(tmp_path, spec):
    table = _table(tmp_path, partitioned=True)
    _append(table, [1, 2], [10, 20])
    builder = table.new_batch_write_builder()
    assert builder.with_overwrite(spec) is builder
    builder.new_commit().commit(_roundtrip(_prepare(builder, [3], [10])))
    assert _rows(table) == [2, 3]
    table.new_batch_write_builder().with_overwrite().new_commit().commit([])
    assert _rows(table) == [2, 3]


@pytest.mark.parametrize("null_value", [None, "__DEFAULT_PARTITION__"])
def test_static_overwrite_and_empty_truncation(tmp_path, null_value):
    table = _table(tmp_path, partitioned=True, options={"dynamic-partition-overwrite": "FALSE"})
    _append(table, [1, 2, 3], [10, 20, None])
    builder = table.new_batch_write_builder().with_overwrite({"pt": 10})
    builder.new_commit().commit(_prepare(builder, [4], [10]))
    assert _rows(table) == [2, 3, 4]
    table.new_batch_write_builder().with_overwrite({"pt": null_value}).new_commit().commit([])
    assert _rows(table) == [2, 4]
    table.new_batch_write_builder().with_overwrite().new_commit().commit([])
    assert _rows(table) == []


def test_unpartitioned_empty_overwrite_truncates(tmp_path):
    table = _table(tmp_path)
    _append(table, [1], [10])
    table.new_batch_write_builder().with_overwrite().new_commit().commit([])
    assert _rows(table) == []


def test_explicit_none_disables_overwrite_and_context_is_copied(tmp_path):
    table = _table(tmp_path)
    _append(table, [1], [10])
    builder = table.new_batch_write_builder().with_overwrite()
    overwrite = builder.new_commit()
    builder.with_overwrite(None)
    builder.new_commit().commit(_prepare(builder, [2], [20]))
    assert _rows(table) == [1, 2]
    overwrite.commit([])
    assert _rows(table) == []


def test_static_deserialize_uses_committer_overwrite_mode(tmp_path):
    table = _table(tmp_path)
    _append(table, [1], [10])
    builder = table.new_batch_write_builder().with_overwrite()
    body = _prepare(builder, [2], [20])[0].serialize()
    # Model an external message carrying Java totalBuckets.
    flag_offset = 4 + int.from_bytes(body[:4], "big") + 4
    assert body[flag_offset] == 0
    body = body[:flag_offset] + bytes([1]) + (1).to_bytes(4, "big") + body[flag_offset + 1:]
    commit = builder.new_commit()
    message = CommitMessage.deserialize(body)
    assert message.serialize() == body
    commit.commit([message])
    assert _rows(table) == [2]


def test_overwrite_does_not_mutate_deserialized_message(tmp_path):
    table = _table(tmp_path, partitioned=True, options={"dynamic-partition-overwrite": "false"})
    _append(table, [1], [10])
    body = _prepare(table.new_batch_write_builder(), [2], [20])[0].serialize()
    flag_offset = 4 + int.from_bytes(body[:4], "big") + 4
    assert body[flag_offset] == 0
    body = body[:flag_offset] + bytes([1]) + (1).to_bytes(4, "big") + body[flag_offset + 1:]
    message = CommitMessage.deserialize(body)
    overwrite = table.new_batch_write_builder().with_overwrite({"pt": 10}).new_commit()
    with pytest.raises(ValueError, match="does not belong"):
        overwrite.commit([message])
    # A failed overwrite must not stamp the object with its operation mode.
    table.new_batch_write_builder().new_commit().commit([message])
    assert _rows(table) == [1, 2]


def test_failed_batch_commit_consumes_instance(tmp_path):
    table = _table(tmp_path, partitioned=True, options={"dynamic-partition-overwrite": "FALSE"})
    _append(table, [1], [10])
    builder = table.new_batch_write_builder().with_overwrite({"pt": 20})
    messages = _prepare(builder, [2], [10])
    commit = builder.new_commit()
    with pytest.raises(ValueError, match="does not belong"):
        commit.commit(messages)
    with pytest.raises(RuntimeError, match="one-time"):
        commit.commit(messages)
    assert _rows(table) == [1]
    assert len(list(tmp_path.rglob("data-*.parquet"))) == 2


def test_partition_truncation_matches_java_lifecycle(tmp_path):
    table = _table(tmp_path, partitioned=True)
    _append(table, [1, 2], [10, 20])
    commit = table.new_batch_write_builder().new_commit()
    with pytest.raises(ValueError, match="Partitions list cannot be empty"):
        commit.truncate_partitions([])
    commit.truncate_partitions([{"pt": 10}])
    assert _rows(table) == [2]
    # Java truncatePartitions does not consume the batch commit guard.
    commit.commit([])
    commit.truncate_partitions([{"pt": 20}])
    assert _rows(table) == []


@pytest.mark.parametrize("overwrite", [False, True])
def test_decimal_partition_precision_overflow_matches_java_null_spec(tmp_path, overwrite):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE SCHEMA paimon.db")
    ctx.sql("""CREATE TABLE paimon.db.t (id INT, pt DECIMAL(3, 0)) PARTITIONED BY (pt)
        WITH ('dynamic-partition-overwrite' = 'false')""")
    ctx.sql("INSERT INTO paimon.db.t (id, pt) VALUES (1, NULL), (2, 12)")
    table = PaimonCatalog({"warehouse": str(tmp_path)}).get_table("db.t")
    if overwrite:
        table.new_batch_write_builder().with_overwrite({"pt": "1000"}).new_commit().commit([])
    else:
        table.new_batch_write_builder().new_commit().truncate_partitions([{"pt": "1000"}])
    assert _rows(table) == [2]


def test_integer_partition_spec_accepts_java_unicode_digits(tmp_path):
    table = _table(tmp_path, partitioned=True)
    _append(table, [1, 2], [12, 13])
    table.new_batch_write_builder().new_commit().truncate_partitions([{"pt": "１２"}])
    assert _rows(table) == [2]


@pytest.mark.parametrize("spec", [{"id": 1}, {"pt": "bad"}, {"pt": True}])
def test_partition_spec_validation_precedes_mutation(tmp_path, spec):
    table = _table(tmp_path, partitioned=True)
    _append(table, [1], [10])
    snapshot_id = table.latest_snapshot().id()
    with pytest.raises(ValueError):
        table.new_batch_write_builder().with_overwrite(spec)
    with pytest.raises(ValueError):
        table.new_batch_write_builder().new_commit().truncate_partitions([spec])
    assert table.latest_snapshot().id() == snapshot_id
    assert _rows(table) == [1]


def test_recovery_checks_all_pending_files_before_any_commit(tmp_path):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder().with_commit_user("job")
    first = _prepare(builder, [1], [10], 1)
    before = set(tmp_path.rglob("data-*.parquet"))
    second = _prepare(builder, [2], [20], 2)
    for path in set(tmp_path.rglob("data-*.parquet")) - before:
        path.unlink()
    with pytest.raises(ValueError, match="does not exist"):
        builder.new_commit().filter_and_commit({1: first, 2: second})
    assert table.latest_snapshot() is None


def test_recovery_skips_file_check_for_committed_checkpoints(tmp_path):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder().with_commit_user("job")
    messages = _prepare(builder, [1], [10], 1)
    commit = builder.new_commit()
    commit.commit(1, messages)
    # Model retention after a later overwrite: a filtered checkpoint can reference expired files.
    for path in tmp_path.rglob("data-*.parquet"):
        path.unlink()
    assert commit.filter_and_commit({1: messages}) == 0
    assert table.latest_snapshot().id() == 1


@pytest.mark.parametrize("operation", ["overwrite", "truncate"])
def test_empty_destructive_batch_operation_records_snapshot(tmp_path, operation):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    if operation == "overwrite":
        builder.with_overwrite().new_commit().commit([])
    else:
        builder.new_commit().truncate_table()
    assert _snapshot(table)["commitKind"] == "OVERWRITE"
    assert _rows(table) == []


@pytest.mark.parametrize("mode", ["batch", "stream"])
def test_close_writer_preserves_prepared_files(tmp_path, mode):
    table = _table(tmp_path)
    builder = getattr(table, f"new_{mode}_write_builder")()
    writer = builder.new_write()
    _write(writer, [1], [10])
    messages = writer.prepare_commit() if mode == "batch" else writer.prepare_commit(True, 1)
    writer.close()
    writer.close()
    with pytest.raises(RuntimeError, match="closed"):
        _write(writer, [2], [20])
    commit = builder.new_commit()
    if mode == "batch":
        commit.commit(messages)
    else:
        commit.commit(1, messages)
    commit.close()
    assert _rows(table) == [1]


def test_static_overwrite_accepts_java_numeric_partition_strings(tmp_path):
    table = _table(tmp_path, partitioned=True, options={"dynamic-partition-overwrite": "false"})
    _append(table, [1, 2], [10, 20])
    table.new_batch_write_builder().with_overwrite({"pt": "10"}).new_commit().commit([])
    assert _rows(table) == [2]


@pytest.mark.parametrize("bucket", [None, "1", "-2"])
def test_close_cleans_unprepared_output_but_preserves_prepared_files(tmp_path, bucket):
    options = {"target-file-size": "1 b", "write.parquet-buffer-size": "1 b"}
    if bucket is not None:
        options["bucket"] = bucket
    table = _table(tmp_path, options=options, primary_key=bucket is not None)
    builder = table.new_stream_write_builder()
    writer = builder.new_write()
    _write(writer, [1], [10])
    messages = writer.prepare_commit(True, 1)
    prepared_paths = set(tmp_path.rglob("*.parquet"))
    assert prepared_paths
    _write(writer, [2], [20])
    _write(writer, [3], [30])
    # Rolled files may still be closing in the background. close() must await
    # that work and clean outstanding output before we inspect the directory.
    writer.close()
    assert set(tmp_path.rglob("*.parquet")) == prepared_paths
    builder.new_commit().commit(1, messages)
    assert _snapshot(table)["totalRecordCount"] == 1
    assert [row["id"] for path in prepared_paths
            for row in pq.ParquetFile(path).read(columns=["id"]).to_pylist()] == [1]
    if bucket != "-2":
        assert _rows(table) == [1]


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('close', [False, True])
@pytest.mark.parametrize('bound_method', [False, True])
def test_commit_callback_owner_cycle_is_collected(tmp_path, stream, close, bound_method):
    import gc
    import weakref

    table = _table(tmp_path)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    commit = builder.new_commit()

    class Owner:
        def __init__(self, committer):
            self.committer = committer

        def __call__(self, context):
            pass

    owner = Owner(commit)
    reference = weakref.ref(owner)
    callback = owner.__call__ if bound_method else owner
    commit.add_commit_callback(callback)
    # Register more than one reference so GC traversal must count each edge.
    commit.add_commit_callback(callback)
    if close:
        commit.close()
    del callback, owner, commit
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize('stream', [False, True])
def test_commit_callback_bridge_preserves_error_and_publication(tmp_path, stream):
    table = _table(tmp_path)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    commit = builder.new_commit()
    seen = []
    error = LookupError('notification failed')

    def callback(context):
        assert isinstance(context, datafusion.CommitCallbackContext)
        assert context.snapshot.id() == 1
        assert context.identifier == (7 if stream else (2 ** 63 - 1))
        assert len(context.commit_entries) == 1
        entry = context.commit_entries[0]
        assert isinstance(entry, datafusion.ManifestEntry)
        assert entry.kind == 0
        assert entry.total_buckets == -1
        assert entry.bucket == 0
        assert isinstance(entry.file, datafusion.DataFileMeta)
        assert entry.file.row_count == 1
        assert entry.file.file_name.endswith('.parquet')
        assert _rows(table) == [1]  # Re-enter the Native runtime during notification.
        seen.append(context)
        raise error

    commit.add_commit_callback(callback)
    messages = _prepare(builder, [1], [10], 7 if stream else None)
    with pytest.raises(LookupError) as raised:
        commit.commit(7, messages) if stream else commit.commit(messages)
    assert raised.value is error
    assert len(seen) == 1
    assert _rows(table) == [1]
    assert table.latest_snapshot().id() == 1
    commit.close()


def test_commit_callback_rejects_non_callable_before_publication(tmp_path):
    table = _table(tmp_path)
    commit = table.new_batch_write_builder().new_commit()
    with pytest.raises(TypeError, match='must be callable'):
        commit.add_commit_callback(42)
    assert table.latest_snapshot() is None


@pytest.mark.parametrize('stream', [False, True])
def test_public_commit_callback_retains_typed_core_context(tmp_path, stream):
    import gc

    table = _table(tmp_path, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    if stream:
        builder.with_commit_user('callback-user')
    commit = builder.new_commit()
    contexts = []
    commit.add_commit_callback(contexts.append)
    assert not hasattr(commit, '_add_commit_callback')
    messages = _prepare(builder, [1, 2], [10, 20], 7 if stream else None)
    if stream:
        commit.commit(7, messages, snapshot_properties={'source': 'typed-callback'})
    else:
        commit.commit(messages, snapshot_properties={'source': 'typed-callback'})
    commit.close()
    del messages, builder, commit
    gc.collect()

    assert len(contexts) == 1
    context = contexts[0]
    assert isinstance(context, datafusion.CommitCallbackContext)
    assert isinstance(context.snapshot, datafusion.Snapshot)
    assert context.snapshot.id() == 1
    assert context.snapshot.schema_id() == 0
    assert context.snapshot.commit_identifier() == context.identifier
    assert context.snapshot.next_row_id() == 2
    assert context.snapshot.properties() == {'source': 'typed-callback'}
    if stream:
        assert context.snapshot.commit_user() == 'callback-user'
    entry = context.commit_entries[0]
    file = entry.file
    assert isinstance(entry.partition, bytes)
    assert file.row_count == 2
    assert file.file_size > 0
    assert file.first_row_id == 0
    assert file.min_sequence_number == file.max_sequence_number == 1
    assert file.schema_id == 0
    assert file.file_source == 0
    assert file.external_path is None
    assert file.write_cols is None
    assert isinstance(file.value_stats, datafusion.BinaryTableStats)
    assert file.value_stats.null_counts == [0, 0]
    assert file.value_stats.min_values != file.value_stats.max_values
    assert file.creation_time is not None
    assert file.delete_row_count == 0
    # Views are read-only; returned Python lists/maps cannot mutate core metadata.
    with pytest.raises(AttributeError):
        context.identifier = 99
    with pytest.raises(AttributeError):
        entry.kind = 1
    with pytest.raises(AttributeError):
        file.row_count = 99
    context.commit_entries.clear()
    file.extra_files.append('not-a-real-file')
    context.snapshot.properties()['source'] = 'changed'
    assert len(context.commit_entries) == 1
    assert file.extra_files == []
    assert context.snapshot.properties() == {'source': 'typed-callback'}
    assert _rows(table) == [1, 2]
