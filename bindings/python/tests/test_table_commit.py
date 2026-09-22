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

import json
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def _table(path, partitioned=False, fixed_bucket=False):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(path)})
    ctx.sql("CREATE SCHEMA paimon.db")
    ddl = "CREATE TABLE paimon.db.t (id INT, pt INT"
    ddl += ", PRIMARY KEY (id)" if fixed_bucket else ""
    ddl += ") PARTITIONED BY (pt)" if partitioned else ")"
    ddl += " WITH ('bucket' = '1')" if fixed_bucket else ""
    ctx.sql(ddl)
    return PaimonCatalog({"warehouse": str(path)}).get_table("db.t")


def _prepare(table, ids, partitions, **builder_options):
    writer = table.new_write_builder(**builder_options).new_write()
    writer.write_arrow(pa.record_batch(
        [ids, partitions], schema=pa.schema([("id", pa.int32()), ("pt", pa.int32())])
    ))
    return writer.prepare_commit()


def _import(committer, table, messages, **options):
    return [committer.deserialize_commit_message(
        message.serialize(), table.location(), **options
    ) for message in messages]


def _rows(table):
    reader = table.new_read_builder()
    batches = reader.new_read().read(reader.new_scan().plan().splits())
    return sorted(row["id"] for batch in batches for row in batch.to_pylist())


def _snapshot(table):
    return json.loads((Path(table.location()) / "snapshot" /
                       f"snapshot-{table.latest_snapshot().id()}").read_text())


def test_serialized_commit_with_stable_user_and_retry(tmp_path):
    table = _table(tmp_path)
    committer = table.new_commit("python-job")
    messages = _import(committer, table, _prepare(table, [1, 2], [10, 20]))
    committer.commit(messages, commit_identifier=7)
    assert _rows(table) == [1, 2]
    snapshot = _snapshot(table)
    assert snapshot["commitUser"] == "python-job"
    assert snapshot["commitIdentifier"] == 7
    # Recreate after a process restart: retry must not append twice.
    restored = table.new_commit("python-job")
    restored.filter_and_commit(messages, 7)
    assert table.latest_snapshot().id() == snapshot["id"]
    restored.commit(_import(restored, table, _prepare(table, [3], [30])), 8)
    assert _rows(table) == [1, 2, 3]


def test_builder_and_direct_commit_share_explicit_user(tmp_path):
    table = _table(tmp_path)
    messages = _prepare(table, [1], [10], commit_user="shared-job")
    table.new_commit("shared-job").commit(messages)
    assert _rows(table) == [1]


@pytest.mark.parametrize("user", ["", "../job", "a/b"])
def test_invalid_commit_user(tmp_path, user):
    table = _table(tmp_path)
    with pytest.raises(ValueError):
        table.new_commit(user)
    with pytest.raises(ValueError):
        table.new_write_builder(commit_user=user)


def test_deserialize_checks_source_version_and_payload(tmp_path):
    table = _table(tmp_path)
    body = _prepare(table, [1], [10])[0].serialize()
    for importer in (table.new_commit("job"), table.new_write_builder()):
        with pytest.raises(ValueError, match="source table"):
            importer.deserialize_commit_message(body, table.location() + "-other")
        with pytest.raises((ValueError, NotImplementedError), match="version"):
            importer.deserialize_commit_message(body, table.location(), version=13)
        with pytest.raises(ValueError):
            importer.deserialize_commit_message(body[:-1], table.location())
        assert importer.deserialize_commit_message(body, table.location()).serialize() == body
    assert table.latest_snapshot() is None


def test_abort_serialized_messages_deletes_files(tmp_path):
    table = _table(tmp_path)
    committer = table.new_commit("job")
    messages = _import(committer, table, _prepare(table, [1], [10]))
    files = list(tmp_path.rglob("data-*.parquet"))
    assert files
    committer.abort(messages)
    assert not any(file.exists() for file in files)
    assert table.latest_snapshot() is None


@pytest.mark.parametrize("method", ["commit", "filter_and_commit", "overwrite", "abort"])
def test_commit_operations_validate_messages(tmp_path, method):
    table = _table(tmp_path)
    messages = _prepare(table, [1], [10])
    operation = getattr(table.new_commit("another-job"), method)
    args = (1,) if method == "filter_and_commit" else ()
    with pytest.raises(ValueError, match="commit_user"):
        operation(messages, *args)
    with pytest.raises(TypeError):
        operation([b"not a CommitMessage"], *args)
    assert table.latest_snapshot() is None


@pytest.mark.parametrize("null_value", [None, "__DEFAULT_PARTITION__"])
def test_dynamic_overwrite_and_static_empty_overwrite(tmp_path, null_value):
    table = _table(tmp_path, partitioned=True)
    commit = table.new_commit("job")
    commit.commit(_import(commit, table, _prepare(table, [1, 2, 3], [10, 20, None])))
    commit.overwrite(_import(commit, table, _prepare(table, [4], [10])))
    assert _rows(table) == [2, 3, 4]
    commit.overwrite([], {"pt": null_value})
    assert _rows(table) == [2, 4]
    commit.overwrite([], {"pt": 10})
    assert _rows(table) == [2]
    commit.overwrite([], {})
    assert _rows(table) == []


def test_fixed_bucket_overwrite_import_requires_context(tmp_path):
    table = _table(tmp_path, fixed_bucket=True)
    commit = table.new_commit("job")
    commit.commit(_import(commit, table, _prepare(table, [1], [10])))
    body = _prepare(table, [2], [20], overwrite=True)[0].serialize()
    # Ordinary fixed-bucket writers omit totalBuckets. Model an external
    # fixed-bucket message carrying it, as a postpone fixed-bucket writer does.
    flag_offset = 4 + int.from_bytes(body[:4], "big") + 4
    assert body[flag_offset] == 0
    body = body[:flag_offset] + bytes([1]) + (1).to_bytes(4, "big") + body[flag_offset + 1:]
    with pytest.raises(ValueError, match="submitted as overwrite"):
        commit.overwrite([commit.deserialize_commit_message(body, table.location())])
    messages = [commit.deserialize_commit_message(body, table.location(), overwrite=True)]
    with pytest.raises(ValueError, match="submitted as append"):
        commit.commit(messages)
    commit.overwrite(messages)
    assert _rows(table) == [2]


def test_overwrite_identifier_retry_preserves_later_data(tmp_path):
    table = _table(tmp_path, partitioned=True)
    commit = table.new_commit("job")
    commit.commit(_import(commit, table, _prepare(table, [1], [10])), 1)
    replacement = _import(commit, table, _prepare(table, [2], [10]))
    commit.overwrite(replacement, {"pt": 10}, commit_identifier=2)
    other = table.new_commit("other")
    other.commit(_import(other, table, _prepare(table, [3], [10])))
    snapshot_id = table.latest_snapshot().id()
    table.new_commit("job").overwrite(replacement, {"pt": 10}, commit_identifier=2)
    assert table.latest_snapshot().id() == snapshot_id
    assert _rows(table) == [2, 3]


@pytest.mark.parametrize("operation", ["truncate_partitions", "truncate_table"])
def test_truncate_identifier_retry_preserves_later_data(tmp_path, operation):
    table = _table(tmp_path, partitioned=True)
    commit = table.new_commit("job")
    commit.commit(_import(commit, table, _prepare(table, [1, 2], [10, 20])), 1)
    args = ([{"pt": 10}],) if operation == "truncate_partitions" else ()
    getattr(commit, operation)(*args, commit_identifier=2)
    assert _rows(table) == ([2] if args else [])
    other = table.new_commit("other")
    other.commit(_import(other, table, _prepare(table, [3], [10])))
    snapshot_id = table.latest_snapshot().id()
    getattr(table.new_commit("job"), operation)(*args, commit_identifier=2)
    assert table.latest_snapshot().id() == snapshot_id
    assert _rows(table) == ([2, 3] if args else [3])


@pytest.mark.parametrize("spec", [{"id": 1}, {"pt": "10"}, {"pt": True}])
def test_partition_spec_validation_precedes_mutation(tmp_path, spec):
    table = _table(tmp_path, partitioned=True)
    commit = table.new_commit("job")
    commit.commit(_import(commit, table, _prepare(table, [1], [10])))
    snapshot_id = table.latest_snapshot().id()
    with pytest.raises(ValueError):
        commit.overwrite([], spec)
    with pytest.raises(ValueError):
        commit.truncate_partitions([spec])
    assert table.latest_snapshot().id() == snapshot_id
    assert _rows(table) == [1]


@pytest.mark.parametrize("identifier", [None, 1])
def test_truncate_partitions_rejects_empty_specs(tmp_path, identifier):
    table = _table(tmp_path, partitioned=True)
    with pytest.raises(ValueError, match="Partitions list cannot be empty"):
        table.new_commit("job").truncate_partitions([], commit_identifier=identifier)
    assert table.latest_snapshot() is None
