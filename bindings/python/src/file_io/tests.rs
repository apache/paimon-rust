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

use super::*;
use bytes::Bytes;
use opendal::raw::oio::{Read, ReadStream, Write};
use paimon::io::FileRead;
use std::ffi::CString;

const MOCK: &str = r#"
from types import SimpleNamespace

class Output:
    def __init__(self, io, path):
        self.io = io
        self.path = path
        self.data = bytearray()
        io.events.append(('open', path))
    def write(self, data):
        count = min(len(data), self.io.write_size)
        self.data.extend(data[:count])
        self.io.events.append(('write', count))
        if self.io.write_none:
            self.data.extend(data[count:])
            return None
        return count
    def close(self):
        self.io.events.append(('close', self.path))
        self.io.files[self.path] = bytes(self.data)
        if self.io.fail_close:
            raise OSError('close failed after publication')

class IO:
    def __init__(self):
        self.files = {}
        self.events = []
        self.write_size = 3
        self.write_none = False
        self.fail_close = False
        self.fail_read = False
        self.fail_list = False
        self.fail_rename = False
    def new_output_stream(self, path):
        return Output(self, path)
    def get_file_status(self, path):
        self.events.append(('stat', path))
        if path in self.files:
            return SimpleNamespace(type=2, size=len(self.files[path]), mtime=0.125)
        if any(key.startswith(path.rstrip('/') + '/') for key in self.files):
            return SimpleNamespace(type=3, size=None, mtime=None)
        raise FileNotFoundError(path)
    def get_file_size(self, path):
        return self.get_file_status(path).size
    def read_file_range(self, path, offset, length):
        if self.fail_read:
            raise PermissionError('read denied')
        assert 0 <= length
        self.events.append(('read', path, offset, length))
        return self.files[path][offset:offset + length]
    def exists(self, path):
        return path in self.files
    def delete(self, path, recursive=False):
        self.events.append(('delete', path))
        if recursive:
            for key in list(self.files):
                if key.startswith(path.rstrip('/') + '/'):
                    del self.files[key]
            return True
        return self.files.pop(path, None) is not None
    def mkdirs(self, path):
        return True
    def is_dir(self, path):
        return self.get_file_status(path).type == 3
    def rename(self, source, target):
        self.events.append(('rename', source, target))
        if self.fail_rename:
            return False
        self.files[target] = self.files.pop(source)
        return True
    def list_status(self, path):
        if self.fail_list:
            raise PermissionError('list denied')
        self.events.append(('list', path))
        base = path.rstrip('/') + '/'
        result = {}
        for key in self.files:
            if key.startswith(base):
                suffix = key[len(base):]
                name, separator, rest = suffix.partition('/')
                result[name] = SimpleNamespace(base_name=name, type=3 if separator else 2,
                                               size=None if separator else len(self.files[key]),
                                               mtime=None)
        return list(result.values())
"#;

fn setup() -> (FileIO, Arc<Py<PyAny>>) {
    let io = Python::attach(|py| {
        let code = CString::new(MOCK).unwrap();
        PyModule::from_code(py, &code, c"file_io_test.py", c"file_io_test")
            .unwrap()
            .getattr("IO")
            .unwrap()
            .call0()
            .unwrap()
            .unbind()
    });
    let io = Arc::new(io);
    let bridge = Python::attach(|py| python_file_io(io.clone_ref(py)).unwrap());
    (bridge, io)
}

fn set(io: &Py<PyAny>, name: &str, value: bool) {
    Python::attach(|py| io.bind(py).setattr(name, value).unwrap());
}

#[tokio::test]
async fn custom_io_preserves_literal_paths_ranges_and_partial_writes() {
    let (io, mock) = setup();
    let path = "custom://bucket/table/中文 a%2Fb?x=1#y.parquet";
    let mut writer = io.new_output(path).unwrap().writer().await.unwrap();
    writer
        .write(Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    writer.write(Bytes::from_static(b"abcdef")).await.unwrap();
    writer.close().await.unwrap();
    let input = io.new_input(path).unwrap();
    assert_eq!(
        input.reader().await.unwrap().read(3..12).await.unwrap(),
        "3456789ab"
    );
    let status = input.metadata().await.unwrap();
    assert_eq!(status.size, 16);
    assert_eq!(status.last_modified.unwrap().timestamp_millis(), 125);
    assert_eq!(
        Python::attach(|py| {
            mock.bind(py)
                .getattr("files")
                .unwrap()
                .get_item(path)
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap()
        }),
        b"0123456789abcdef"
    );
    let target = "custom://bucket/table/moved.parquet";
    io.rename(path, target).await.unwrap();
    assert!(!io.exists(path).await.unwrap());
    assert_eq!(
        io.new_input(target).unwrap().read().await.unwrap(),
        "0123456789abcdef"
    );
}

#[tokio::test]
async fn custom_io_streams_large_reads_without_unbounded_python_reads() {
    let (_, mock) = setup();
    let path = "custom://bucket/payload";
    Python::attach(|py| {
        mock.bind(py)
            .getattr("files")
            .unwrap()
            .set_item(
                path,
                PyBytes::new(py, &vec![7; READ_CHUNK_SIZE as usize * 2 + 17]),
            )
            .unwrap();
    });
    let reader = PythonReader {
        io: Arc::clone(&mock),
        path: path.to_string(),
    };
    let (_, mut stream) = reader.open(BytesRange::default()).await.unwrap();
    assert_eq!(stream.read().await.unwrap().len(), READ_CHUNK_SIZE as usize);
    assert_eq!(stream.read().await.unwrap().len(), READ_CHUNK_SIZE as usize);
    assert_eq!(stream.read().await.unwrap().len(), 17);
    assert!(stream.read().await.unwrap().is_empty());
    let lengths = Python::attach(|py| {
        mock.bind(py)
            .getattr("events")
            .unwrap()
            .try_iter()
            .unwrap()
            .filter_map(|event| {
                let event = event.unwrap();
                (event.get_item(0).unwrap().extract::<String>().unwrap() == "read")
                    .then(|| event.get_item(3).unwrap().extract::<u64>().unwrap())
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(lengths, [READ_CHUNK_SIZE, READ_CHUNK_SIZE, 17]);
}

#[tokio::test]
async fn custom_io_close_failure_and_abort_never_delete_output() {
    let (io, mock) = setup();
    let path = "custom://bucket/uncertain";
    set(&mock, "fail_close", true);
    let mut writer = io.new_output(path).unwrap().writer().await.unwrap();
    writer
        .write(Bytes::from_static(b"committed-or-orphan"))
        .await
        .unwrap();
    assert!(writer.close().await.is_err());
    drop(writer);
    assert!(io.exists(path).await.unwrap());
    Python::attach(|py| {
        for event in mock.bind(py).getattr("events").unwrap().try_iter().unwrap() {
            assert_ne!(
                event
                    .unwrap()
                    .get_item(0)
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "delete"
            );
        }
    });
    let service = PythonService {
        io: Arc::clone(&mock),
        prefix: "custom://bucket/".to_string(),
    };
    let mut writer = service
        .write(&OperationContext::default(), "abort", OpWrite::default())
        .unwrap();
    writer.write(Buffer::from("payload")).await.unwrap();
    assert!(writer.abort().await.is_err());
    writer.abort().await.unwrap();
    drop(writer);
    assert!(io.exists("custom://bucket/abort").await.unwrap());
}

#[tokio::test]
async fn custom_io_supports_none_write_and_propagates_errors() {
    let (io, mock) = setup();
    set(&mock, "write_none", true);
    let path = "custom://bucket/source";
    io.new_output(path)
        .unwrap()
        .write(Bytes::from_static(b"payload"))
        .await
        .unwrap();
    assert_eq!(io.new_input(path).unwrap().read().await.unwrap(), "payload");
    set(&mock, "fail_read", true);
    let error = io.new_input(path).unwrap().read().await.unwrap_err();
    Python::attach(|py| {
        assert!(crate::error::to_py_err(error)
            .is_instance_of::<pyo3::exceptions::PyPermissionError>(py))
    });
    set(&mock, "fail_rename", true);
    assert!(io.rename(path, "custom://bucket/target").await.is_err());
    assert!(io.exists(path).await.unwrap());
    set(&mock, "fail_list", true);
    let error = io.list_status("custom://bucket/").await.unwrap_err();
    Python::attach(|py| {
        assert!(crate::error::to_py_err(error)
            .is_instance_of::<pyo3::exceptions::PyPermissionError>(py))
    });
    assert!(!io.exists("custom://bucket/missing").await.unwrap());
}

#[tokio::test]
async fn custom_io_listing_reuses_original_uri_and_recurses() {
    let (io, _) = setup();
    for path in [
        "custom://bucket/root/a%20b",
        "custom://bucket/root/sub/中文",
    ] {
        io.new_output(path)
            .unwrap()
            .write(Bytes::from_static(b"value"))
            .await
            .unwrap();
    }
    let mut statuses = io.list_status("custom://bucket/root").await.unwrap();
    statuses.sort_by(|left, right| left.path.cmp(&right.path));
    assert_eq!(
        statuses.iter().map(|s| s.path.as_str()).collect::<Vec<_>>(),
        ["custom://bucket/root/a%20b", "custom://bucket/root/sub/"]
    );
    let statuses = io
        .list_status_recursive("custom://bucket/root")
        .await
        .unwrap();
    assert!(statuses
        .iter()
        .any(|s| s.path == "custom://bucket/root/sub/中文"));
    io.delete_dir("custom://bucket/root").await.unwrap();
    assert!(!io.exists("custom://bucket/root/sub/中文").await.unwrap());
}

#[tokio::test]
async fn bounded_reads_do_not_stat_and_short_reads_fail() {
    let (_, mock) = setup();
    let path = "custom://bucket/value";
    Python::attach(|py| {
        mock.bind(py)
            .getattr("files")
            .unwrap()
            .set_item(path, PyBytes::new(py, b"abc"))
            .unwrap();
    });
    let reader = PythonReader {
        io: Arc::clone(&mock),
        path: path.to_string(),
    };
    let (_, bytes) = reader.read(BytesRange::from(1..3)).await.unwrap();
    assert_eq!(bytes.to_bytes().as_ref(), b"bc");
    assert!(reader.read(BytesRange::from(1..5)).await.is_err());
    let events = Python::attach(|py| {
        mock.bind(py)
            .getattr("events")
            .unwrap()
            .try_iter()
            .unwrap()
            .map(|event| {
                event
                    .unwrap()
                    .get_item(0)
                    .unwrap()
                    .extract::<String>()
                    .unwrap()
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(events, ["read", "read"]);
}

#[tokio::test]
async fn normalization_cannot_silently_target_another_file() {
    let (io, mock) = setup();
    for path in ["custom://bucket/key ", "custom://bucket/a//b", "/tmp/file "] {
        let output = io.new_output(path).unwrap();
        assert!(output.write(Bytes::from_static(b"value")).await.is_err());
    }
    assert!(Python::attach(|py| mock.bind(py).getattr("files").unwrap().len().unwrap()) == 0);
}

#[test]
fn routing_preserves_uri_keys_and_root_operator_identity() {
    for (path, prefix, relative) in [
        (
            "s3://bucket/table/a%2Fb?x#y",
            "s3://bucket/",
            "table/a%2Fb?x#y",
        ),
        ("s3://bucket", "s3://bucket/", ""),
        ("file:/tmp/table", "file:/", "tmp/table"),
        ("/tmp/table", "/", "tmp/table"),
        ("relative/table", "", "relative/table"),
    ] {
        assert_eq!(split_path(path), (prefix.to_string(), relative.to_string()));
    }
}

#[pyfunction]
fn nested_native_read(_path: String, _offset: u64, length: usize) -> Vec<u8> {
    paimon_datafusion::runtime::runtime().block_on(async { vec![9; length] })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_io_callback_can_call_another_native_operation() {
    let (_, mock) = setup();
    Python::attach(|py| {
        mock.bind(py)
            .setattr(
                "read_file_range",
                wrap_pyfunction!(nested_native_read, py).unwrap(),
            )
            .unwrap();
    });
    let reader = PythonReader {
        io: mock,
        path: "custom://bucket/value".to_string(),
    };
    let (_, bytes) = reader.read(BytesRange::from(0..3)).await.unwrap();
    assert_eq!(bytes.to_bytes().as_ref(), [9; 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_file_io_supports_core_parquet_write_commit_plan_and_read() {
    use arrow::array::Int32Array;
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use paimon::catalog::Identifier;
    use paimon::spec::{DataType, IntType, Schema, TableSchema};

    let (io, mock) = setup();
    Python::attach(|py| {
        mock.bind(py)
            .setattr("write_size", READ_CHUNK_SIZE)
            .unwrap()
    });
    let location = "custom://bucket/table";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("bucket", "-1")
        .option("file.format", "parquet")
        .option("manifest.compression", "xz")
        .build()
        .unwrap();
    let schema = TableSchema::new(0, &schema);
    io.new_output(&format!("{location}/schema/schema-0"))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
        .await
        .unwrap();
    let table = paimon::table::Table::from_resolved_schema(
        io,
        Identifier::new("default", "table"),
        location.to_string(),
        schema,
        "main",
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
        vec![Arc::new(Int32Array::from(vec![7, 11, 19]))],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert!(!messages.is_empty());
    builder
        .try_new_commit()
        .unwrap()
        .commit(messages)
        .await
        .unwrap();
    writer.close().await;

    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    assert!(!plan.splits().is_empty());
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, [7, 11, 19]);
    Python::attach(|py| {
        let paths = mock.bind(py).getattr("files").unwrap();
        assert!(paths.try_iter().unwrap().any(|path| path
            .unwrap()
            .extract::<String>()
            .unwrap()
            .ends_with(".parquet")));
        assert!(paths
            .contains(format!("{location}/snapshot/snapshot-1"))
            .unwrap());
    });
}
