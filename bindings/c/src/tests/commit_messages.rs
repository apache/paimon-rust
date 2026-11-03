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
use base64::Engine;

// Java CommitMessageSerializer v14 fixtures also used by the core codec tests:
// an empty increment, an index increment, and an index with DV/global metadata.
const JAVA_BODIES: [&str; 3] = [
    "AAAADAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABw==",
    "AAAADAAAAAAAAAAAAAAAAAAAAAMBAAAABQAAAAAAAAAAAAAAAAAAAAEAAABAAHAAAAAAAABJAAAAAAAAgWluZGV4AACFCQAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABw==",
    "AAAADAAAAAAAAAAAAAAAAAAAAAMBAAAABQAAAAAAAAAAAAAAAAAAAAEAAADgAAAAAAAAAABEVgAAAAAAgmluZGV4AACFCQAAAAAAAAACAAAAAAAAAEgAAABAAAAACwAAAIgAAABIAAAAmAAAAAEAAAAAAAAAOAAAABAAAAAAAAAAAAAAAAwAAAAoAAAAAwAAAAAAAAAMAAAAAAAAAAIAAAAAAAAAZGF0YS5wYXJxdWV0AAAAAGZpbGU6L2luZGV4AAAAAAAAAAAAAAAAAAoAAAAAAAAAFAAAAAAAAAADAAAAAAAAABAAAAA4AAAAAQIAAAAAAIIDAAAAAAAAgQIAAAAAAAAAAQAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAc=",
];

fn java_bodies() -> Vec<Vec<u8>> {
    JAVA_BODIES
        .iter()
        .map(|body| {
            base64::engine::general_purpose::STANDARD
                .decode(body)
                .unwrap()
        })
        .collect()
}

fn slices(bodies: &[Vec<u8>]) -> Vec<paimon_byte_slice> {
    bodies
        .iter()
        .map(|body| paimon_byte_slice {
            data: body.as_ptr(),
            len: body.len(),
        })
        .collect()
}

unsafe fn assert_ok(error: *mut paimon_error) {
    if !error.is_null() {
        let message = String::from_utf8_lossy(std::slice::from_raw_parts(
            (*error).message.data,
            (*error).message.len,
        ))
        .into_owned();
        paimon_error_free(error);
        panic!("{message}");
    }
}

unsafe fn assert_error(error: *mut paimon_error, code: PaimonErrorCode) {
    assert!(!error.is_null());
    let actual = (*error).code;
    paimon_error_free(error);
    assert_eq!(actual, code as i32);
}

unsafe fn new_committer(
    table: *const paimon_table,
    user: &str,
    overwrite: bool,
) -> *mut paimon_table_commit {
    let user = CString::new(user).unwrap();
    let builder = paimon_table_new_write_builder_with_commit_user(table, user.as_ptr());
    assert_ok(builder.error);
    if overwrite {
        assert_ok(paimon_write_builder_with_overwrite(builder.write_builder));
    }
    let result = paimon_write_builder_new_commit(builder.write_builder);
    paimon_write_builder_free(builder.write_builder);
    assert_ok(result.error);
    result.commit
}

unsafe fn prepare(
    table: *const paimon_table,
    user: &str,
    overwrite: bool,
    batch: RecordBatch,
) -> *mut paimon_commit_messages {
    let user = CString::new(user).unwrap();
    let builder = paimon_table_new_write_builder_with_commit_user(table, user.as_ptr());
    assert_ok(builder.error);
    if overwrite {
        assert_ok(paimon_write_builder_with_overwrite(builder.write_builder));
    }
    let writer = paimon_write_builder_new_write(builder.write_builder);
    assert_ok(writer.error);
    let (array, schema) = export_batch_to_ffi(batch);
    assert_ok(paimon_table_write_write_arrow_batch(
        writer.write,
        (&**array) as *const FFI_ArrowArray as *mut c_void,
        (&**schema) as *const FFI_ArrowSchema as *mut c_void,
    ));
    let result = paimon_table_write_prepare_commit(writer.write);
    assert_ok(result.error);
    paimon_table_write_free(writer.write);
    paimon_write_builder_free(builder.write_builder);
    result.messages
}

unsafe fn copy_bodies(array: &paimon_bytes_array) -> Vec<Vec<u8>> {
    (0..array.len)
        .map(|index| {
            let bytes = &*array.data.add(index);
            std::slice::from_raw_parts(bytes.data, bytes.len).to_vec()
        })
        .collect()
}

unsafe fn import(
    commit: *const paimon_table_commit,
    bodies: &[Vec<u8>],
) -> *mut paimon_commit_messages {
    let slices = slices(bodies);
    let result = paimon_commit_messages_deserialize(
        commit,
        paimon_commit_messages_serializer_version(),
        slices.as_ptr(),
        slices.len(),
    );
    assert_ok(result.error);
    assert!(!result.messages.is_null());
    result.messages
}

unsafe fn memory_table(path: &str) -> *mut paimon_table {
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    wrap_table(Table::new(
        file_io,
        Identifier::new("default", "messages"),
        path.into(),
        simple_table_schema(),
        None,
    ))
}

#[test]
fn java_v14_round_trip_owns_buffers_and_context() {
    assert_eq!(paimon_commit_messages_serializer_version(), 14);
    unsafe {
        let table = memory_table("memory:/java_commit_messages");
        for overwrite in [false, true] {
            let commit = new_committer(table, "coordinator", overwrite);
            let bodies = java_bodies();
            let messages = import(commit, &bodies);
            drop(bodies);
            paimon_table_commit_free(commit);

            let state = &*((*messages).inner as *const CommitMessagesState);
            assert_eq!(state.commit_user, "coordinator");
            assert_eq!(state.table_location, "memory:/java_commit_messages");
            assert_eq!(state.overwrite, overwrite);
            assert_eq!(state.messages[0].bucket, 3);
            assert_eq!(state.messages[0].check_from_snapshot, Some(7));
            assert_eq!(state.messages[1].total_buckets, Some(5));
            assert_eq!(state.messages[1].new_index_files[0].file_name, "index");
            let index = &state.messages[2].new_index_files[0];
            assert_eq!(index.external_path.as_deref(), Some("file:/index"));
            assert_eq!(index.deletion_vectors_ranges.as_ref().unwrap().len(), 1);

            let result = paimon_commit_messages_serialize(messages);
            assert_ok(result.error);
            paimon_commit_messages_free(messages);
            assert_eq!(copy_bodies(&result.bytes), java_bodies());
            paimon_bytes_array_free(result.bytes);
        }
        unwrap_table(table);
    }
}

#[test]
fn deserialize_rejects_invalid_inputs_without_partial_handles() {
    unsafe {
        let table = memory_table("memory:/invalid_commit_messages");
        let commit = new_committer(table, "coordinator", false);
        let reject = |target, version, bodies, len, code| {
            let result = paimon_commit_messages_deserialize(target, version, bodies, len);
            assert!(result.messages.is_null());
            assert_error(result.error, code);
        };
        for version in [-1, 0, 13, 15] {
            reject(
                commit,
                version,
                ptr::null(),
                0,
                PaimonErrorCode::Unsupported,
            );
        }
        reject(
            ptr::null(),
            14,
            ptr::null(),
            0,
            PaimonErrorCode::InvalidInput,
        );
        let uninitialized = paimon_table_commit {
            inner: ptr::null_mut(),
        };
        reject(
            &uninitialized,
            14,
            ptr::null(),
            0,
            PaimonErrorCode::InvalidInput,
        );
        reject(commit, 14, ptr::null(), 1, PaimonErrorCode::InvalidInput);
        let body = java_bodies().remove(0);
        let valid = paimon_byte_slice {
            data: body.as_ptr(),
            len: body.len(),
        };
        reject(
            commit,
            14,
            &valid,
            usize::MAX,
            PaimonErrorCode::InvalidInput,
        );
        for (data, len) in [
            (ptr::null(), 0),
            (ptr::null(), 1),
            (body.as_ptr(), 0),
            (body.as_ptr(), usize::MAX),
        ] {
            let invalid = paimon_byte_slice { data, len };
            reject(commit, 14, &invalid, 1, PaimonErrorCode::InvalidInput);
        }

        // Every truncation, including after an already-decoded first body,
        // must return an error rather than a partial message list or a panic.
        for body in java_bodies() {
            for len in 0..body.len() {
                let inputs = [
                    valid,
                    paimon_byte_slice {
                        data: body.as_ptr(),
                        len,
                    },
                ];
                reject(
                    commit,
                    14,
                    inputs.as_ptr(),
                    inputs.len(),
                    PaimonErrorCode::InvalidInput,
                );
            }
        }
        let mut malformed = Vec::new();
        let mut trailing = body.clone();
        trailing.push(0);
        malformed.push(trailing);
        for (offset, value) in [(0, -1_i32), (21, -1), (21, i32::MAX)] {
            let mut bytes = body.clone();
            bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
            malformed.push(bytes);
        }
        let mut invalid_flag = body.clone();
        invalid_flag[20] = 2;
        malformed.push(invalid_flag);
        for bytes in malformed {
            let invalid = paimon_byte_slice {
                data: bytes.as_ptr(),
                len: bytes.len(),
            };
            reject(commit, 14, &invalid, 1, PaimonErrorCode::InvalidInput);
        }
        let messages = import(commit, &[body]);
        paimon_commit_messages_free(messages);
        paimon_table_commit_free(commit);
        unwrap_table(table);
    }
}

#[test]
fn empty_messages_and_null_export() {
    unsafe {
        for messages in [
            ptr::null(),
            &paimon_commit_messages {
                inner: ptr::null_mut(),
            },
        ] {
            let result = paimon_commit_messages_serialize(messages);
            assert_error(result.error, PaimonErrorCode::InvalidInput);
            assert!(result.bytes.data.is_null());
            assert_eq!(result.bytes.len, 0);
            paimon_bytes_array_free(result.bytes);
        }
        let table = memory_table("memory:/empty_imported_messages");
        let commit = new_committer(table, "coordinator", false);
        let result = paimon_commit_messages_deserialize(commit, 14, ptr::null(), 0);
        assert_ok(result.error);
        assert!(!result.messages.is_null());
        let exported = paimon_commit_messages_serialize(result.messages);
        assert_ok(exported.error);
        assert!(exported.bytes.data.is_null());
        assert_eq!(exported.bytes.len, 0);
        assert_ok(paimon_table_commit_commit(commit, result.messages));
        paimon_bytes_array_free(exported.bytes);
        paimon_commit_messages_free(result.messages);
        paimon_table_commit_free(commit);
        unwrap_table(table);
    }
}

#[test]
fn imported_messages_use_target_context_for_commit_merge_and_overwrite() {
    unsafe {
        for overwrite in [false, true] {
            let path = format!("memory:/imported_context_{overwrite}");
            let table = memory_table(&path);
            write_data_rust(table_ref(table), &[make_batch(vec![1], vec!["old"])]);
            let messages = prepare(table, "worker", overwrite, make_batch(vec![2], vec!["new"]));
            let exported = paimon_commit_messages_serialize(messages);
            assert_ok(exported.error);
            paimon_commit_messages_free(messages);
            let commit = new_committer(table, "coordinator", overwrite);
            let restored = import(commit, &copy_bodies(&exported.bytes));
            paimon_bytes_array_free(exported.bytes);
            paimon_table_commit_free(commit);

            let other_table = memory_table(&format!("{path}_other"));
            for wrong in [
                new_committer(other_table, "coordinator", overwrite),
                new_committer(table, "other-user", overwrite),
                new_committer(table, "coordinator", !overwrite),
            ] {
                assert_error(
                    paimon_table_commit_commit(wrong, restored),
                    PaimonErrorCode::InvalidInput,
                );
                assert_error(
                    paimon_table_commit_abort(wrong, restored),
                    PaimonErrorCode::InvalidInput,
                );
                let empty = import(wrong, &[]);
                assert_error(
                    paimon_commit_messages_merge(restored, empty),
                    PaimonErrorCode::InvalidInput,
                );
                paimon_commit_messages_free(empty);
                paimon_table_commit_free(wrong);
            }
            unwrap_table(other_table);

            let commit = new_committer(table, "coordinator", overwrite);
            if overwrite {
                assert_error(
                    paimon_table_commit_commit(commit, restored),
                    PaimonErrorCode::InvalidInput,
                );
                assert_ok(paimon_table_commit_overwrite_with_identifier(
                    commit, restored, 42,
                ));
            } else {
                assert_error(
                    paimon_table_commit_overwrite(commit, restored),
                    PaimonErrorCode::InvalidInput,
                );
                assert_ok(paimon_table_commit_commit_with_identifier(
                    commit, restored, 42,
                ));
                assert_ok(paimon_table_commit_filter_and_commit_with_identifier(
                    commit, restored, 42,
                ));
            }
            let expected = if overwrite {
                vec![(2, "new".into())]
            } else {
                vec![(1, "old".into()), (2, "new".into())]
            };
            assert_eq!(read_rows_ffi(table), expected);
            let snapshot = crate::runtime()
                .block_on(table_ref(table).snapshot_manager().get_latest_snapshot())
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.commit_user(), "coordinator");
            assert_eq!(snapshot.commit_identifier(), 42);
            assert_eq!(snapshot.id(), 2);
            paimon_commit_messages_free(restored);
            paimon_table_commit_free(commit);
            unwrap_table(table);
        }
    }
}

#[test]
fn imported_fixed_bucket_flag_comes_from_target_mode() {
    unsafe {
        let table = memory_table("memory:/imported_bucket_mode");
        let mut message = paimon::table::CommitMessage::new(Vec::new(), 0, Vec::new());
        message.total_buckets = Some(1);
        let bodies = vec![message.serialize().unwrap()];
        for overwrite in [false, true] {
            let commit = new_committer(table, "coordinator", overwrite);
            let messages = import(commit, &bodies);
            let error = if overwrite {
                paimon_table_commit_overwrite(commit, messages)
            } else {
                paimon_table_commit_commit(commit, messages)
            };
            assert_ok(error);
            paimon_commit_messages_free(messages);
            paimon_table_commit_free(commit);
        }
        unwrap_table(table);
    }
}

#[test]
fn imported_messages_can_abort_prepared_files() {
    unsafe {
        let path = "memory:/imported_abort";
        let table = memory_table(path);
        let messages = prepare(table, "worker", false, make_batch(vec![1], vec!["a"]));
        let state = &*((*messages).inner as *const CommitMessagesState);
        let message = &state.messages[0];
        let file = format!(
            "{path}/bucket-{}/{}",
            message.bucket, message.new_files[0].file_name
        );
        let exported = paimon_commit_messages_serialize(messages);
        assert_ok(exported.error);
        paimon_commit_messages_free(messages);
        let commit = new_committer(table, "coordinator", false);
        let imported = import(commit, &copy_bodies(&exported.bytes));
        paimon_bytes_array_free(exported.bytes);
        let io = table_ref(table).file_io();
        assert!(crate::runtime().block_on(io.exists(&file)).unwrap());
        assert_ok(paimon_table_commit_abort(commit, imported));
        assert!(!crate::runtime().block_on(io.exists(&file)).unwrap());
        paimon_commit_messages_free(imported);
        paimon_table_commit_free(commit);
        unwrap_table(table);
    }
}

#[test]
fn format_table_messages_are_unsupported_and_remain_owned_by_caller() {
    unsafe {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .option("type", "format-table")
            .option("file.format", "parquet")
            .build()
            .unwrap();
        let table = wrap_table(Table::new(
            memory_file_io(),
            Identifier::new("default", "format"),
            "memory:/format_message_bytes".into(),
            TableSchema::new(0, &schema),
            None,
        ));
        let messages = prepare(table, "worker", false, make_batch(vec![1], vec!["a"]));
        let exported = paimon_commit_messages_serialize(messages);
        assert_error(exported.error, PaimonErrorCode::Unsupported);
        assert!(exported.bytes.data.is_null());
        assert_eq!(exported.bytes.len, 0);
        paimon_bytes_array_free(exported.bytes);
        let commit = new_committer(table, "worker", false);
        let result = paimon_commit_messages_deserialize(commit, 14, ptr::null(), 0);
        assert_error(result.error, PaimonErrorCode::Unsupported);
        assert!(result.messages.is_null());
        assert_ok(paimon_table_commit_abort(commit, messages));
        paimon_commit_messages_free(messages);
        paimon_table_commit_free(commit);
        unwrap_table(table);
    }
}

#[test]
fn cross_process_commit() {
    const ROLE: &str = "PAIMON_C_COMMIT_BYTES_ROLE";
    const ROOT: &str = "PAIMON_C_COMMIT_BYTES_ROOT";
    let open_catalog = |root: &str| {
        let mut options = Options::new();
        options.set("warehouse", root);
        FileSystemCatalog::new(options).unwrap()
    };
    let identifier = Identifier::new("default", "messages");
    if let Ok(role) = std::env::var(ROLE) {
        let root = std::env::var(ROOT).unwrap();
        let catalog = open_catalog(&root);
        let table = crate::runtime()
            .block_on(catalog.get_table(&identifier))
            .unwrap();
        unsafe {
            let table = wrap_table(table);
            if role == "coordinator" {
                let commit = new_committer(table, "distributed-job", false);
                let merged = import(commit, &[]);
                for worker in [1, 2] {
                    let transport = std::fs::read(format!("{root}/worker-{worker}.json")).unwrap();
                    let (version, bodies): (i32, Vec<Vec<u8>>) =
                        serde_json::from_slice(&transport).unwrap();
                    let inputs = slices(&bodies);
                    let decoded = paimon_commit_messages_deserialize(
                        commit,
                        version,
                        inputs.as_ptr(),
                        inputs.len(),
                    );
                    assert_ok(decoded.error);
                    drop(bodies);
                    assert_ok(paimon_commit_messages_merge(merged, decoded.messages));
                    paimon_commit_messages_free(decoded.messages);
                }
                assert_ok(paimon_table_commit_commit_with_identifier(
                    commit, merged, 42,
                ));
                paimon_table_commit_free(commit);
                let retry = new_committer(table, "distributed-job", false);
                assert_ok(paimon_table_commit_filter_and_commit_with_identifier(
                    retry, merged, 42,
                ));
                paimon_table_commit_free(retry);
                paimon_commit_messages_free(merged);
            } else {
                let worker: i32 = role.parse().unwrap();
                let messages = prepare(
                    table,
                    "distributed-job",
                    false,
                    make_batch(vec![worker * 2 - 1, worker * 2], vec!["a", "b"]),
                );
                let exported = paimon_commit_messages_serialize(messages);
                assert_ok(exported.error);
                assert_eq!(exported.bytes.len, 2, "one body for each partition");
                paimon_commit_messages_free(messages);
                let transport = serde_json::to_vec(&(
                    paimon_commit_messages_serializer_version(),
                    copy_bodies(&exported.bytes),
                ))
                .unwrap();
                paimon_bytes_array_free(exported.bytes);
                std::fs::write(format!("{root}/worker-{worker}.json"), transport).unwrap();
            }
            unwrap_table(table);
        }
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().to_str().unwrap();
    let catalog = open_catalog(root);
    crate::runtime().block_on(async {
        catalog
            .create_database("default", false, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .partition_keys(["name"])
            .build()
            .unwrap();
        catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap();
    });
    // Each worker exits before the coordinator starts: only disk bytes and
    // prepared files survive, never handles, Rust objects or memory storage.
    for role in ["1", "2", "coordinator"] {
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::commit_messages::cross_process_commit",
                "--nocapture",
            ])
            .env(ROLE, role)
            .env(ROOT, root)
            .status()
            .unwrap();
        assert!(status.success(), "{role} process failed");
    }
    let table = crate::runtime()
        .block_on(catalog.get_table(&identifier))
        .unwrap();
    let snapshot = crate::runtime()
        .block_on(table.snapshot_manager().get_latest_snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.id(),
        1,
        "all workers publish once and retry is filtered"
    );
    assert_eq!(snapshot.commit_user(), "distributed-job");
    assert_eq!(snapshot.commit_identifier(), 42);
    unsafe {
        let table = wrap_table(table);
        assert_eq!(
            read_rows_ffi(table),
            vec![
                (1, "a".into()),
                (2, "b".into()),
                (3, "a".into()),
                (4, "b".into())
            ]
        );
        unwrap_table(table);
    }
}
