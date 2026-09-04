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

#ifndef PAIMON_CPP_TEST_PAIMON_STUB_H
#define PAIMON_CPP_TEST_PAIMON_STUB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define PAIMON_ERROR_UNEXPECTED 0
#define PAIMON_ERROR_UNSUPPORTED 1
#define PAIMON_ERROR_NOT_FOUND 2
#define PAIMON_ERROR_ALREADY_EXISTS 3
#define PAIMON_ERROR_INVALID_INPUT 4
#define PAIMON_ERROR_IO 5
#define PAIMON_ERROR_OUT_OF_RANGE 6

#define PAIMON_STREAM_STARTUP_LATEST_FULL 0
#define PAIMON_STREAM_STARTUP_LATEST 1
#define PAIMON_STREAM_STARTUP_FROM_SNAPSHOT 2
#define PAIMON_STREAM_STARTUP_FROM_SNAPSHOT_FULL 3
#define PAIMON_STREAM_FOLLOW_UP_AUTO 0
#define PAIMON_STREAM_FOLLOW_UP_DELTA 1
#define PAIMON_STREAM_FOLLOW_UP_CHANGELOG 2
#define PAIMON_STREAM_POLL_DATA 0
#define PAIMON_STREAM_POLL_WAITING 1
#define PAIMON_STREAM_POLL_END 2
#define PAIMON_STREAM_READ_DATA 0
#define PAIMON_STREAM_READ_AUDIT_LOG 1

#ifdef __cplusplus
extern "C" {
#endif

typedef struct paimon_bytes {
  uint8_t* data;
  size_t len;
} paimon_bytes;

typedef struct paimon_error {
  int32_t code;
  paimon_bytes message;
} paimon_error;

typedef struct paimon_option {
  const char* key;
  const char* value;
} paimon_option;

typedef struct paimon_catalog paimon_catalog;
typedef struct paimon_identifier paimon_identifier;
typedef struct paimon_table paimon_table;
typedef struct paimon_read_builder paimon_read_builder;
typedef struct paimon_table_scan paimon_table_scan;
typedef struct paimon_plan paimon_plan;
typedef struct paimon_table_read paimon_table_read;
typedef struct paimon_record_batch_reader paimon_record_batch_reader;
typedef struct paimon_write_builder paimon_write_builder;
typedef struct paimon_table_write paimon_table_write;
typedef struct paimon_commit_messages paimon_commit_messages;
typedef struct paimon_table_commit paimon_table_commit;
typedef struct paimon_prepared_commit paimon_prepared_commit;
typedef struct paimon_stream_scan paimon_stream_scan;
typedef struct paimon_stream_plan paimon_stream_plan;

typedef struct paimon_stream_scan_options {
  uint32_t struct_size;
  int32_t startup_mode;
  int32_t follow_up_mode;
  int64_t snapshot_id;
  uint64_t reserved[4];
} paimon_stream_scan_options;

typedef struct paimon_arrow_batch {
  void* array;
  void* schema;
} paimon_arrow_batch;

typedef struct paimon_result_catalog_new {
  paimon_catalog* catalog;
  paimon_error* error;
} paimon_result_catalog_new;

typedef struct paimon_result_identifier_new {
  paimon_identifier* identifier;
  paimon_error* error;
} paimon_result_identifier_new;

typedef struct paimon_result_get_table {
  paimon_table* table;
  paimon_error* error;
} paimon_result_get_table;

typedef struct paimon_result_read_builder {
  paimon_read_builder* read_builder;
  paimon_error* error;
} paimon_result_read_builder;

typedef struct paimon_result_table_scan {
  paimon_table_scan* scan;
  paimon_error* error;
} paimon_result_table_scan;

typedef struct paimon_result_new_read {
  paimon_table_read* read;
  paimon_error* error;
} paimon_result_new_read;

typedef struct paimon_result_plan {
  paimon_plan* plan;
  paimon_error* error;
} paimon_result_plan;

typedef struct paimon_result_record_batch_reader {
  paimon_record_batch_reader* reader;
  paimon_error* error;
} paimon_result_record_batch_reader;

typedef struct paimon_result_next_batch {
  paimon_arrow_batch batch;
  paimon_error* error;
} paimon_result_next_batch;

typedef struct paimon_result_write_builder {
  paimon_write_builder* write_builder;
  paimon_error* error;
} paimon_result_write_builder;

typedef struct paimon_result_table_write {
  paimon_table_write* write;
  paimon_error* error;
} paimon_result_table_write;

typedef struct paimon_result_table_commit {
  paimon_table_commit* commit;
  paimon_error* error;
} paimon_result_table_commit;

typedef struct paimon_result_prepare_commit {
  paimon_commit_messages* messages;
  paimon_error* error;
} paimon_result_prepare_commit;

typedef struct paimon_result_prepared_commit {
  paimon_prepared_commit* prepared;
  paimon_error* error;
} paimon_result_prepared_commit;

typedef struct paimon_result_bytes {
  paimon_bytes bytes;
  paimon_error* error;
} paimon_result_bytes;

typedef struct paimon_result_stream_scan {
  paimon_stream_scan* scan;
  paimon_error* error;
} paimon_result_stream_scan;

typedef struct paimon_result_stream_poll {
  int32_t status;
  paimon_stream_plan* plan;
  int64_t snapshot_id;
  int64_t next_snapshot_id;
  int64_t watermark;
  uint8_t has_watermark;
  uint8_t reserved[7];
  paimon_error* error;
} paimon_result_stream_poll;

void paimon_error_free(paimon_error* error);
void paimon_bytes_free(paimon_bytes bytes);
paimon_result_catalog_new paimon_catalog_create(const paimon_option* options,
                                                size_t options_len);
void paimon_catalog_free(paimon_catalog* catalog);
paimon_result_get_table paimon_catalog_get_table(
    const paimon_catalog* catalog, const paimon_identifier* identifier);
paimon_error* paimon_catalog_create_table_from_schema_json(
    const paimon_catalog* catalog, const paimon_identifier* identifier,
    const char* schema_json, bool ignore_if_exists);
paimon_error* paimon_catalog_drop_table(
    const paimon_catalog* catalog, const paimon_identifier* identifier,
    bool ignore_if_not_exists);
paimon_result_identifier_new paimon_identifier_new(const char* database,
                                                   const char* object);
void paimon_identifier_free(paimon_identifier* identifier);

paimon_result_get_table paimon_table_from_schema_json(
    const char* table_path, const char* table_schema_json,
    const char* database, const char* table_name, const char* branch,
    const paimon_option* storage_options, size_t storage_options_len);
void paimon_table_free(paimon_table* table);
paimon_result_read_builder paimon_table_new_read_builder(
    const paimon_table* table);
paimon_result_read_builder paimon_table_new_read_builder_with_options(
    const paimon_table* table, const paimon_option* options,
    size_t options_len);
void paimon_read_builder_free(paimon_read_builder* builder);
paimon_error* paimon_read_builder_with_projection(
    paimon_read_builder* builder, const char* const* columns);
paimon_error* paimon_read_builder_with_case_sensitive(
    paimon_read_builder* builder, bool case_sensitive);
paimon_result_table_scan paimon_read_builder_new_scan(
    const paimon_read_builder* builder);
paimon_result_new_read paimon_read_builder_new_read(
    const paimon_read_builder* builder);
paimon_error* paimon_stream_scan_options_init(
    paimon_stream_scan_options* options);
paimon_result_stream_scan paimon_read_builder_new_stream_scan(
    const paimon_read_builder* builder,
    const paimon_stream_scan_options* options);
paimon_result_stream_poll paimon_stream_scan_poll(paimon_stream_scan* scan);
int64_t paimon_stream_scan_checkpoint(const paimon_stream_scan* scan);
paimon_error* paimon_stream_scan_restore(paimon_stream_scan* scan,
                                         int64_t next_snapshot_id);
void paimon_stream_scan_free(paimon_stream_scan* scan);
uint8_t paimon_stream_plan_is_full(const paimon_stream_plan* plan);
size_t paimon_stream_plan_num_splits(const paimon_stream_plan* plan);
paimon_result_bytes paimon_stream_plan_serialize(
    const paimon_stream_plan* plan);
paimon_result_stream_poll paimon_stream_plan_deserialize(
    const uint8_t* data, size_t size);
paimon_result_record_batch_reader paimon_stream_plan_read_to_arrow(
    const paimon_table_read* read, const paimon_stream_plan* plan,
    size_t offset, size_t length, int32_t read_mode);
void paimon_stream_plan_free(paimon_stream_plan* plan);
void paimon_table_scan_free(paimon_table_scan* scan);
paimon_result_plan paimon_table_scan_plan(const paimon_table_scan* scan);
paimon_result_plan paimon_plan_from_split_bytes(const uint8_t* data,
                                                size_t size);
void paimon_plan_free(paimon_plan* plan);
size_t paimon_plan_num_splits(const paimon_plan* plan);
void paimon_table_read_free(paimon_table_read* read);
paimon_result_record_batch_reader paimon_table_read_to_arrow(
    const paimon_table_read* read, const paimon_plan* plan, size_t offset,
    size_t length);
paimon_result_next_batch paimon_record_batch_reader_next(
    paimon_record_batch_reader* reader);
void paimon_record_batch_reader_free(paimon_record_batch_reader* reader);
void paimon_arrow_batch_free(paimon_arrow_batch batch);

paimon_result_write_builder paimon_table_new_write_builder(
    const paimon_table* table);
paimon_result_write_builder paimon_table_new_write_builder_with_commit_user(
    const paimon_table* table, const char* commit_user);
void paimon_write_builder_free(paimon_write_builder* builder);
paimon_error* paimon_write_builder_with_overwrite(
    paimon_write_builder* builder);
paimon_result_table_write paimon_write_builder_new_write(
    const paimon_write_builder* builder);
paimon_result_table_commit paimon_write_builder_new_commit(
    const paimon_write_builder* builder);
void paimon_table_write_free(paimon_table_write* writer);
paimon_error* paimon_table_write_write_arrow_batch(paimon_table_write* writer,
                                                   void* array,
                                                   void* schema);
paimon_result_prepare_commit paimon_table_write_prepare_commit(
    paimon_table_write* writer);
void paimon_commit_messages_free(paimon_commit_messages* messages);
paimon_result_prepared_commit paimon_commit_messages_prepare(
    const paimon_commit_messages* messages, int64_t checkpoint_id);
paimon_result_bytes paimon_prepared_commit_serialize(
    const paimon_prepared_commit* prepared);
paimon_result_prepared_commit paimon_prepared_commit_deserialize(
    const uint8_t* data, size_t size);
int64_t paimon_prepared_commit_identifier(
    const paimon_prepared_commit* prepared);
void paimon_prepared_commit_free(paimon_prepared_commit* prepared);
paimon_error* paimon_commit_messages_merge(
    paimon_commit_messages* target, const paimon_commit_messages* source);
paimon_error* paimon_prepared_commit_merge(
    paimon_prepared_commit* target, const paimon_prepared_commit* source);
void paimon_table_commit_free(paimon_table_commit* committer);
paimon_error* paimon_table_commit_commit(
    const paimon_table_commit* committer, paimon_commit_messages* messages);
paimon_error* paimon_table_commit_commit_with_identifier(
    const paimon_table_commit* committer, paimon_commit_messages* messages,
    int64_t checkpoint_id);
paimon_error* paimon_table_commit_filter_and_commit_with_identifier(
    const paimon_table_commit* committer, paimon_commit_messages* messages,
    int64_t checkpoint_id);
paimon_error* paimon_table_commit_commit_prepared(
    const paimon_table_commit* committer,
    const paimon_prepared_commit* prepared);
paimon_error* paimon_table_commit_overwrite(
    const paimon_table_commit* committer, paimon_commit_messages* messages);
paimon_error* paimon_table_commit_overwrite_with_identifier(
    const paimon_table_commit* committer, paimon_commit_messages* messages,
    int64_t checkpoint_id);
paimon_error* paimon_table_commit_truncate_table(
    const paimon_table_commit* committer);
paimon_error* paimon_table_commit_truncate_table_with_identifier(
    const paimon_table_commit* committer, int64_t checkpoint_id);
paimon_error* paimon_table_commit_abort(
    const paimon_table_commit* committer, paimon_commit_messages* messages);
paimon_error* paimon_table_commit_abort_prepared(
    const paimon_table_commit* committer,
    const paimon_prepared_commit* prepared);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // PAIMON_CPP_TEST_PAIMON_STUB_H
