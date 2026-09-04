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

#include <paimon/paimon.hpp>

#include <type_traits>

static_assert(std::is_move_constructible<paimon::Catalog>::value, "moveable");
static_assert(!std::is_copy_constructible<paimon::Catalog>::value,
              "not copyable");
static_assert(std::is_nothrow_destructible<paimon::ArrowBatch>::value,
              "noexcept Arrow ownership");
static_assert(std::is_nothrow_destructible<paimon::TableCommit>::value,
              "noexcept committer ownership");

void paimon_cpp_header_smoke(const paimon::Option* options,
                             std::size_t option_count,
                             void* arrow_array, void* arrow_schema) {
  auto catalog = paimon::Catalog::create(options, option_count);
  auto identifier = paimon::Identifier::create("default", "table");
  auto direct_table = paimon::Table::from_schema_json(
      "/tmp/table", "{}", "default", "table");
  auto split_plan = paimon::Plan::from_split_bytes(nullptr, 0);
  (void)direct_table;
  (void)split_plan;
  if (!catalog || !identifier) {
    return;
  }

  auto table = catalog.value().get_table(identifier.value());
  auto create_table_status = catalog.value().create_table_from_schema_json(
      identifier.value(), "{}", true);
  auto drop_table_status = catalog.value().drop_table(identifier.value(), true);
  (void)create_table_status;
  (void)drop_table_status;
  if (!table) {
    return;
  }

  auto read_builder = table.value().new_read_builder();
  if (read_builder) {
    const char* projection[] = {"id", nullptr};
    auto projection_status = read_builder.value().with_projection(projection);
    auto case_status = read_builder.value().with_case_sensitive(true);
    auto scan = read_builder.value().new_scan();
    auto read = read_builder.value().new_read();
    auto stream_options = paimon::StreamScanOptions::defaults();
    (void)projection_status;
    (void)case_status;
    if (scan && read) {
      auto plan = scan.value().plan();
      if (plan) {
        auto reader = read.value().to_arrow(plan.value());
        if (reader) {
          auto batch = reader.value().next();
          (void)batch;
        }
      }
      if (stream_options) {
        stream_options.value().with_startup(
            paimon::StreamStartupMode::latest);
        stream_options.value().with_follow_up(
            paimon::StreamFollowUpMode::automatic);
        auto stream_scan = read_builder.value().new_stream_scan(
            stream_options.value());
        if (stream_scan) {
          const auto checkpoint = stream_scan.value().checkpoint();
          auto restore = stream_scan.value().restore(checkpoint);
          auto poll = stream_scan.value().poll();
          (void)restore;
          if (poll && poll.value().has_data()) {
            auto plan_bytes = poll.value().plan().serialize();
            if (plan_bytes) {
              auto restored_plan = paimon::StreamPlan::deserialize(
                  plan_bytes.value().data(), plan_bytes.value().size());
              (void)restored_plan;
            }
            auto stream_reader = poll.value().plan().read_to_arrow(
                read.value(), paimon::StreamReadMode::data);
            (void)stream_reader;
          }
        }
      }
    }
  }

  auto write_builder = table.value().new_write_builder("stable-writer");
  if (!write_builder) {
    return;
  }
  auto overwrite_status = write_builder.value().with_overwrite();
  auto writer = write_builder.value().new_write();
  auto committer = write_builder.value().new_commit();
  (void)overwrite_status;
  if (!writer || !committer) {
    return;
  }
  auto write_status = writer.value().write_arrow(arrow_array, arrow_schema);
  auto prepared = writer.value().prepare_commit();
  (void)write_status;
  if (prepared) {
    auto durable = prepared.value().prepare(1);
    if (durable) {
      auto serialized = durable.value().serialize();
      if (serialized) {
        auto restored = paimon::PreparedCommit::deserialize(
            serialized.value().data(), serialized.value().size());
        if (restored) {
          auto merge_status = durable.value().merge(restored.value());
          auto commit_status = committer.value().commit_prepared(durable.value());
          (void)merge_status;
          (void)commit_status;
        }
      }
    }
  }
}
