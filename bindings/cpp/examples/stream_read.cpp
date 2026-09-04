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

#include <cstdio>
#include <cstdlib>
#include <utility>

namespace {

void print_error(const paimon::Error& error) {
  const auto message = error.message();
  std::fprintf(stderr, "Paimon error %d: %.*s\n",
               static_cast<int>(error.code()),
               static_cast<int>(message.size()), message.data());
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 4) {
    std::fprintf(stderr, "usage: %s WAREHOUSE DATABASE TABLE\n", argv[0]);
    return EXIT_FAILURE;
  }

  const paimon::Option catalog_options[] = {{"warehouse", argv[1]}};
  auto catalog_result = paimon::Catalog::create(catalog_options);
  auto identifier_result = paimon::Identifier::create(argv[2], argv[3]);
  if (!catalog_result || !identifier_result) {
    print_error(!catalog_result ? catalog_result.error()
                                : identifier_result.error());
    return EXIT_FAILURE;
  }
  auto catalog = std::move(catalog_result).value();
  auto identifier = std::move(identifier_result).value();

  auto table_result = catalog.get_table(identifier);
  if (!table_result) {
    print_error(table_result.error());
    return EXIT_FAILURE;
  }
  auto table = std::move(table_result).value();

  auto builder_result = table.new_read_builder();
  auto options_result = paimon::StreamScanOptions::defaults();
  if (!builder_result || !options_result) {
    print_error(!builder_result ? builder_result.error()
                                : options_result.error());
    return EXIT_FAILURE;
  }
  auto builder = std::move(builder_result).value();
  auto options = std::move(options_result).value();
  options.with_startup(paimon::StreamStartupMode::latest_full)
      .with_follow_up(paimon::StreamFollowUpMode::automatic);

  auto read_result = builder.new_read();
  auto scan_result = builder.new_stream_scan(options);
  if (!read_result || !scan_result) {
    print_error(!read_result ? read_result.error() : scan_result.error());
    return EXIT_FAILURE;
  }
  auto read = std::move(read_result).value();
  auto scan = std::move(scan_result).value();

  // poll() is a pull operation and never waits. A scheduler should call it
  // again later after Waiting; this standalone example exits instead.
  auto poll_result = scan.poll();
  if (!poll_result) {
    print_error(poll_result.error());
    return EXIT_FAILURE;
  }
  auto poll = std::move(poll_result).value();
  if (poll.waiting()) {
    std::printf("waiting next_snapshot_id=%lld\n",
                static_cast<long long>(poll.next_snapshot_id()));
    return EXIT_SUCCESS;
  }
  if (poll.end()) {
    std::puts("end");
    return EXIT_SUCCESS;
  }

  auto pending_plan_result = poll.plan().serialize();
  if (!pending_plan_result) {
    print_error(pending_plan_result.error());
    return EXIT_FAILURE;
  }
  auto pending_plan = std::move(pending_plan_result).value();
  // Persist pending_plan together with the cursor before exposing rows. On
  // recovery, StreamPlan::deserialize recreates this PollResult for replay.
  std::printf("pending-plan-bytes=%zu\n", pending_plan.size());

  auto reader_result = poll.plan().read_to_arrow(
      read, paimon::StreamReadMode::data);
  if (!reader_result) {
    print_error(reader_result.error());
    return EXIT_FAILURE;
  }
  auto reader = std::move(reader_result).value();
  std::size_t batches = 0;
  for (;;) {
    auto next = reader.next();
    if (!next) {
      print_error(next.error());
      return EXIT_FAILURE;
    }
    auto batch = std::move(next).value();
    if (!batch) {
      break;
    }
    // Import through any Arrow C Data consumer before batch is destroyed.
    ++batches;
  }

  // Persist this cursor only after the plan's split progress is durably part of
  // the surrounding checkpoint barrier.
  std::printf("snapshot=%lld splits=%zu batches=%zu checkpoint=%lld\n",
              static_cast<long long>(poll.snapshot_id()),
              poll.plan().num_splits(), batches,
              static_cast<long long>(scan.checkpoint()));
  return EXIT_SUCCESS;
}
