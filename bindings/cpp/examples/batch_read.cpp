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

  const paimon::Option options[] = {{"warehouse", argv[1]}};
  auto catalog_result = paimon::Catalog::create(options);
  if (!catalog_result) {
    print_error(catalog_result.error());
    return EXIT_FAILURE;
  }
  auto catalog = std::move(catalog_result).value();

  auto identifier_result = paimon::Identifier::create(argv[2], argv[3]);
  if (!identifier_result) {
    print_error(identifier_result.error());
    return EXIT_FAILURE;
  }
  auto identifier = std::move(identifier_result).value();

  auto table_result = catalog.get_table(identifier);
  if (!table_result) {
    print_error(table_result.error());
    return EXIT_FAILURE;
  }
  auto table = std::move(table_result).value();

  auto builder_result = table.new_read_builder();
  if (!builder_result) {
    print_error(builder_result.error());
    return EXIT_FAILURE;
  }
  auto builder = std::move(builder_result).value();

  auto scan_result = builder.new_scan();
  auto read_result = builder.new_read();
  if (!scan_result || !read_result) {
    print_error(!scan_result ? scan_result.error() : read_result.error());
    return EXIT_FAILURE;
  }
  auto scan = std::move(scan_result).value();
  auto read = std::move(read_result).value();

  auto plan_result = scan.plan();
  if (!plan_result) {
    print_error(plan_result.error());
    return EXIT_FAILURE;
  }
  auto plan = std::move(plan_result).value();

  auto reader_result = read.to_arrow(plan);
  if (!reader_result) {
    print_error(reader_result.error());
    return EXIT_FAILURE;
  }
  auto reader = std::move(reader_result).value();

  std::size_t batch_count = 0;
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
    // Import batch.array()/batch.schema() with any Arrow C Data consumer here.
    // ArrowBatch releases both native containers when it leaves this scope.
    ++batch_count;
  }

  std::printf("splits=%zu batches=%zu\n", plan.num_splits(), batch_count);
  return EXIT_SUCCESS;
}
