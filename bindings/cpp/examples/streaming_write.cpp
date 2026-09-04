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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <utility>

// Data and checkpoint barriers are separate events so an idle stream can still
// checkpoint. Paimon consumes Arrow contents in place; the producer continues
// to own the ArrowArray and ArrowSchema container memory.
enum class StreamEventKind : std::uint8_t { batch, checkpoint, end };

struct StreamEvent {
  StreamEventKind kind;
  void* array;
  void* schema;
};

using Producer = paimon::Status (*)(void* context, StreamEvent* output);
using Persist = paimon::Status (*)(void* context, std::int64_t checkpoint_id,
                                  const std::uint8_t* data, std::size_t size);

paimon::Status recover_checkpoint(const paimon::TableCommit& committer,
                                  const std::uint8_t* data,
                                  std::size_t size) {
  // A zero-length checkpoint records source progress but has no Paimon data to
  // commit. The surrounding engine owns that source-state representation.
  if (size == 0) {
    return paimon::Status::success();
  }
  auto restored_result = paimon::PreparedCommit::deserialize(data, size);
  if (!restored_result) {
    return paimon::Status::failure(std::move(restored_result).error());
  }
  auto restored = std::move(restored_result).value();
  return committer.commit_prepared(restored);
}

paimon::Status complete_checkpoint(paimon::TableWrite& writer,
                                   const paimon::TableCommit& committer,
                                   Persist persist, void* context,
                                   std::int64_t checkpoint_id) {
  auto prepared_result = writer.prepare_commit();
  if (!prepared_result) {
    return paimon::Status::failure(std::move(prepared_result).error());
  }
  auto prepared = std::move(prepared_result).value();

  auto durable_result = prepared.prepare(checkpoint_id);
  if (!durable_result) {
    return paimon::Status::failure(std::move(durable_result).error());
  }
  auto durable = std::move(durable_result).value();
  auto bytes_result = durable.serialize();
  if (!bytes_result) {
    return paimon::Status::failure(std::move(bytes_result).error());
  }
  auto bytes = std::move(bytes_result).value();

  // persist must not report success until the checkpoint blob and the engine's
  // source state are durable in the same checkpoint protocol.
  auto persist_status =
      persist(context, checkpoint_id, bytes.data(), bytes.size());
  if (!persist_status) {
    return persist_status;
  }

  // commit_prepared is retry-safe. After a crash, deserialize the persisted
  // blob and call this again with the same stable commit_user.
  return committer.commit_prepared(durable);
}

paimon::Status run_stream(paimon::TableWrite& writer,
                          const paimon::TableCommit& committer,
                          Producer producer, void* context,
                          Persist persist,
                          std::int64_t first_checkpoint_id) {
  auto checkpoint_id = first_checkpoint_id;
  bool dirty = false;
  for (;;) {
    StreamEvent event{StreamEventKind::end, nullptr, nullptr};
    auto producer_status = producer(context, &event);
    if (!producer_status) {
      return producer_status;
    }

    switch (event.kind) {
      case StreamEventKind::batch: {
        auto write_status = writer.write_arrow(event.array, event.schema);
        if (!write_status) {
          return write_status;
        }
        dirty = true;
        break;
      }
      case StreamEventKind::checkpoint: {
        auto checkpoint_status =
            dirty ? complete_checkpoint(writer, committer, persist, context,
                                        checkpoint_id)
                  : persist(context, checkpoint_id, nullptr, 0);
        if (!checkpoint_status) {
          return checkpoint_status;
        }
        dirty = false;
        ++checkpoint_id;
        break;
      }
      case StreamEventKind::end:
        // Never discard a tail batch merely because the producer ended before
        // emitting its next periodic checkpoint barrier.
        return dirty ? complete_checkpoint(writer, committer, persist, context,
                                           checkpoint_id)
                     : paimon::Status::success();
    }
  }
}

namespace {

void print_error(const paimon::Error& error) {
  const auto message = error.message();
  std::fprintf(stderr, "Paimon error %d: %.*s\n",
               static_cast<int>(error.code()),
               static_cast<int>(message.size()), message.data());
}

paimon::Status no_input(void*, StreamEvent* event) {
  *event = {StreamEventKind::end, nullptr, nullptr};
  return paimon::Status::success();
}

paimon::Status no_op_persist(void*, std::int64_t, const std::uint8_t*,
                             std::size_t) {
  // Replace this with fsync/rename or the surrounding engine's durable state.
  return paimon::Status::success();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 5) {
    std::fprintf(stderr,
                 "usage: %s WAREHOUSE DATABASE TABLE STABLE_COMMIT_USER\n",
                 argv[0]);
    return EXIT_FAILURE;
  }

  const paimon::Option options[] = {{"warehouse", argv[1]}};
  auto catalog_result = paimon::Catalog::create(options);
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

  auto builder_result = table.new_write_builder(argv[4]);
  if (!builder_result) {
    print_error(builder_result.error());
    return EXIT_FAILURE;
  }
  auto builder = std::move(builder_result).value();

  auto writer_result = builder.new_write();
  auto committer_result = builder.new_commit();
  if (!writer_result || !committer_result) {
    print_error(!writer_result ? writer_result.error()
                               : committer_result.error());
    return EXIT_FAILURE;
  }
  auto writer = std::move(writer_result).value();
  auto committer = std::move(committer_result).value();

  // Replace no_input and no_op_persist with the application's Arrow producer
  // and durable checkpoint store.
  auto status = run_stream(writer, committer, no_input, nullptr,
                           no_op_persist, 1);
  if (!status) {
    print_error(status.error());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
