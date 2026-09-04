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

#ifndef PAIMON_CPP_PAIMON_HPP
#define PAIMON_CPP_PAIMON_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>
#include <string_view>
#include <type_traits>
#include <utility>

// Tests and embedders may override this with a quoted header name. Normal
// consumers use the cbindgen-generated paimon.h shipped with libpaimon_c.
#ifndef PAIMON_C_HEADER
#define PAIMON_C_HEADER <paimon.h>
#endif

// Keep overridden test/embedding headers under C linkage too. Nesting is
// harmless for the generated paimon.h, which has its own C++ compatibility
// guard.
extern "C" {
#include PAIMON_C_HEADER
}

namespace paimon {

struct adopt_handle_t {
  explicit constexpr adopt_handle_t() noexcept = default;
};

inline constexpr adopt_handle_t adopt_handle{};

enum class ErrorCode : std::int32_t {
  unexpected = PAIMON_ERROR_UNEXPECTED,
  unsupported = PAIMON_ERROR_UNSUPPORTED,
  not_found = PAIMON_ERROR_NOT_FOUND,
  already_exists = PAIMON_ERROR_ALREADY_EXISTS,
  invalid_input = PAIMON_ERROR_INVALID_INPUT,
  io_error = PAIMON_ERROR_IO,
  out_of_range = PAIMON_ERROR_OUT_OF_RANGE,
};

// Owns one paimon_error. Error is also used as the storage for Status: a null
// native handle means success. The message view remains valid until this Error
// is moved, reset, or destroyed.
class Error final {
 public:
  constexpr Error() noexcept = default;

  explicit Error(adopt_handle_t, ::paimon_error* error) noexcept
      : error_(error) {}

  Error(const Error&) = delete;
  Error& operator=(const Error&) = delete;

  Error(Error&& other) noexcept : error_(other.release()) {}

  Error& operator=(Error&& other) noexcept {
    if (this != &other) {
      reset(other.release());
    }
    return *this;
  }

  ~Error() noexcept { reset(); }

  [[nodiscard]] bool ok() const noexcept { return error_ == nullptr; }
  [[nodiscard]] explicit operator bool() const noexcept { return !ok(); }

  [[nodiscard]] ErrorCode code() const noexcept {
    return error_ == nullptr
               ? ErrorCode::unexpected
               : static_cast<ErrorCode>(error_->code);
  }

  [[nodiscard]] std::string_view message() const noexcept {
    if (error_ == nullptr || error_->message.data == nullptr) {
      return {};
    }
    return {reinterpret_cast<const char*>(error_->message.data),
            error_->message.len};
  }

  [[nodiscard]] ::paimon_error* native_handle() const noexcept {
    return error_;
  }

  [[nodiscard]] ::paimon_error* release() noexcept {
    auto* result = error_;
    error_ = nullptr;
    return result;
  }

  void reset(::paimon_error* error = nullptr) noexcept {
    if (error_ == error) {
      return;
    }
    if (error_ != nullptr) {
      ::paimon_error_free(error_);
    }
    error_ = error;
  }

 private:
  ::paimon_error* error_ = nullptr;
};

// Owns a byte buffer allocated by libpaimon_c. This is used by durable stream
// plan and prepared-commit APIs and never allocates through a C++ runtime.
class Bytes final {
 public:
  constexpr Bytes() noexcept : bytes_{nullptr, 0} {}
  explicit constexpr Bytes(adopt_handle_t, ::paimon_bytes bytes) noexcept
      : bytes_(bytes) {}

  Bytes(const Bytes&) = delete;
  Bytes& operator=(const Bytes&) = delete;

  Bytes(Bytes&& other) noexcept : bytes_(other.release()) {}

  Bytes& operator=(Bytes&& other) noexcept {
    if (this != &other) {
      reset();
      bytes_ = other.release();
    }
    return *this;
  }

  ~Bytes() noexcept { reset(); }

  [[nodiscard]] const std::uint8_t* data() const noexcept {
    return bytes_.data;
  }

  [[nodiscard]] std::size_t size() const noexcept { return bytes_.len; }
  [[nodiscard]] bool empty() const noexcept { return bytes_.len == 0; }

  [[nodiscard]] std::string_view string_view() const noexcept {
    if (bytes_.data == nullptr) {
      return {};
    }
    return {reinterpret_cast<const char*>(bytes_.data), bytes_.len};
  }

  [[nodiscard]] ::paimon_bytes native_handle() const noexcept { return bytes_; }

  [[nodiscard]] ::paimon_bytes release() noexcept {
    const auto result = bytes_;
    bytes_ = {nullptr, 0};
    return result;
  }

  void reset() noexcept {
    if (bytes_.data != nullptr) {
      ::paimon_bytes_free(bytes_);
      bytes_ = {nullptr, 0};
    }
  }

 private:
  ::paimon_bytes bytes_;
};

// A small C++17 expected-like result. It deliberately does not throw and does
// not allocate. Accessing the wrong alternative is a programming error.
template <typename T>
class [[nodiscard]] Result final {
 public:
  Result(const Result&) = delete;
  Result& operator=(const Result&) = delete;

  Result(Result&& other) noexcept(
      std::is_nothrow_move_constructible<T>::value)
      : has_value_(other.has_value_) {
    if (has_value_) {
      new (&storage_.value) T(std::move(other.storage_.value));
    } else {
      new (&storage_.error) Error(std::move(other.storage_.error));
    }
  }

  Result& operator=(Result&& other) noexcept(
      std::is_nothrow_move_constructible<T>::value) {
    if (this != &other) {
      destroy();
      has_value_ = other.has_value_;
      if (has_value_) {
        new (&storage_.value) T(std::move(other.storage_.value));
      } else {
        new (&storage_.error) Error(std::move(other.storage_.error));
      }
    }
    return *this;
  }

  ~Result() noexcept { destroy(); }

  static Result success(T value) noexcept(
      std::is_nothrow_move_constructible<T>::value) {
    return Result(value_tag{}, std::move(value));
  }

  static Result failure(Error error) noexcept {
    return Result(error_tag{}, std::move(error));
  }

  [[nodiscard]] bool ok() const noexcept { return has_value_; }
  [[nodiscard]] explicit operator bool() const noexcept { return ok(); }

  [[nodiscard]] T& value() & noexcept {
    assert(has_value_);
    return storage_.value;
  }

  [[nodiscard]] const T& value() const& noexcept {
    assert(has_value_);
    return storage_.value;
  }

  [[nodiscard]] T&& value() && noexcept {
    assert(has_value_);
    return std::move(storage_.value);
  }

  [[nodiscard]] Error& error() & noexcept {
    assert(!has_value_);
    return storage_.error;
  }

  [[nodiscard]] const Error& error() const& noexcept {
    assert(!has_value_);
    return storage_.error;
  }

  [[nodiscard]] Error&& error() && noexcept {
    assert(!has_value_);
    return std::move(storage_.error);
  }

 private:
  struct value_tag {};
  struct error_tag {};

  union Storage {
    T value;
    Error error;

    Storage() noexcept {}
    ~Storage() noexcept {}
  } storage_;

  explicit Result(value_tag, T&& value) noexcept(
      std::is_nothrow_move_constructible<T>::value)
      : has_value_(true) {
    new (&storage_.value) T(std::move(value));
  }

  explicit Result(error_tag, Error&& error) noexcept : has_value_(false) {
    new (&storage_.error) Error(std::move(error));
  }

  void destroy() noexcept {
    if (has_value_) {
      storage_.value.~T();
    } else {
      storage_.error.~Error();
    }
  }

  bool has_value_;
};

template <>
class [[nodiscard]] Result<void> final {
 public:
  Result(const Result&) = delete;
  Result& operator=(const Result&) = delete;
  Result(Result&&) noexcept = default;
  Result& operator=(Result&&) noexcept = default;
  ~Result() noexcept = default;

  static Result success() noexcept { return Result(Error{}); }
  static Result failure(Error error) noexcept {
    return Result(std::move(error));
  }

  [[nodiscard]] bool ok() const noexcept { return error_.ok(); }
  [[nodiscard]] explicit operator bool() const noexcept { return ok(); }

  [[nodiscard]] Error& error() & noexcept {
    assert(!ok());
    return error_;
  }

  [[nodiscard]] const Error& error() const& noexcept {
    assert(!ok());
    return error_;
  }

  [[nodiscard]] Error&& error() && noexcept {
    assert(!ok());
    return std::move(error_);
  }

 private:
  explicit Result(Error error) noexcept : error_(std::move(error)) {}
  Error error_;
};

using Status = Result<void>;
using Option = ::paimon_option;

namespace detail {

inline Status status_from(::paimon_error* error) noexcept {
  if (error == nullptr) {
    return Status::success();
  }
  return Status::failure(Error(adopt_handle, error));
}

template <typename Raw, void (*Free)(Raw*)>
class UniqueHandle final {
 public:
  constexpr UniqueHandle() noexcept = default;
  explicit UniqueHandle(adopt_handle_t, Raw* raw) noexcept : raw_(raw) {}

  UniqueHandle(const UniqueHandle&) = delete;
  UniqueHandle& operator=(const UniqueHandle&) = delete;

  UniqueHandle(UniqueHandle&& other) noexcept : raw_(other.release()) {}

  UniqueHandle& operator=(UniqueHandle&& other) noexcept {
    if (this != &other) {
      reset(other.release());
    }
    return *this;
  }

  ~UniqueHandle() noexcept { reset(); }

  [[nodiscard]] Raw* get() const noexcept { return raw_; }
  [[nodiscard]] explicit operator bool() const noexcept {
    return raw_ != nullptr;
  }

  [[nodiscard]] Raw* release() noexcept {
    Raw* result = raw_;
    raw_ = nullptr;
    return result;
  }

  void reset(Raw* raw = nullptr) noexcept {
    if (raw_ != nullptr) {
      Free(raw_);
    }
    raw_ = raw;
  }

 private:
  Raw* raw_ = nullptr;
};

}  // namespace detail

class Identifier;
class Table;
class ReadBuilder;
class Scan;
class Plan;
class TableRead;
class RecordBatchReader;
class WriteBuilder;
class TableWrite;
class PreparedMessages;
class TableCommit;
class PreparedCommit;
class StreamScan;
class StreamPlan;
class PollResult;

enum class StreamStartupMode : std::int32_t {
  latest_full = PAIMON_STREAM_STARTUP_LATEST_FULL,
  latest = PAIMON_STREAM_STARTUP_LATEST,
  from_snapshot = PAIMON_STREAM_STARTUP_FROM_SNAPSHOT,
  from_snapshot_full = PAIMON_STREAM_STARTUP_FROM_SNAPSHOT_FULL,
};

enum class StreamFollowUpMode : std::int32_t {
  automatic = PAIMON_STREAM_FOLLOW_UP_AUTO,
  delta = PAIMON_STREAM_FOLLOW_UP_DELTA,
  changelog = PAIMON_STREAM_FOLLOW_UP_CHANGELOG,
};

enum class StreamPollStatus : std::int32_t {
  data = PAIMON_STREAM_POLL_DATA,
  waiting = PAIMON_STREAM_POLL_WAITING,
  end = PAIMON_STREAM_POLL_END,
};

enum class StreamReadMode : std::int32_t {
  data = PAIMON_STREAM_READ_DATA,
  audit_log = PAIMON_STREAM_READ_AUDIT_LOG,
};

class StreamScanOptions final {
 public:
  StreamScanOptions(const StreamScanOptions&) noexcept = default;
  StreamScanOptions& operator=(const StreamScanOptions&) noexcept = default;
  StreamScanOptions(StreamScanOptions&&) noexcept = default;
  StreamScanOptions& operator=(StreamScanOptions&&) noexcept = default;
  ~StreamScanOptions() noexcept = default;

  [[nodiscard]] static Result<StreamScanOptions> defaults() noexcept;

  StreamScanOptions& with_startup(StreamStartupMode mode,
                                  std::int64_t snapshot_id = -1) noexcept {
    options_.startup_mode = static_cast<std::int32_t>(mode);
    options_.snapshot_id = snapshot_id;
    return *this;
  }

  StreamScanOptions& with_follow_up(StreamFollowUpMode mode) noexcept {
    options_.follow_up_mode = static_cast<std::int32_t>(mode);
    return *this;
  }

  [[nodiscard]] const ::paimon_stream_scan_options* native_handle()
      const noexcept {
    return &options_;
  }

  [[nodiscard]] ::paimon_stream_scan_options* native_handle() noexcept {
    return &options_;
  }

 private:
  explicit StreamScanOptions(::paimon_stream_scan_options options) noexcept
      : options_(options) {}

  ::paimon_stream_scan_options options_{};
};

class Catalog final {
 public:
  Catalog() noexcept = default;
  explicit Catalog(adopt_handle_t tag, ::paimon_catalog* raw) noexcept
      : handle_(tag, raw) {}

  Catalog(const Catalog&) = delete;
  Catalog& operator=(const Catalog&) = delete;
  Catalog(Catalog&&) noexcept = default;
  Catalog& operator=(Catalog&&) noexcept = default;
  ~Catalog() noexcept = default;

  static Result<Catalog> create(const Option* options = nullptr,
                                std::size_t options_len = 0) noexcept;

  template <std::size_t N>
  static Result<Catalog> create(const Option (&options)[N]) noexcept {
    return create(options, N);
  }

  [[nodiscard]] Result<Table> get_table(
      const Identifier& identifier) const noexcept;

  [[nodiscard]] Status create_table_from_schema_json(
      const Identifier& identifier, const char* schema_json,
      bool ignore_if_exists = false) const noexcept;

  [[nodiscard]] Status drop_table(
      const Identifier& identifier,
      bool ignore_if_not_exists = false) const noexcept;

  [[nodiscard]] ::paimon_catalog* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_catalog, ::paimon_catalog_free> handle_;
};

class Identifier final {
 public:
  Identifier() noexcept = default;
  explicit Identifier(adopt_handle_t tag, ::paimon_identifier* raw) noexcept
      : handle_(tag, raw) {}

  Identifier(const Identifier&) = delete;
  Identifier& operator=(const Identifier&) = delete;
  Identifier(Identifier&&) noexcept = default;
  Identifier& operator=(Identifier&&) noexcept = default;
  ~Identifier() noexcept = default;

  static Result<Identifier> create(const char* database,
                                   const char* object) noexcept;

  [[nodiscard]] ::paimon_identifier* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_identifier, ::paimon_identifier_free> handle_;
};

class Table final {
 public:
  Table() noexcept = default;
  explicit Table(adopt_handle_t tag, ::paimon_table* raw) noexcept
      : handle_(tag, raw) {}

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  Table(Table&&) noexcept = default;
  Table& operator=(Table&&) noexcept = default;
  ~Table() noexcept = default;

  static Result<Table> from_schema_json(
      const char* table_path, const char* table_schema_json,
      const char* database, const char* table_name, const char* branch = nullptr,
      const Option* storage_options = nullptr,
      std::size_t storage_options_len = 0) noexcept;

  [[nodiscard]] Result<ReadBuilder> new_read_builder() const noexcept;
  [[nodiscard]] Result<ReadBuilder> new_read_builder(
      const Option* options, std::size_t options_len) const noexcept;

  template <std::size_t N>
  [[nodiscard]] Result<ReadBuilder> new_read_builder(
      const Option (&options)[N]) const noexcept;

  [[nodiscard]] Result<WriteBuilder> new_write_builder() const noexcept;
  [[nodiscard]] Result<WriteBuilder> new_write_builder(
      const char* stable_commit_user) const noexcept;

  [[nodiscard]] ::paimon_table* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_table, ::paimon_table_free> handle_;
};

class ReadBuilder final {
 public:
  ReadBuilder() noexcept = default;
  explicit ReadBuilder(adopt_handle_t tag, ::paimon_read_builder* raw) noexcept
      : handle_(tag, raw) {}

  ReadBuilder(const ReadBuilder&) = delete;
  ReadBuilder& operator=(const ReadBuilder&) = delete;
  ReadBuilder(ReadBuilder&&) noexcept = default;
  ReadBuilder& operator=(ReadBuilder&&) noexcept = default;
  ~ReadBuilder() noexcept = default;

  // columns must be a null-terminated array. Passing nullptr clears projection.
  [[nodiscard]] Status with_projection(
      const char* const* columns) noexcept {
    return detail::status_from(
        ::paimon_read_builder_with_projection(handle_.get(), columns));
  }

  [[nodiscard]] Status with_case_sensitive(bool case_sensitive) noexcept {
    return detail::status_from(::paimon_read_builder_with_case_sensitive(
        handle_.get(), case_sensitive));
  }

  [[nodiscard]] Result<Scan> new_scan() const noexcept;
  [[nodiscard]] Result<TableRead> new_read() const noexcept;
  [[nodiscard]] Result<StreamScan> new_stream_scan(
      const StreamScanOptions& options) const noexcept;

  [[nodiscard]] ::paimon_read_builder* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_read_builder, ::paimon_read_builder_free>
      handle_;
};

class Scan final {
 public:
  Scan() noexcept = default;
  explicit Scan(adopt_handle_t tag, ::paimon_table_scan* raw) noexcept
      : handle_(tag, raw) {}

  Scan(const Scan&) = delete;
  Scan& operator=(const Scan&) = delete;
  Scan(Scan&&) noexcept = default;
  Scan& operator=(Scan&&) noexcept = default;
  ~Scan() noexcept = default;

  [[nodiscard]] Result<Plan> plan() const noexcept;

  [[nodiscard]] ::paimon_table_scan* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_table_scan, ::paimon_table_scan_free> handle_;
};

class Plan final {
 public:
  Plan() noexcept = default;
  explicit Plan(adopt_handle_t tag, ::paimon_plan* raw) noexcept
      : handle_(tag, raw) {}

  Plan(const Plan&) = delete;
  Plan& operator=(const Plan&) = delete;
  Plan(Plan&&) noexcept = default;
  Plan& operator=(Plan&&) noexcept = default;
  ~Plan() noexcept = default;

  static Result<Plan> from_split_bytes(const std::uint8_t* data,
                                       std::size_t size) noexcept;

  [[nodiscard]] std::size_t num_splits() const noexcept {
    return ::paimon_plan_num_splits(handle_.get());
  }

  [[nodiscard]] ::paimon_plan* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_plan, ::paimon_plan_free> handle_;
};

// Owns the two heap-allocated Arrow C Data container structs returned by
// libpaimon_c. This type intentionally has no dependency on Arrow C++.
class ArrowBatch final {
 public:
  constexpr ArrowBatch() noexcept : batch_{nullptr, nullptr} {}
  explicit constexpr ArrowBatch(adopt_handle_t,
                                ::paimon_arrow_batch batch) noexcept
      : batch_(batch) {}

  ArrowBatch(const ArrowBatch&) = delete;
  ArrowBatch& operator=(const ArrowBatch&) = delete;

  ArrowBatch(ArrowBatch&& other) noexcept : batch_(other.release()) {}

  ArrowBatch& operator=(ArrowBatch&& other) noexcept {
    if (this != &other) {
      reset();
      batch_ = other.release();
    }
    return *this;
  }

  ~ArrowBatch() noexcept { reset(); }

  [[nodiscard]] bool empty() const noexcept {
    return batch_.array == nullptr && batch_.schema == nullptr;
  }

  [[nodiscard]] explicit operator bool() const noexcept { return !empty(); }
  [[nodiscard]] void* array() const noexcept { return batch_.array; }
  [[nodiscard]] void* schema() const noexcept { return batch_.schema; }

  [[nodiscard]] ::paimon_arrow_batch native_handle() const noexcept {
    return batch_;
  }

  // The caller becomes responsible for paimon_arrow_batch_free(raw).
  [[nodiscard]] ::paimon_arrow_batch release() noexcept {
    const auto result = batch_;
    batch_ = {nullptr, nullptr};
    return result;
  }

  void reset() noexcept {
    if (!empty()) {
      ::paimon_arrow_batch_free(batch_);
      batch_ = {nullptr, nullptr};
    }
  }

 private:
  ::paimon_arrow_batch batch_;
};

class RecordBatchReader final {
 public:
  RecordBatchReader() noexcept = default;
  explicit RecordBatchReader(adopt_handle_t tag,
                             ::paimon_record_batch_reader* raw) noexcept
      : handle_(tag, raw) {}

  RecordBatchReader(const RecordBatchReader&) = delete;
  RecordBatchReader& operator=(const RecordBatchReader&) = delete;
  RecordBatchReader(RecordBatchReader&&) noexcept = default;
  RecordBatchReader& operator=(RecordBatchReader&&) noexcept = default;
  ~RecordBatchReader() noexcept = default;

  // A successful empty ArrowBatch is end-of-stream.
  [[nodiscard]] Result<ArrowBatch> next() noexcept;

  [[nodiscard]] ::paimon_record_batch_reader* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_record_batch_reader,
                       ::paimon_record_batch_reader_free>
      handle_;
};

class TableRead final {
 public:
  TableRead() noexcept = default;
  explicit TableRead(adopt_handle_t tag, ::paimon_table_read* raw) noexcept
      : handle_(tag, raw) {}

  TableRead(const TableRead&) = delete;
  TableRead& operator=(const TableRead&) = delete;
  TableRead(TableRead&&) noexcept = default;
  TableRead& operator=(TableRead&&) noexcept = default;
  ~TableRead() noexcept = default;

  [[nodiscard]] Result<RecordBatchReader> to_arrow(
      const Plan& plan, std::size_t offset = 0,
      std::size_t length = static_cast<std::size_t>(-1)) const noexcept;

  [[nodiscard]] ::paimon_table_read* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_table_read, ::paimon_table_read_free> handle_;
};

class StreamPlan final {
 public:
  StreamPlan() noexcept = default;
  explicit StreamPlan(adopt_handle_t tag, ::paimon_stream_plan* raw) noexcept
      : handle_(tag, raw) {}

  StreamPlan(const StreamPlan&) = delete;
  StreamPlan& operator=(const StreamPlan&) = delete;
  StreamPlan(StreamPlan&&) noexcept = default;
  StreamPlan& operator=(StreamPlan&&) noexcept = default;
  ~StreamPlan() noexcept = default;

  [[nodiscard]] bool is_full() const noexcept {
    return ::paimon_stream_plan_is_full(handle_.get()) != 0;
  }

  [[nodiscard]] std::size_t num_splits() const noexcept {
    return ::paimon_stream_plan_num_splits(handle_.get());
  }

  [[nodiscard]] Result<Bytes> serialize() const noexcept;

  [[nodiscard]] static Result<PollResult> deserialize(
      const std::uint8_t* data, std::size_t size) noexcept;

  [[nodiscard]] Result<RecordBatchReader> read_to_arrow(
      const TableRead& read, StreamReadMode mode = StreamReadMode::data,
      std::size_t offset = 0,
      std::size_t length = static_cast<std::size_t>(-1)) const noexcept;

  [[nodiscard]] ::paimon_stream_plan* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_stream_plan, ::paimon_stream_plan_free> handle_;
};

class PollResult final {
 public:
  PollResult(StreamPollStatus status, StreamPlan plan,
             std::int64_t snapshot_id, std::int64_t next_snapshot_id,
             std::int64_t watermark, bool has_watermark) noexcept
      : status_(status),
        plan_(std::move(plan)),
        snapshot_id_(snapshot_id),
        next_snapshot_id_(next_snapshot_id),
        watermark_(watermark),
        has_watermark_(has_watermark) {}

  PollResult(const PollResult&) = delete;
  PollResult& operator=(const PollResult&) = delete;
  PollResult(PollResult&&) noexcept = default;
  PollResult& operator=(PollResult&&) noexcept = default;
  ~PollResult() noexcept = default;

  [[nodiscard]] StreamPollStatus status() const noexcept { return status_; }
  [[nodiscard]] bool has_data() const noexcept {
    return status_ == StreamPollStatus::data;
  }
  [[nodiscard]] bool waiting() const noexcept {
    return status_ == StreamPollStatus::waiting;
  }
  [[nodiscard]] bool end() const noexcept {
    return status_ == StreamPollStatus::end;
  }

  [[nodiscard]] StreamPlan& plan() & noexcept {
    assert(has_data());
    return plan_;
  }

  [[nodiscard]] const StreamPlan& plan() const& noexcept {
    assert(has_data());
    return plan_;
  }

  [[nodiscard]] StreamPlan&& plan() && noexcept {
    assert(has_data());
    return std::move(plan_);
  }

  [[nodiscard]] std::int64_t snapshot_id() const noexcept {
    return snapshot_id_;
  }

  [[nodiscard]] std::int64_t next_snapshot_id() const noexcept {
    return next_snapshot_id_;
  }

  [[nodiscard]] bool has_watermark() const noexcept { return has_watermark_; }

  [[nodiscard]] std::int64_t watermark() const noexcept {
    assert(has_watermark_);
    return watermark_;
  }

 private:
  StreamPollStatus status_;
  StreamPlan plan_;
  std::int64_t snapshot_id_;
  std::int64_t next_snapshot_id_;
  std::int64_t watermark_;
  bool has_watermark_;
};

class StreamScan final {
 public:
  StreamScan() noexcept = default;
  explicit StreamScan(adopt_handle_t tag, ::paimon_stream_scan* raw) noexcept
      : handle_(tag, raw) {}

  StreamScan(const StreamScan&) = delete;
  StreamScan& operator=(const StreamScan&) = delete;
  StreamScan(StreamScan&&) noexcept = default;
  StreamScan& operator=(StreamScan&&) noexcept = default;
  ~StreamScan() noexcept = default;

  // poll() never waits for a future snapshot. Waiting is a normal result, not
  // an error, so the caller controls scheduling, cancellation and backpressure.
  // One StreamScan is single-thread-confined; poll/checkpoint/restore/free must
  // be externally serialized.
  [[nodiscard]] Result<PollResult> poll() noexcept;

  // This cursor is safe to persist only after every split in the returned plan
  // has been durably accounted for by the caller's checkpoint barrier.
  [[nodiscard]] std::int64_t checkpoint() const noexcept {
    return ::paimon_stream_scan_checkpoint(handle_.get());
  }

  [[nodiscard]] Status restore(std::int64_t next_snapshot_id) noexcept {
    return detail::status_from(
        ::paimon_stream_scan_restore(handle_.get(), next_snapshot_id));
  }

  [[nodiscard]] ::paimon_stream_scan* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_stream_scan, ::paimon_stream_scan_free> handle_;
};

class PreparedMessages final {
 public:
  PreparedMessages() noexcept = default;
  explicit PreparedMessages(adopt_handle_t tag,
                            ::paimon_commit_messages* raw) noexcept
      : handle_(tag, raw) {}

  PreparedMessages(const PreparedMessages&) = delete;
  PreparedMessages& operator=(const PreparedMessages&) = delete;
  PreparedMessages(PreparedMessages&&) noexcept = default;
  PreparedMessages& operator=(PreparedMessages&&) noexcept = default;

  // Destruction only frees the messages. It never commits or aborts files.
  ~PreparedMessages() noexcept = default;

  [[nodiscard]] Status merge(const PreparedMessages& source) noexcept {
    return detail::status_from(::paimon_commit_messages_merge(
        handle_.get(), source.handle_.get()));
  }

  [[nodiscard]] Result<PreparedCommit> prepare(
      std::int64_t checkpoint_id) const noexcept;

  [[nodiscard]] ::paimon_commit_messages* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_commit_messages,
                       ::paimon_commit_messages_free>
      handle_;
};

class PreparedCommit final {
 public:
  PreparedCommit() noexcept = default;
  explicit PreparedCommit(adopt_handle_t tag,
                          ::paimon_prepared_commit* raw) noexcept
      : handle_(tag, raw) {}

  PreparedCommit(const PreparedCommit&) = delete;
  PreparedCommit& operator=(const PreparedCommit&) = delete;
  PreparedCommit(PreparedCommit&&) noexcept = default;
  PreparedCommit& operator=(PreparedCommit&&) noexcept = default;

  // Destruction only releases the durable in-memory envelope. It never commits
  // or aborts the referenced data files.
  ~PreparedCommit() noexcept = default;

  [[nodiscard]] static Result<PreparedCommit> deserialize(
      const std::uint8_t* data, std::size_t size) noexcept;

  [[nodiscard]] std::int64_t identifier() const noexcept {
    return ::paimon_prepared_commit_identifier(handle_.get());
  }

  [[nodiscard]] Result<Bytes> serialize() const noexcept;

  [[nodiscard]] Status merge(const PreparedCommit& source) noexcept {
    return detail::status_from(::paimon_prepared_commit_merge(
        handle_.get(), source.handle_.get()));
  }

  [[nodiscard]] ::paimon_prepared_commit* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_prepared_commit,
                       ::paimon_prepared_commit_free>
      handle_;
};

class TableWrite final {
 public:
  TableWrite() noexcept = default;
  explicit TableWrite(adopt_handle_t tag, ::paimon_table_write* raw) noexcept
      : handle_(tag, raw) {}

  TableWrite(const TableWrite&) = delete;
  TableWrite& operator=(const TableWrite&) = delete;
  TableWrite(TableWrite&&) noexcept = default;
  TableWrite& operator=(TableWrite&&) noexcept = default;
  ~TableWrite() noexcept = default;

  // The Arrow C Data contents are consumed in place once import begins. The
  // caller continues to own the ArrowArray/ArrowSchema container memory.
  [[nodiscard]] Status write_arrow(void* array, void* schema) noexcept {
    return detail::status_from(::paimon_table_write_write_arrow_batch(
        handle_.get(), array, schema));
  }

  // Convenient bridge for a Rust-allocated batch. Its heap container structs
  // remain owned by batch and are released before this call returns.
  [[nodiscard]] Status write_arrow(ArrowBatch&& batch) noexcept {
    auto status = write_arrow(batch.array(), batch.schema());
    batch.reset();
    return status;
  }

  [[nodiscard]] Result<PreparedMessages> prepare_commit() noexcept;

  [[nodiscard]] ::paimon_table_write* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_table_write, ::paimon_table_write_free> handle_;
};

class TableCommit final {
 public:
  TableCommit() noexcept = default;
  explicit TableCommit(adopt_handle_t tag, ::paimon_table_commit* raw) noexcept
      : handle_(tag, raw) {}

  TableCommit(const TableCommit&) = delete;
  TableCommit& operator=(const TableCommit&) = delete;
  TableCommit(TableCommit&&) noexcept = default;
  TableCommit& operator=(TableCommit&&) noexcept = default;
  ~TableCommit() noexcept = default;

  // Commit calls never consume messages. Keep them until the outcome is known;
  // retry an uncertain outcome with filter_and_commit(checkpoint_id).
  [[nodiscard]] Status commit(PreparedMessages& messages) const noexcept {
    return detail::status_from(::paimon_table_commit_commit(
        handle_.get(), messages.native_handle()));
  }

  [[nodiscard]] Status commit(PreparedMessages& messages,
                              std::int64_t checkpoint_id) const noexcept {
    return detail::status_from(::paimon_table_commit_commit_with_identifier(
        handle_.get(), messages.native_handle(), checkpoint_id));
  }

  [[nodiscard]] Status filter_and_commit(
      PreparedMessages& messages, std::int64_t checkpoint_id) const noexcept {
    return detail::status_from(
        ::paimon_table_commit_filter_and_commit_with_identifier(
            handle_.get(), messages.native_handle(), checkpoint_id));
  }

  // Retry-safe commit for a serialized/restored checkpoint. The PreparedCommit
  // remains owned by the caller and can be retried after an uncertain result.
  [[nodiscard]] Status commit_prepared(
      const PreparedCommit& prepared) const noexcept {
    return detail::status_from(::paimon_table_commit_commit_prepared(
        handle_.get(), prepared.native_handle()));
  }

  [[nodiscard]] Status overwrite(PreparedMessages& messages) const noexcept {
    return detail::status_from(::paimon_table_commit_overwrite(
        handle_.get(), messages.native_handle()));
  }

  [[nodiscard]] Status overwrite(PreparedMessages& messages,
                                 std::int64_t checkpoint_id) const noexcept {
    return detail::status_from(
        ::paimon_table_commit_overwrite_with_identifier(
            handle_.get(), messages.native_handle(), checkpoint_id));
  }

  [[nodiscard]] Status truncate_table() const noexcept {
    return detail::status_from(
        ::paimon_table_commit_truncate_table(handle_.get()));
  }

  [[nodiscard]] Status truncate_table(
      std::int64_t checkpoint_id) const noexcept {
    return detail::status_from(
        ::paimon_table_commit_truncate_table_with_identifier(
            handle_.get(), checkpoint_id));
  }

  // Abort is always explicit. PreparedMessages destruction does not call it.
  [[nodiscard]] Status abort(PreparedMessages& messages) const noexcept {
    return detail::status_from(::paimon_table_commit_abort(
        handle_.get(), messages.native_handle()));
  }

  // Fence all commit/abort calls for the same table and commit_user across
  // processes. Truncated snapshot history is reported as an error and no file
  // is removed.
  [[nodiscard]] Status abort_prepared(
      const PreparedCommit& prepared) const noexcept {
    return detail::status_from(::paimon_table_commit_abort_prepared(
        handle_.get(), prepared.native_handle()));
  }

  [[nodiscard]] ::paimon_table_commit* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_table_commit, ::paimon_table_commit_free>
      handle_;
};

class WriteBuilder final {
 public:
  WriteBuilder() noexcept = default;
  explicit WriteBuilder(adopt_handle_t tag, ::paimon_write_builder* raw) noexcept
      : handle_(tag, raw) {}

  WriteBuilder(const WriteBuilder&) = delete;
  WriteBuilder& operator=(const WriteBuilder&) = delete;
  WriteBuilder(WriteBuilder&&) noexcept = default;
  WriteBuilder& operator=(WriteBuilder&&) noexcept = default;
  ~WriteBuilder() noexcept = default;

  [[nodiscard]] Status with_overwrite() noexcept {
    return detail::status_from(
        ::paimon_write_builder_with_overwrite(handle_.get()));
  }

  [[nodiscard]] Result<TableWrite> new_write() const noexcept;
  [[nodiscard]] Result<TableCommit> new_commit() const noexcept;

  [[nodiscard]] ::paimon_write_builder* native_handle() const noexcept {
    return handle_.get();
  }

 private:
  detail::UniqueHandle<::paimon_write_builder, ::paimon_write_builder_free>
      handle_;
};

inline Result<Catalog> Catalog::create(const Option* options,
                                       std::size_t options_len) noexcept {
  const auto result = ::paimon_catalog_create(options, options_len);
  if (result.error != nullptr) {
    if (result.catalog != nullptr) {
      ::paimon_catalog_free(result.catalog);
    }
    return Result<Catalog>::failure(Error(adopt_handle, result.error));
  }
  return Result<Catalog>::success(
      Catalog(adopt_handle, result.catalog));
}

inline Result<StreamScanOptions> StreamScanOptions::defaults() noexcept {
  ::paimon_stream_scan_options options{};
  auto* error = ::paimon_stream_scan_options_init(&options);
  if (error != nullptr) {
    return Result<StreamScanOptions>::failure(Error(adopt_handle, error));
  }
  return Result<StreamScanOptions>::success(StreamScanOptions(options));
}

inline Result<Identifier> Identifier::create(const char* database,
                                             const char* object) noexcept {
  const auto result = ::paimon_identifier_new(database, object);
  if (result.error != nullptr) {
    if (result.identifier != nullptr) {
      ::paimon_identifier_free(result.identifier);
    }
    return Result<Identifier>::failure(Error(adopt_handle, result.error));
  }
  return Result<Identifier>::success(
      Identifier(adopt_handle, result.identifier));
}

inline Result<Table> Catalog::get_table(
    const Identifier& identifier) const noexcept {
  const auto result =
      ::paimon_catalog_get_table(handle_.get(), identifier.native_handle());
  if (result.error != nullptr) {
    if (result.table != nullptr) {
      ::paimon_table_free(result.table);
    }
    return Result<Table>::failure(Error(adopt_handle, result.error));
  }
  return Result<Table>::success(Table(adopt_handle, result.table));
}

inline Status Catalog::create_table_from_schema_json(
    const Identifier& identifier, const char* schema_json,
    bool ignore_if_exists) const noexcept {
  return detail::status_from(::paimon_catalog_create_table_from_schema_json(
      handle_.get(), identifier.native_handle(), schema_json,
      ignore_if_exists));
}

inline Status Catalog::drop_table(const Identifier& identifier,
                                  bool ignore_if_not_exists) const noexcept {
  return detail::status_from(::paimon_catalog_drop_table(
      handle_.get(), identifier.native_handle(), ignore_if_not_exists));
}

inline Result<Table> Table::from_schema_json(
    const char* table_path, const char* table_schema_json, const char* database,
    const char* table_name, const char* branch, const Option* storage_options,
    std::size_t storage_options_len) noexcept {
  const auto result = ::paimon_table_from_schema_json(
      table_path, table_schema_json, database, table_name, branch,
      storage_options, storage_options_len);
  if (result.error != nullptr) {
    if (result.table != nullptr) {
      ::paimon_table_free(result.table);
    }
    return Result<Table>::failure(Error(adopt_handle, result.error));
  }
  return Result<Table>::success(Table(adopt_handle, result.table));
}

inline Result<ReadBuilder> Table::new_read_builder() const noexcept {
  const auto result = ::paimon_table_new_read_builder(handle_.get());
  if (result.error != nullptr) {
    if (result.read_builder != nullptr) {
      ::paimon_read_builder_free(result.read_builder);
    }
    return Result<ReadBuilder>::failure(Error(adopt_handle, result.error));
  }
  return Result<ReadBuilder>::success(
      ReadBuilder(adopt_handle, result.read_builder));
}

inline Result<ReadBuilder> Table::new_read_builder(
    const Option* options, std::size_t options_len) const noexcept {
  const auto result = ::paimon_table_new_read_builder_with_options(
      handle_.get(), options, options_len);
  if (result.error != nullptr) {
    if (result.read_builder != nullptr) {
      ::paimon_read_builder_free(result.read_builder);
    }
    return Result<ReadBuilder>::failure(Error(adopt_handle, result.error));
  }
  return Result<ReadBuilder>::success(
      ReadBuilder(adopt_handle, result.read_builder));
}

template <std::size_t N>
inline Result<ReadBuilder> Table::new_read_builder(
    const Option (&options)[N]) const noexcept {
  return new_read_builder(options, N);
}

inline Result<Scan> ReadBuilder::new_scan() const noexcept {
  const auto result = ::paimon_read_builder_new_scan(handle_.get());
  if (result.error != nullptr) {
    if (result.scan != nullptr) {
      ::paimon_table_scan_free(result.scan);
    }
    return Result<Scan>::failure(Error(adopt_handle, result.error));
  }
  return Result<Scan>::success(Scan(adopt_handle, result.scan));
}

inline Result<TableRead> ReadBuilder::new_read() const noexcept {
  const auto result = ::paimon_read_builder_new_read(handle_.get());
  if (result.error != nullptr) {
    if (result.read != nullptr) {
      ::paimon_table_read_free(result.read);
    }
    return Result<TableRead>::failure(Error(adopt_handle, result.error));
  }
  return Result<TableRead>::success(TableRead(adopt_handle, result.read));
}

inline Result<StreamScan> ReadBuilder::new_stream_scan(
    const StreamScanOptions& options) const noexcept {
  const auto result = ::paimon_read_builder_new_stream_scan(
      handle_.get(), options.native_handle());
  if (result.error != nullptr) {
    if (result.scan != nullptr) {
      ::paimon_stream_scan_free(result.scan);
    }
    return Result<StreamScan>::failure(Error(adopt_handle, result.error));
  }
  return Result<StreamScan>::success(StreamScan(adopt_handle, result.scan));
}

inline Result<Plan> Scan::plan() const noexcept {
  const auto result = ::paimon_table_scan_plan(handle_.get());
  if (result.error != nullptr) {
    if (result.plan != nullptr) {
      ::paimon_plan_free(result.plan);
    }
    return Result<Plan>::failure(Error(adopt_handle, result.error));
  }
  return Result<Plan>::success(Plan(adopt_handle, result.plan));
}

inline Result<Plan> Plan::from_split_bytes(const std::uint8_t* data,
                                           std::size_t size) noexcept {
  const auto result = ::paimon_plan_from_split_bytes(data, size);
  if (result.error != nullptr) {
    if (result.plan != nullptr) {
      ::paimon_plan_free(result.plan);
    }
    return Result<Plan>::failure(Error(adopt_handle, result.error));
  }
  return Result<Plan>::success(Plan(adopt_handle, result.plan));
}

inline Result<RecordBatchReader> TableRead::to_arrow(
    const Plan& plan, std::size_t offset, std::size_t length) const noexcept {
  const auto result = ::paimon_table_read_to_arrow(
      handle_.get(), plan.native_handle(), offset, length);
  if (result.error != nullptr) {
    if (result.reader != nullptr) {
      ::paimon_record_batch_reader_free(result.reader);
    }
    return Result<RecordBatchReader>::failure(
        Error(adopt_handle, result.error));
  }
  return Result<RecordBatchReader>::success(
      RecordBatchReader(adopt_handle, result.reader));
}

inline Result<ArrowBatch> RecordBatchReader::next() noexcept {
  auto result = ::paimon_record_batch_reader_next(handle_.get());
  if (result.error != nullptr) {
    if (result.batch.array != nullptr || result.batch.schema != nullptr) {
      ::paimon_arrow_batch_free(result.batch);
    }
    return Result<ArrowBatch>::failure(Error(adopt_handle, result.error));
  }
  return Result<ArrowBatch>::success(
      ArrowBatch(adopt_handle, result.batch));
}

inline Result<RecordBatchReader> StreamPlan::read_to_arrow(
    const TableRead& read, StreamReadMode mode, std::size_t offset,
    std::size_t length) const noexcept {
  const auto result = ::paimon_stream_plan_read_to_arrow(
      read.native_handle(), handle_.get(), offset, length,
      static_cast<std::int32_t>(mode));
  if (result.error != nullptr) {
    if (result.reader != nullptr) {
      ::paimon_record_batch_reader_free(result.reader);
    }
    return Result<RecordBatchReader>::failure(
        Error(adopt_handle, result.error));
  }
  return Result<RecordBatchReader>::success(
      RecordBatchReader(adopt_handle, result.reader));
}

inline Result<Bytes> StreamPlan::serialize() const noexcept {
  auto result = ::paimon_stream_plan_serialize(handle_.get());
  if (result.error != nullptr) {
    if (result.bytes.data != nullptr) {
      ::paimon_bytes_free(result.bytes);
    }
    return Result<Bytes>::failure(Error(adopt_handle, result.error));
  }
  return Result<Bytes>::success(Bytes(adopt_handle, result.bytes));
}

inline Result<PollResult> StreamPlan::deserialize(
    const std::uint8_t* data, std::size_t size) noexcept {
  auto result = ::paimon_stream_plan_deserialize(data, size);
  if (result.error != nullptr) {
    if (result.plan != nullptr) {
      ::paimon_stream_plan_free(result.plan);
    }
    return Result<PollResult>::failure(Error(adopt_handle, result.error));
  }
  return Result<PollResult>::success(PollResult(
      static_cast<StreamPollStatus>(result.status),
      StreamPlan(adopt_handle, result.plan), result.snapshot_id,
      result.next_snapshot_id, result.watermark, result.has_watermark != 0));
}

inline Result<PollResult> StreamScan::poll() noexcept {
  auto result = ::paimon_stream_scan_poll(handle_.get());
  if (result.error != nullptr) {
    if (result.plan != nullptr) {
      ::paimon_stream_plan_free(result.plan);
    }
    return Result<PollResult>::failure(Error(adopt_handle, result.error));
  }
  return Result<PollResult>::success(PollResult(
      static_cast<StreamPollStatus>(result.status),
      StreamPlan(adopt_handle, result.plan), result.snapshot_id,
      result.next_snapshot_id, result.watermark, result.has_watermark != 0));
}

inline Result<WriteBuilder> Table::new_write_builder() const noexcept {
  const auto result = ::paimon_table_new_write_builder(handle_.get());
  if (result.error != nullptr) {
    if (result.write_builder != nullptr) {
      ::paimon_write_builder_free(result.write_builder);
    }
    return Result<WriteBuilder>::failure(Error(adopt_handle, result.error));
  }
  return Result<WriteBuilder>::success(
      WriteBuilder(adopt_handle, result.write_builder));
}

inline Result<WriteBuilder> Table::new_write_builder(
    const char* stable_commit_user) const noexcept {
  const auto result = ::paimon_table_new_write_builder_with_commit_user(
      handle_.get(), stable_commit_user);
  if (result.error != nullptr) {
    if (result.write_builder != nullptr) {
      ::paimon_write_builder_free(result.write_builder);
    }
    return Result<WriteBuilder>::failure(Error(adopt_handle, result.error));
  }
  return Result<WriteBuilder>::success(
      WriteBuilder(adopt_handle, result.write_builder));
}

inline Result<TableWrite> WriteBuilder::new_write() const noexcept {
  const auto result = ::paimon_write_builder_new_write(handle_.get());
  if (result.error != nullptr) {
    if (result.write != nullptr) {
      ::paimon_table_write_free(result.write);
    }
    return Result<TableWrite>::failure(Error(adopt_handle, result.error));
  }
  return Result<TableWrite>::success(TableWrite(adopt_handle, result.write));
}

inline Result<TableCommit> WriteBuilder::new_commit() const noexcept {
  const auto result = ::paimon_write_builder_new_commit(handle_.get());
  if (result.error != nullptr) {
    if (result.commit != nullptr) {
      ::paimon_table_commit_free(result.commit);
    }
    return Result<TableCommit>::failure(Error(adopt_handle, result.error));
  }
  return Result<TableCommit>::success(
      TableCommit(adopt_handle, result.commit));
}

inline Result<PreparedMessages> TableWrite::prepare_commit() noexcept {
  const auto result = ::paimon_table_write_prepare_commit(handle_.get());
  if (result.error != nullptr) {
    if (result.messages != nullptr) {
      ::paimon_commit_messages_free(result.messages);
    }
    return Result<PreparedMessages>::failure(
        Error(adopt_handle, result.error));
  }
  return Result<PreparedMessages>::success(
      PreparedMessages(adopt_handle, result.messages));
}

inline Result<PreparedCommit> PreparedMessages::prepare(
    std::int64_t checkpoint_id) const noexcept {
  const auto result =
      ::paimon_commit_messages_prepare(handle_.get(), checkpoint_id);
  if (result.error != nullptr) {
    if (result.prepared != nullptr) {
      ::paimon_prepared_commit_free(result.prepared);
    }
    return Result<PreparedCommit>::failure(Error(adopt_handle, result.error));
  }
  return Result<PreparedCommit>::success(
      PreparedCommit(adopt_handle, result.prepared));
}

inline Result<Bytes> PreparedCommit::serialize() const noexcept {
  auto result = ::paimon_prepared_commit_serialize(handle_.get());
  if (result.error != nullptr) {
    if (result.bytes.data != nullptr) {
      ::paimon_bytes_free(result.bytes);
    }
    return Result<Bytes>::failure(Error(adopt_handle, result.error));
  }
  return Result<Bytes>::success(Bytes(adopt_handle, result.bytes));
}

inline Result<PreparedCommit> PreparedCommit::deserialize(
    const std::uint8_t* data, std::size_t size) noexcept {
  const auto result = ::paimon_prepared_commit_deserialize(data, size);
  if (result.error != nullptr) {
    if (result.prepared != nullptr) {
      ::paimon_prepared_commit_free(result.prepared);
    }
    return Result<PreparedCommit>::failure(Error(adopt_handle, result.error));
  }
  return Result<PreparedCommit>::success(
      PreparedCommit(adopt_handle, result.prepared));
}

static_assert(!std::is_copy_constructible<Catalog>::value,
              "native handles must stay move-only");
static_assert(std::is_nothrow_destructible<Catalog>::value,
              "native handle destructors must be noexcept");
static_assert(!std::is_copy_constructible<PreparedMessages>::value,
              "prepared messages must stay move-only");
static_assert(std::is_nothrow_destructible<PreparedMessages>::value,
              "prepared-message destruction must be noexcept");
static_assert(!std::is_copy_constructible<StreamScan>::value,
              "stream scans must stay move-only");
static_assert(std::is_nothrow_destructible<StreamPlan>::value,
              "stream plan destruction must be noexcept");
static_assert(!std::is_copy_constructible<PreparedCommit>::value,
              "durable prepared commits must stay move-only");

}  // namespace paimon

#endif  // PAIMON_CPP_PAIMON_HPP
