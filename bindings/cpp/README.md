<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Paimon C++ facade

This directory provides a header-only C++17 RAII facade over the stable Paimon
C ABI. It deliberately builds no C++ shared library: the only Paimon binary is
`libpaimon_c`, produced by Rust, and every symbol called by the facade has C
linkage. The facade does not depend on Arrow C++; Arrow batches cross the API as
raw Arrow C Data `array` and `schema` pointers.

All native handles are move-only. Their destructors are `noexcept` and only
release resources. In particular, destroying `PreparedMessages` or
`PreparedCommit` never commits or aborts them. A streaming writer binds messages
to a checkpoint with `PreparedMessages::prepare`, persists the bytes from
`PreparedCommit::serialize`, then calls `TableCommit::commit_prepared`. After an
uncertain result or process restart, deserialize the same bytes and retry with
the same stable `commit_user`; the identifier path filters duplicate commits.
Exactly-once filtering is recorded in retained snapshot metadata. Keep snapshot
history for at least the maximum writer-recovery horizon, never retry a
checkpoint older than that horizon, and give each fresh job a new globally
unique `commit_user`. Checkpoint identifiers must be in `[0, INT64_MAX)`;
`INT64_MAX` is reserved for unidentified batch commits.
Checkpoint blobs are trusted state, not a security token: store them behind
normal integrity/access controls. Before `abort_prepared`, fence every commit
and abort for the same `(table, commit_user)` across processes. If snapshot
history is too old to prove safety, abort fails closed and leaves cleanup to an
orphan-file policy.
Continuous reading is a pull API. `StreamScan::poll` immediately returns data,
waiting, or end; it never starts a callback thread and never waits for a future
snapshot. A data result owns a `StreamPlan`, which can be read in data or audit
log mode using the same Arrow C Data `RecordBatchReader` as bounded reads.
Each `StreamScan` is single-thread-confined; serialize poll, checkpoint,
restore, and destruction. Decoupled changelog fallback and consumer-retention
registration are not implemented yet, so snapshot retention must cover the
maximum expected reader lag.
Persisted stream plans currently reject external data-file paths. The failure
is reported by `StreamPlan::serialize` before a checkpoint can be acknowledged,
instead of producing a checkpoint that cannot be restored.

Catalog DDL is available directly from the facade. Creation accepts the JSON
form of Paimon's logical `Schema`; it validates and canonically reassigns field
IDs before calling the catalog. Both operations return `Status`, so callers can
choose strict or idempotent create/drop semantics without a Java helper:

```cpp
auto identifier = paimon::Identifier::create("default", "events");
auto created = catalog.create_table_from_schema_json(
    identifier.value(), schema_json, /*ignore_if_exists=*/false);
auto dropped = catalog.drop_table(
    identifier.value(), /*ignore_if_not_exists=*/true);
```

## Build

Configure and build the C++ facade directly. The build always compiles the
in-tree `bindings/c` crate first, so the C and C++ layers come from the same
source revision:

```bash
cmake -S bindings/cpp -B target/cpp-build \
  -DPAIMON_CPP_BUILD_EXAMPLES=ON
cmake --build target/cpp-build
```

The CMake build directory itself is a complete, directly consumable artifact
tree:

```text
target/cpp-build/
├── include/paimon.h
├── include/paimon/paimon.hpp
├── <libdir>/libpaimon_c.so       # Linux
└── <libdir>/cmake/PaimonCpp/
```

`<libdir>` follows GNUInstallDirs and is normally `lib` or `lib64`. macOS uses
`libpaimon_c.dylib` in the same location. The facade is header-only, so there is
intentionally no separate `libpaimon_cpp` shared library.

All platforms use `cargo build --locked --release -p paimon-c`. Build Linux
release artifacts on the oldest glibc version that must be supported; glibc is
backward compatible with binaries built against older symbol versions.
External prebuilt paimon-c libraries and parent-provided `Paimon::c` targets
are deliberately unsupported.

Linux builds use the OpenSSL selected by the locked `openssl-sys` dependency
and link it dynamically. OpenSSL 1.0.2 is not supported by the current lock;
an old-glibc build host must provide a parallel OpenSSL 1.1 or newer development
installation. The resulting package requires that exact OpenSSL SONAME at
runtime.

Source builds require `cbindgen`. CMake regenerates `paimon.h` from the Rust C
ABI in `target/cpp-build/generated/include`; the generated header is not stored
in Git. The staged build tree and every installed package still contain it at
`include/paimon.h`.

Install the CMake interface target elsewhere when needed:

```bash
cmake --install target/cpp-build --prefix /your/prefix
```

Installation always bundles the just-built `libpaimon_c` and exports
`Paimon::c` plus the header-only `Paimon::cpp` target. A consumer only needs:

```cmake
find_package(PaimonCpp CONFIG REQUIRED)
add_executable(my_paimon_app main.cpp)
target_link_libraries(my_paimon_app PRIVATE Paimon::cpp)
```

## Linux packages

The CPack `package` target builds all supported Linux package formats in one
run after compiling the in-tree Rust library:

```bash
cmake -S bindings/cpp -B target/cpp-build
cmake --build target/cpp-build --target package
ls target/cpp-build/packages
```

It produces a Debian/Ubuntu `paimon-cpp-dev` DEB, an RPM-family
`paimon-cpp-devel` RPM, and a `paimon-cpp-sdk` TGZ, plus a SHA-256 checksum for
each package. Building the RPM requires the distribution's `rpmbuild` tool.
Install the native package with, for example:

```bash
sudo apt install ./paimon-cpp-dev_*.deb
sudo dnf install ./paimon-cpp-devel-*.rpm
```

All formats from one run contain the same `libpaimon_c.so`. Package format does
not change its glibc or OpenSSL ABI: build on each binary compatibility baseline
that customers need. Publish a DEB from a Debian/Ubuntu baseline and an RPM from
an RPM-family baseline so native library-directory and dependency conventions
match the target distribution. The TGZ is the format-neutral fallback and
contains the same `/usr` installation tree.

`Scan::plan()` remains a bounded scan. Use `StreamScanOptions` and
`ReadBuilder::new_stream_scan` for a stateful continuous scan. Persist
`StreamScan::checkpoint()` only after every split in the returned plan has been
durably accounted for by the surrounding checkpoint barrier.
