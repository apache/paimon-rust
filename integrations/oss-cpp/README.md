<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# OSS C++ SDK backend (experimental, read-only)

An opt-in, read-only FileIO backend using [Alibaba Cloud OSS C++ SDK v2](https://github.com/aliyun/alibabacloud-oss-cpp-sdk-v2)
for HEAD, Range GET and paginated LIST. OpenDAL remains the default.

## Build

Install the SDK, then build the bridge and enable the Rust feature:

```bash
cmake -S integrations/oss-cpp -B build/oss-cpp \
  -DCMAKE_PREFIX_PATH=/path/to/oss-sdk-install
cmake --build build/oss-cpp
cargo build -p paimon --features storage-oss-cpp
```

The Python binding already includes this feature, but neither the bridge nor
the SDK is bundled in its wheel. Install both on each worker. Tested on Linux
with SDK 0.2.0 (`c13a82038b0696f99c32f4e86b623ffcc9e62140`).

## Configure

Set the native table/catalog FileIO options:

```text
fs.oss.impl = cpp
fs.oss.cpp.library.path = /absolute/path/liboss_cpp_bridge.so
fs.oss.endpoint = https://oss-cn-shanghai-internal.aliyuncs.com
fs.oss.region = cn-shanghai
```

Use the existing `fs.oss.accessKeyId`, `fs.oss.accessKeySecret` and optional
`fs.oss.securityToken` credentials. REST catalog credentials still refresh;
static FileIO credentials do not.

Python bindings can omit `fs.oss.cpp.library.path` when an optional
`pypaimon_oss_cpp` package contains `liboss_cpp_bridge.so` (Linux) or
`liboss_cpp_bridge.dylib` (macOS) beside its `__init__.py`. Explicit paths always
take precedence. This package layout is an integration contract, not a published
wheel; packaging the bridge and its dependencies remains a separate step.
Discovery does not load or validate the library; normal backend initialization
still reports missing dependencies or an incompatible ABI.

| Option | Default |
|---|---:|
| `fs.oss.cpp.max.concurrent.requests` | 8 |
| `fs.oss.cpp.connect.timeout-ms` | 10000 |
| `fs.oss.cpp.request.timeout-ms` | 30000 |
| `fs.oss.cpp.retry.max-attempts` | 3 |
| `fs.oss.cpp.path-style` | false |

The SDK handles retries. Timeouts are per connection/read-write, not
end-to-end. Concurrency is bounded per FileIO, not per process; cancelled
calls retain a slot until the SDK returns. Use spawned workers: SDK access
after fork is rejected. Mutations and presigning are unsupported.
Python-side image downloads are unaffected.

## Test

```bash
cargo test -p paimon --features storage-oss-cpp --lib io::storage_oss_cpp

OSS_CPP_BRIDGE_LIBRARY="$PWD/build/oss-cpp/liboss_cpp_bridge.so" \
  cargo test -p paimon --features storage-oss-cpp --lib io::storage_oss_cpp \
  -- --ignored
```

The ignored tests use the real SDK, fake credentials and a local HTTP server;
no cloud account is needed.
