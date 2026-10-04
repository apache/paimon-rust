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

An opt-in FileIO backend using [Alibaba Cloud OSS C++ SDK v2](https://github.com/aliyun/alibabacloud-oss-cpp-sdk-v2).
OpenDAL remains the default. This integration still uses OpenDAL's operator
abstraction, but HEAD, Range GET and paginated LIST are executed by the C++ SDK.

## Build

Build/install the C++ SDK using its instructions, then build this bridge:

```bash
cmake -S integrations/oss-cpp -B build/oss-cpp \
  -DCMAKE_PREFIX_PATH=/path/to/oss-sdk-install
cmake --build build/oss-cpp
cargo build -p paimon --features storage-oss-cpp
```

The Python binding enables `storage-all`, which includes this backend.
Neither the bridge nor the C++ SDK is bundled
in the wheel. Make their shared-library dependencies available on every worker.
Only Linux has been tested. Tested SDK commit:
`c13a82038b0696f99c32f4e86b623ffcc9e62140` (0.2.0).

## Configure

Pass these options to the native table/catalog FileIO:

```text
fs.oss.impl = cpp
fs.oss.cpp.library.path = /absolute/path/liboss_cpp_bridge.so
fs.oss.endpoint = https://oss-cn-shanghai-internal.aliyuncs.com
fs.oss.region = cn-shanghai
```

Credentials use existing `fs.oss.accessKeyId`, `fs.oss.accessKeySecret` and
optional `fs.oss.securityToken` options. REST catalogs continue to supply and
refresh credentials through RESTTokenFileIO. A directly constructed static
FileIO does not refresh credentials itself.

| Option | Default |
|---|---:|
| `fs.oss.cpp.max.concurrent.requests` | 8 |
| `fs.oss.cpp.connect.timeout-ms` | 10000 |
| `fs.oss.cpp.request.timeout-ms` | 30000 |
| `fs.oss.cpp.retry.max-attempts` | 3 |
| `fs.oss.cpp.path-style` | false |

The SDK handles retries; no extra OpenDAL retry layer is added. Timeout values
are passed to the SDK's connect/read-write timeouts, **not** an end-to-end
deadline including retries and permit waits. HTTPS certificate verification
remains enabled; an explicit HTTP endpoint is supported for testing.

One native client and connection pool is reused per bucket per FileIO.
The gate covers HEAD, GET and LIST. A cancelled async caller retains its
permit and client until the blocking SDK call completes. This is not a
process-wide or host-wide rate limiter. Use spawned workers: invoking the
SDK after a process that initialized it has forked is rejected.

Writes, deletes, copies, renames and presigning are unsupported; there is no
silent fallback. Range reads validate HTTP 206, Content-Range and exact body
length. Bounded reads do not issue an extra HEAD. Unbounded/suffix reads use
HEAD to determine size; returned data is buffered for the requested range.

This changes native FileIO only. Python-side image-body downloads are not
switched automatically. No throughput or long-tail improvement is claimed.

## Test

```bash
cargo test -p paimon --features storage-oss-cpp --lib io::storage_oss_cpp

OSS_CPP_BRIDGE_LIBRARY="$PWD/build/oss-cpp/liboss_cpp_bridge.so" \
  cargo test -p paimon --features storage-oss-cpp --lib io::storage_oss_cpp \
  -- --ignored
```

The ignored tests exercise the real SDK against a local HTTP server, with fake
credentials: ranges, short/incorrect responses, error mapping, retries, pagination,
credential replacement and bounded concurrency including cancellation.
No cloud account or customer data is needed.
