#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

target="${1:?pass the Rust target}"
root="$(cd "$(dirname "$0")/.." && pwd)"
sdk="$root/.oss-cpp-sdk"
build="$root/build/python-oss-cpp-$target"
package="$root/bindings/python/python/pypaimon_rust"

test -f "$sdk/CMakeLists.txt" || { echo "Missing pinned OSS C++ SDK checkout" >&2; exit 1; }

compiler=()
if [[ "$target" == aarch64-unknown-linux-gnu ]]; then
  compiler=(-DCMAKE_C_COMPILER=aarch64-linux-gnu-gcc
            -DCMAKE_CXX_COMPILER=aarch64-linux-gnu-g++
            -DOPENSSL_ROOT_DIR=/usr/aarch64-unknown-linux-gnu)
  strip=aarch64-linux-gnu-strip
elif [[ "$target" != x86_64-unknown-linux-gnu ]]; then
  echo "Unsupported OSS C++ wheel target: $target" >&2
  exit 1
else
  strip=strip
fi

cmake -S "$sdk" -B "$build/sdk" \
  "${compiler[@]}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$build/install" \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DOPENSSL_USE_STATIC_LIBS=ON \
  -DUSE_SYSTEM_OPENSSL=ON \
  -DUSE_SYSTEM_CURL=OFF \
  -DBUILD_SHARED_LIBS=OFF \
  -DCMAKE_CXX_FLAGS=-Wno-error=maybe-uninitialized
cmake --build "$build/sdk" --parallel 2
cmake --install "$build/sdk"

cmake -S "$root/integrations/oss-cpp" -B "$build/bridge" \
  "${compiler[@]}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="$build/install" \
  -DOPENSSL_USE_STATIC_LIBS=ON \
  -DCMAKE_SHARED_LINKER_FLAGS=-Wl,--no-undefined \
  -Wno-dev
cmake --build "$build/bridge" --parallel 2

bridge="$build/bridge/liboss_cpp_bridge.so"
if readelf -d "$bridge" | grep -Eq 'Shared library: \[lib(ssl|crypto|curl)'; then
  echo "OSS C++ bridge still depends on system OpenSSL or curl" >&2
  exit 1
fi
cp "$bridge" "$package/liboss_cpp_bridge.so"
"$strip" --strip-unneeded "$package/liboss_cpp_bridge.so"
