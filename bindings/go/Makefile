# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

RUST_ROOT := $(shell cd ../.. && pwd)
TARGET_DIR := $(RUST_ROOT)/target
GO_DIR := $(shell pwd)

# Detect platform
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
    LIB_EXT := dylib
    OS := darwin
else
    LIB_EXT := so
    OS := linux
endif

ifeq ($(UNAME_M),arm64)
    ARCH := arm64
else ifeq ($(UNAME_M),aarch64)
    ARCH := arm64
else
    ARCH := amd64
endif

LIB_NAME := libpaimon_c.$(OS).$(ARCH).$(LIB_EXT)

.PHONY: build test clean

# Build the Rust shared library, compress with zstd, and place in Go package dir.
build:
	cd $(RUST_ROOT) && cargo build --locked -p paimon-c --release
	zstd -19 -f $(TARGET_DIR)/release/libpaimon_c.$(LIB_EXT) -o $(GO_DIR)/$(LIB_NAME).zst

# Run Go integration tests.
# The tests extract the committed golden warehouse into temporary directories.
# CGO is needed by arrow-go's cdata package (imported by the main paimon package).
test: build
	cd $(GO_DIR)/tests && go test -v ./...

clean:
	cd $(RUST_ROOT) && cargo clean
	rm -f $(GO_DIR)/libpaimon_c.*.zst
