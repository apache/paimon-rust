// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ABI v2: all strings are UTF-8; inputs are borrowed for the call.
// Clients allow concurrent calls; each call owns its error output.
// No C++ exception may cross this boundary.
typedef struct {
  const char *endpoint, *region, *access_key, *secret_key, *token, *user_agent;
  int64_t connect_timeout_ms, request_timeout_ms, retry_attempts;
  uint8_t path_style;
} OssCppBridgeConfig;

typedef struct {
  int32_t status;
  char code[128];
  char request_id[256];
} OssCppBridgeError;

typedef struct {
  int64_t size;
  char modified[64];
} OssCppBridgeMetadata;

typedef void (*OssCppBridgeEntry)(void *, const char *, size_t, int64_t,
                                  const char *, uint8_t);

uint32_t oss_cpp_bridge_abi_version(void);
void *oss_cpp_bridge_create(const OssCppBridgeConfig *, OssCppBridgeError *);
void oss_cpp_bridge_destroy(void *);
int32_t oss_cpp_bridge_stat(void *, const char *, const char *,
                            OssCppBridgeMetadata *, OssCppBridgeError *);
int32_t oss_cpp_bridge_read(void *, const char *, const char *, uint64_t,
                            size_t, uint8_t *, OssCppBridgeError *);
// The callback borrows strings until it returns. next_token is owned by
// the bridge and must be released with oss_cpp_bridge_free_string.
int32_t oss_cpp_bridge_list(void *, const char *, const char *, const char *,
                            uint8_t, uint32_t, OssCppBridgeEntry, void *,
                            char **, OssCppBridgeError *);
void oss_cpp_bridge_free_string(char *);

#ifdef __cplusplus
}
#endif
