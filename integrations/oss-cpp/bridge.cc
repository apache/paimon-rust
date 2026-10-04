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

#include "bridge.h"
#include <alibabacloud/oss2/ClientConfiguration.h>
#include <alibabacloud/oss2/OSSClient.h>
#include <alibabacloud/oss2/credentials/CredentialsProvider.h>
#include <alibabacloud/oss2/io/ByteWriter.h>
#include <alibabacloud/oss2/models/BucketBasic.h>
#include <alibabacloud/oss2/models/ObjectBasic.h>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <string>

namespace oss = alibabacloud::oss2;

static int32_t failure(OssCppBridgeError *error, const char *code,
                       int status = 0, const char *request_id = "") noexcept {
  error->status = status;
  std::snprintf(error->code, sizeof(error->code), "%s", code);
  std::snprintf(error->request_id, sizeof(error->request_id), "%s", request_id);
  return -1;
}

template <class F>
static int32_t guarded(OssCppBridgeError *error, F fn) noexcept {
  *error = {};
  try {
    return fn();
  }
  // Do not include SDK exception messages: they can contain signed requests.
  catch (...) {
    return failure(error, "CppException");
  }
}

template <class E>
static int32_t sdk_failure(OssCppBridgeError *error, const E &e) noexcept {
  return failure(error, e.getCode().c_str(), e.getStatusCode(),
                 e.getRequestId().c_str());
}

extern "C" {
uint32_t oss_cpp_bridge_abi_version() { return 1; }

void *oss_cpp_bridge_create(const OssCppBridgeConfig *in,
                            OssCppBridgeError *error) {
  void *client = nullptr;
  guarded(error, [&]() {
    oss::ClientConfiguration config;
    config.endpoint = in->endpoint;
    config.region = in->region;
    config.credentialsProvider =
        std::make_shared<oss::StaticCredentialsProvider>(
            in->access_key, in->secret_key, in->token);
    config.userAgent = in->user_agent;
    config.connectTimeout = in->connect_timeout_ms;
    config.readWriteTimeout = in->request_timeout_ms;
    config.retryMaxAttempts = in->retry_attempts;
    config.usePathStyle = in->path_style != 0;
    client = new oss::OSSClient(config);
    return 0;
  });
  return client;
}

void oss_cpp_bridge_destroy(void *client) {
  try {
    delete static_cast<oss::OSSClient *>(client);
  } catch (...) {
  }
}

int32_t oss_cpp_bridge_stat(void *client, const char *bucket, const char *key,
                            OssCppBridgeMetadata *out,
                            OssCppBridgeError *error) {
  return guarded(error, [&]() {
    auto result = static_cast<oss::OSSClient *>(client)->headObject(
        oss::models::HeadObjectRequest().setBucket(bucket).setKey(key));
    if (!result.has_value())
      return sdk_failure(error, result.error());
    const auto &value = result.value();
    if (value.getContentLength() < 0)
      return failure(error, "InvalidContentLength");
    out->size = value.getContentLength();
    std::snprintf(out->modified, sizeof(out->modified), "%s",
                  value.getLastModified().c_str());
    return 0;
  });
}

int32_t oss_cpp_bridge_read(void *client, const char *bucket, const char *key,
                            uint64_t offset, size_t length, uint8_t *buffer,
                            OssCppBridgeError *error) {
  return guarded(error, [&]() {
    if (!length)
      return 0;
    if (offset > UINT64_MAX - (length - 1))
      return failure(error, "InvalidRange");
    std::shared_ptr<oss::MemoryWriter> sink;
    oss::SinkFactory factory;
    factory.isOneShot = false;
    // A retry must start at the beginning of the caller's buffer.
    factory.supplier =
        [&](int64_t,
            const oss::HeaderCollection &) -> std::shared_ptr<oss::ByteWriter> {
      sink = std::make_shared<oss::MemoryWriter>(buffer, length);
      return sink;
    };
    const auto end = offset + length - 1;
    auto result = static_cast<oss::OSSClient *>(client)->getObject(
        oss::models::GetObjectRequest()
            .setBucket(bucket)
            .setKey(key)
            .setRange("bytes=" + std::to_string(offset) + "-" +
                      std::to_string(end))
            .setSinkFactory(factory));
    if (!result.has_value())
      return sdk_failure(error, result.error());
    const auto &value = result.value();
    const auto prefix =
        "bytes " + std::to_string(offset) + "-" + std::to_string(end) + "/";
    if (value.getStatusCode() != 206 ||
        value.getContentRange().rfind(prefix, 0) != 0 ||
        value.getContentLength() != static_cast<int64_t>(length) || !sink ||
        sink->written() != length) {
      return failure(error, "InvalidRangeResponse", value.getStatusCode());
    }
    return 0;
  });
}

int32_t oss_cpp_bridge_list(void *client, const char *bucket,
                            const char *prefix, const char *token,
                            uint8_t recursive, OssCppBridgeEntry entry,
                            void *ctx, char **next_token,
                            OssCppBridgeError *error) {
  *next_token = nullptr;
  return guarded(error, [&]() {
    auto request = oss::models::ListObjectsV2Request()
                       .setBucket(bucket)
                       .setPrefix(prefix)
                       .setMaxKeys(1000);
    if (!recursive)
      request.setDelimiter("/");
    if (*token)
      request.setContinuationToken(token);
    auto result = static_cast<oss::OSSClient *>(client)->listObjectsV2(request);
    if (!result.has_value())
      return sdk_failure(error, result.error());
    const auto &value = result.value();
    std::unique_ptr<char[]> next;
    if (value.getIsTruncated()) {
      const auto &s = value.getNextContinuationToken();
      if (s.empty() || s == token)
        return failure(error, "InvalidContinuationToken");
      next = std::make_unique<char[]>(s.size() + 1);
      std::memcpy(next.get(), s.c_str(), s.size() + 1);
    }
    for (const auto &item : value.getContents()) {
      entry(ctx, item.key.data(), item.key.size(), item.size,
            item.lastModified.c_str(),
            !item.key.empty() && item.key.back() == '/');
    }
    for (const auto &item : value.getCommonPrefixes()) {
      entry(ctx, item.prefix.data(), item.prefix.size(), 0, "", 1);
    }
    *next_token = next.release();
    return 0;
  });
}

void oss_cpp_bridge_free_string(char *s) { delete[] s; }
}
