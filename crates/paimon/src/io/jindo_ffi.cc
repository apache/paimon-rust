// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstddef>
#include <exception>

using ListDir = void* (*)(void*, const char*, bool, void*);
using ListObjects = void* (*)(
        void*, const char*, const char*, const char*, int, void*);

namespace {

void copy_error(char* output, std::size_t capacity, const char* message) noexcept {
    if (output == nullptr || capacity == 0) {
        return;
    }
    std::size_t index = 0;
    if (message != nullptr) {
        while (index + 1 < capacity && message[index] != '\0') {
            output[index] = message[index];
            ++index;
        }
    }
    output[index] = '\0';
}

}  // namespace

extern "C" int paimon_jindo_list_dir(
        ListDir list_dir,
        void* handle,
        const char* path,
        bool recursive,
        void* options,
        void** result,
        char* error,
        std::size_t error_capacity) noexcept {
    if (list_dir == nullptr || result == nullptr) {
        copy_error(error, error_capacity, "invalid Jindo list call");
        return 1;
    }
    *result = nullptr;
    try {
        *result = list_dir(handle, path, recursive, options);
        return 0;
    } catch (const std::exception& exception) {
        copy_error(error, error_capacity, exception.what());
    } catch (...) {
        copy_error(error, error_capacity, "unknown C++ exception");
    }
    return 1;
}

extern "C" int paimon_jindo_list_objects(
        ListObjects list_objects,
        void* handle,
        const char* path,
        const char* delimiter,
        const char* marker,
        int max_keys,
        void* options,
        void** result,
        char* error,
        std::size_t error_capacity) noexcept {
    if (list_objects == nullptr || result == nullptr) {
        copy_error(error, error_capacity, "invalid Jindo list call");
        return 1;
    }
    *result = nullptr;
    try {
        *result = list_objects(handle, path, delimiter, marker, max_keys, options);
        return 0;
    } catch (const std::exception& exception) {
        copy_error(error, error_capacity, exception.what());
    } catch (...) {
        copy_error(error, error_capacity, "unknown C++ exception");
    }
    return 1;
}
