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

# Bundled third-party licenses

`openssl-1.1.1.LICENSE` covers OpenSSL 1.1.1k FIPS on x86_64 Linux and
OpenSSL 1.1.1w on aarch64 Linux. Both versions use the same license text.

The XZ Utils 5.8.3 license is read from the locked `liblzma-sys` crate at
`xz/COPYING.0BSD`.

`jieba-rs-0.10.3.LICENSE` is the license file from the root of the upstream
`jieba-rs` workspace at commit
`c62e0df1f9dcc2cc1e014711c5aa4561ae260538`. It covers `jieba-rs` 0.10.3 and
`jieba-macros` 0.10.3.

Linux Python wheels also bundle OSS C++ SDK v2 at
`c13a82038b0696f99c32f4e86b623ffcc9e62140` and curl 8.13.0.
Their notices and curl license are copied here from those pinned sources.

Target-specific Python and Go reports are generated in their binary build jobs.
They are intentionally not committed or included in the ASF source archive.

Sources:

- <https://github.com/openssl/openssl/blob/OpenSSL_1_1_1k/LICENSE>
- <https://github.com/openssl/openssl/blob/OpenSSL_1_1_1w/LICENSE>
- <https://github.com/messense/jieba-rs/blob/c62e0df1f9dcc2cc1e014711c5aa4561ae260538/LICENSE>
- <https://github.com/aliyun/alibabacloud-oss-cpp-sdk-v2/blob/c13a82038b0696f99c32f4e86b623ffcc9e62140/THIRD_PARTY_NOTICES>
- <https://github.com/curl/curl/blob/curl-8_13_0/COPYING>
