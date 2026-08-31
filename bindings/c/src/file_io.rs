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

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use paimon::io::{FileBlockCache, FileIO};

use crate::error::{paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::paimon_result_file_io_new;
use crate::types::{paimon_file_cache_callbacks_v1, paimon_file_io, paimon_option};

#[derive(Debug)]
struct CFileBlockCache {
    inner: Arc<CFileBlockCacheInner>,
}

struct CFileBlockCacheInner {
    context: usize,
    get: unsafe extern "C" fn(*mut c_void, *const u8, usize, u64, usize, *mut u8) -> i64,
    put: Option<unsafe extern "C" fn(*mut c_void, *const u8, usize, u64, *const u8, usize) -> i32>,
    invalidate_path: Option<unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32>,
    invalidate_prefix: Option<unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32>,
    destroy: Option<unsafe extern "C" fn(*mut c_void)>,
}

impl std::fmt::Debug for CFileBlockCacheInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CFileBlockCacheInner")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

impl Drop for CFileBlockCacheInner {
    fn drop(&mut self) {
        if let Some(destroy) = self.destroy {
            unsafe { destroy(self.context as *mut c_void) };
        }
    }
}

impl CFileBlockCache {
    unsafe fn from_callbacks(callbacks: &paimon_file_cache_callbacks_v1) -> Result<Self, String> {
        let get = callbacks
            .get
            .ok_or_else(|| "file cache callback `get` must not be null".to_string())?;
        Ok(Self {
            inner: Arc::new(CFileBlockCacheInner {
                context: callbacks.context as usize,
                get,
                put: callbacks.put,
                invalidate_path: callbacks.invalidate_path,
                invalidate_prefix: callbacks.invalidate_prefix,
                destroy: callbacks.destroy,
            }),
        })
    }

    async fn invalidate(
        &self,
        path: &str,
        callback: Option<unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32>,
    ) {
        let Some(callback) = callback else {
            return;
        };
        let path = path.as_bytes().to_vec();
        let inner = self.inner.clone();
        let _ = tokio::task::spawn_blocking(move || unsafe {
            callback(inner.context as *mut c_void, path.as_ptr(), path.len())
        })
        .await;
    }
}

#[async_trait::async_trait]
impl FileBlockCache for CFileBlockCache {
    async fn get(&self, path: &str, range: Range<u64>) -> Option<Bytes> {
        let length = usize::try_from(range.end.checked_sub(range.start)?).ok()?;
        let expected = i64::try_from(length).ok()?;
        let path = path.as_bytes().to_vec();
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut output = vec![0_u8; length];
            let copied = unsafe {
                (inner.get)(
                    inner.context as *mut c_void,
                    path.as_ptr(),
                    path.len(),
                    range.start,
                    length,
                    output.as_mut_ptr(),
                )
            };
            (copied == expected).then(|| Bytes::from(output))
        })
        .await
        .ok()
        .flatten()
    }

    async fn put(&self, path: &str, offset: u64, data: Bytes) {
        let Some(put) = self.inner.put else {
            return;
        };
        let path = path.as_bytes().to_vec();
        let inner = self.inner.clone();
        let _ = tokio::task::spawn_blocking(move || unsafe {
            put(
                inner.context as *mut c_void,
                path.as_ptr(),
                path.len(),
                offset,
                data.as_ptr(),
                data.len(),
            )
        })
        .await;
    }

    async fn invalidate_path(&self, path: &str) {
        self.invalidate(path, self.inner.invalidate_path).await;
    }

    async fn invalidate_prefix(&self, prefix: &str) {
        self.invalidate(prefix, self.inner.invalidate_prefix).await;
    }
}

unsafe fn parse_options(
    options: *const paimon_option,
    options_len: usize,
) -> Result<HashMap<String, String>, *mut paimon_error> {
    if options.is_null() && options_len > 0 {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null options pointer with non-zero length".to_string(),
        ));
    }
    let mut parsed = HashMap::with_capacity(options_len);
    if options_len > 0 {
        for option in std::slice::from_raw_parts(options, options_len) {
            let key = validate_cstr(option.key, "storage option key")?;
            let value = validate_cstr(option.value, "storage option value")?;
            parsed.insert(key, value);
        }
    }
    Ok(parsed)
}

unsafe fn create_file_io(
    path: *const c_char,
    options: *const paimon_option,
    options_len: usize,
) -> Result<FileIO, *mut paimon_error> {
    let path = validate_cstr(path, "path")?;
    let options = parse_options(options, options_len)?;
    FileIO::from_path(path)
        .and_then(|builder| builder.with_props(options).build())
        .map_err(paimon_error::from_paimon)
}

fn file_io_result(result: Result<FileIO, *mut paimon_error>) -> paimon_result_file_io_new {
    match result {
        Ok(file_io) => paimon_result_file_io_new {
            file_io: Box::into_raw(Box::new(paimon_file_io {
                inner: Box::into_raw(Box::new(file_io)) as *mut c_void,
            })),
            error: std::ptr::null_mut(),
        },
        Err(error) => paimon_result_file_io_new {
            file_io: std::ptr::null_mut(),
            error,
        },
    }
}

/// Create a reusable FileIO from a representative storage path and options.
#[no_mangle]
pub unsafe extern "C" fn paimon_file_io_create(
    path: *const c_char,
    options: *const paimon_option,
    options_len: usize,
) -> paimon_result_file_io_new {
    file_io_result(create_file_io(path, options, options_len))
}

/// Create a reusable FileIO backed by a caller-managed block cache.
///
/// A non-null `callbacks->get` and a non-zero `block_size` are required.
/// `whitelist` may be null to use `meta,global-index`. Once validation and
/// storage construction succeed, Rust owns `callbacks->context` and invokes
/// `destroy` exactly once after the last derived FileIO/table is dropped.
#[no_mangle]
pub unsafe extern "C" fn paimon_file_io_create_with_cache_v1(
    path: *const c_char,
    options: *const paimon_option,
    options_len: usize,
    callbacks: *const paimon_file_cache_callbacks_v1,
    block_size: u64,
    whitelist: *const c_char,
) -> paimon_result_file_io_new {
    if callbacks.is_null() {
        return file_io_result(Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null pointer passed for `callbacks`".to_string(),
        )));
    }
    if block_size == 0 || usize::try_from(block_size).is_err() {
        return file_io_result(Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "file cache block_size must be representable by size_t and greater than zero"
                .to_string(),
        )));
    }
    let whitelist = if whitelist.is_null() {
        "meta,global-index".to_string()
    } else {
        match validate_cstr(whitelist, "whitelist") {
            Ok(value) => value,
            Err(error) => return file_io_result(Err(error)),
        }
    };
    let file_io = match create_file_io(path, options, options_len) {
        Ok(file_io) => file_io,
        Err(error) => return file_io_result(Err(error)),
    };
    let cache = match CFileBlockCache::from_callbacks(&*callbacks) {
        Ok(cache) => Arc::new(cache),
        Err(message) => {
            return file_io_result(Err(paimon_error::new(
                PaimonErrorCode::InvalidInput,
                message,
            )))
        }
    };
    file_io_result(
        file_io
            .with_file_block_cache(cache, block_size, &whitelist)
            .map_err(paimon_error::from_paimon),
    )
}

/// Free a FileIO handle. Tables created from it retain their own clone.
#[no_mangle]
pub unsafe extern "C" fn paimon_file_io_free(file_io: *mut paimon_file_io) {
    if !file_io.is_null() {
        let wrapper = Box::from_raw(file_io);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut FileIO));
        }
    }
}

pub(crate) unsafe fn file_io_ref<'a>(file_io: *const paimon_file_io) -> &'a FileIO {
    &*((*file_io).inner as *const FileIO)
}

// Additive C ABI signature guards. Existing symbols must never gain parameters;
// introduce a new versioned symbol when the callback table changes.
const _: unsafe extern "C" fn(
    *const c_char,
    *const paimon_option,
    usize,
) -> paimon_result_file_io_new = paimon_file_io_create;
const _: unsafe extern "C" fn(
    *const c_char,
    *const paimon_option,
    usize,
    *const paimon_file_cache_callbacks_v1,
    u64,
    *const c_char,
) -> paimon_result_file_io_new = paimon_file_io_create_with_cache_v1;
const _: unsafe extern "C" fn(*mut paimon_file_io) = paimon_file_io_free;
