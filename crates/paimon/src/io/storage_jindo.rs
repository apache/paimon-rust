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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::ffi::{CStr, CString, OsStr};
use std::fmt::{Debug, Formatter};
use std::os::raw::{c_char, c_void};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use libloading::Library;
use opendal::raw::*;
use opendal::{Buffer, Builder, BytesRange, Capability, EntryMode, ErrorKind, Metadata};
use opendal::{Error as OpendalError, OperationContext, Operator, Result as OpendalResult};
use tokio::sync::OnceCell;

use crate::error::Error;
use crate::Result;

const OSS_IMPL: &str = "fs.oss.impl";
const JINDO_IMPL: &str = "jindo";
const JINDO_LIBRARY_PATH: &str = "fs.jindo.library.path";
const JINDO_USER: &str = "fs.jindo.user";
const DEFAULT_JINDO_USER: &str = "root";

const JDO_FILE_NOT_FOUND_ERROR: i32 = 3001;
const JDO_REST_HTTP_403_ERROR: i32 = 6403;
const JDO_REST_HTTP_404_ERROR: i32 = 6404;
const JDO_REST_HTTP_503_ERROR: i32 = 6503;

type JdoPtr = *mut c_void;

type CreateOptions = unsafe extern "C" fn() -> JdoPtr;
type FreeOptions = unsafe extern "C" fn(JdoPtr);
type SetOption = unsafe extern "C" fn(JdoPtr, *const c_char, *const c_char);
type CreateStore = unsafe extern "C" fn(JdoPtr, *const c_char) -> JdoPtr;
type DestroyStore = unsafe extern "C" fn(JdoPtr);
type FreeStore = unsafe extern "C" fn(JdoPtr);
type CreateHandle = unsafe extern "C" fn(JdoPtr) -> JdoPtr;
type FreeHandle = unsafe extern "C" fn(JdoPtr);
type HandleErrorCode = unsafe extern "C" fn(JdoPtr) -> i32;
type HandleErrorMessage = unsafe extern "C" fn(JdoPtr) -> *const c_char;
type Init = unsafe extern "C" fn(JdoPtr, *const c_char);
type GetFileStatus = unsafe extern "C" fn(JdoPtr, *const c_char, JdoPtr) -> JdoPtr;
type FreeFileStatus = unsafe extern "C" fn(JdoPtr);
type FileStatusPath = unsafe extern "C" fn(JdoPtr) -> *const c_char;
type FileStatusType = unsafe extern "C" fn(JdoPtr) -> i8;
type FileStatusSize = unsafe extern "C" fn(JdoPtr) -> i64;
type FileStatusMtime = unsafe extern "C" fn(JdoPtr) -> i64;
type ListDir = unsafe extern "C" fn(JdoPtr, *const c_char, bool, JdoPtr) -> JdoPtr;
type FreeListDirResult = unsafe extern "C" fn(JdoPtr);
type ListDirResultSize = unsafe extern "C" fn(JdoPtr) -> i64;
type IsListDirResultTruncated = unsafe extern "C" fn(JdoPtr) -> bool;
type ListDirResultNextMarker = unsafe extern "C" fn(JdoPtr) -> *const c_char;
type ListDirFileStatus = unsafe extern "C" fn(JdoPtr, usize) -> JdoPtr;
type GetObject = unsafe extern "C" fn(JdoPtr, *const c_char, *mut c_char, i64, i64, JdoPtr) -> i64;

#[derive(Clone)]
pub struct JindoStorageConfig {
    library_path: Option<PathBuf>,
    user: String,
    properties: HashMap<String, String>,
}

impl Debug for JindoStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JindoStorageConfig")
            .field("library_path", &self.library_path)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

pub(crate) fn use_jindo(props: &HashMap<String, String>) -> Result<bool> {
    match props.get(OSS_IMPL).map(|value| value.to_ascii_lowercase()) {
        Some(value) if value == JINDO_IMPL => Ok(true),
        Some(value) if value == "legacy" => Ok(false),
        Some(value) => Err(Error::ConfigInvalid {
            message: format!("Unsupported {OSS_IMPL}: {value}"),
        }),
        None => Ok(false),
    }
}

pub(crate) fn jindo_config_parse(mut props: HashMap<String, String>) -> Result<JindoStorageConfig> {
    let library_path = props.remove(JINDO_LIBRARY_PATH).map(PathBuf::from);
    let user = props
        .remove(JINDO_USER)
        .unwrap_or_else(|| DEFAULT_JINDO_USER.to_string());
    props.remove(OSS_IMPL);
    props.insert(
        "fs.oss.user.agent.features".to_string(),
        "paimon-rust".to_string(),
    );
    props
        .entry("logger.appender".to_string())
        .or_insert_with(|| "file".to_string());
    if let Some(endpoint) = props.get_mut("fs.oss.endpoint") {
        *endpoint = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string();
    }
    Ok(JindoStorageConfig {
        library_path,
        user,
        properties: props,
    })
}

pub(crate) fn jindo_config_build(config: &JindoStorageConfig, bucket: &str) -> Result<Operator> {
    let builder = JindoBuilder {
        config: Some(config.clone()),
        bucket: bucket.to_string(),
    };
    Operator::new(builder).map_err(|source| Error::IoUnexpected {
        message: format!("Failed to initialize Jindo for OSS bucket '{bucket}'"),
        source: Box::new(source),
    })
}

#[derive(Default)]
struct JindoBuilder {
    config: Option<JindoStorageConfig>,
    bucket: String,
}

impl Builder for JindoBuilder {
    type Config = ();

    fn build(self) -> OpendalResult<impl Service> {
        let config = self.config.ok_or_else(|| {
            OpendalError::new(ErrorKind::ConfigInvalid, "missing Jindo configuration")
        })?;
        Ok(JindoService {
            info: ServiceInfo::new("jindo", "/", &self.bucket),
            client: Arc::new(LazyJindoClient::new(config, self.bucket)),
        })
    }
}

#[derive(Clone)]
struct JindoService {
    info: ServiceInfo,
    client: Arc<LazyJindoClient>,
}

impl Debug for JindoService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JindoService")
            .field("info", &self.info)
            .finish()
    }
}

impl Service for JindoService {
    type Reader = JindoReader;
    type Writer = ();
    type Lister = JindoLister;
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            read_with_suffix: true,
            list: true,
            list_with_recursive: true,
            shared: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> OpendalResult<RpCreateDir> {
        unsupported("create_dir")
    }

    async fn stat(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpStat,
    ) -> OpendalResult<RpStat> {
        let client = self.client.get().await?;
        let path = path.to_string();
        let metadata = run_blocking(move || client.stat(&path)).await?;
        Ok(RpStat::new(metadata))
    }

    fn read(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpRead,
    ) -> OpendalResult<Self::Reader> {
        Ok(JindoReader {
            client: Arc::clone(&self.client),
            path: path.to_string(),
        })
    }

    fn write(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpWrite,
    ) -> OpendalResult<Self::Writer> {
        unsupported("write")
    }

    fn delete(&self, _ctx: &OperationContext) -> OpendalResult<Self::Deleter> {
        unsupported("delete")
    }

    fn list(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> OpendalResult<Self::Lister> {
        Ok(JindoLister {
            client: Arc::clone(&self.client),
            path: path.to_string(),
            recursive: args.recursive(),
            entries: None,
        })
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> OpendalResult<Self::Copier> {
        unsupported("copy")
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> OpendalResult<RpRename> {
        unsupported("rename")
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> OpendalResult<RpPresign> {
        unsupported("presign")
    }
}

struct JindoReader {
    client: Arc<LazyJindoClient>,
    path: String,
}

impl oio::Read for JindoReader {
    async fn open(
        &self,
        range: BytesRange,
    ) -> OpendalResult<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (_, buffer) = self.read(range).await?;
        Ok((RpRead::default(), Box::new(buffer)))
    }

    async fn read(&self, range: BytesRange) -> OpendalResult<(RpRead, Buffer)> {
        let client = self.client.get().await?;
        let path = self.path.clone();
        let buffer = run_blocking(move || client.read(&path, range)).await?;
        Ok((RpRead::default(), Buffer::from(buffer)))
    }
}

struct JindoLister {
    client: Arc<LazyJindoClient>,
    path: String,
    recursive: bool,
    entries: Option<std::vec::IntoIter<oio::Entry>>,
}

impl oio::List for JindoLister {
    async fn next(&mut self) -> OpendalResult<Option<oio::Entry>> {
        if self.entries.is_none() {
            let client = self.client.get().await?;
            let path = self.path.clone();
            let recursive = self.recursive;
            let entries = run_blocking(move || client.list(&path, recursive)).await?;
            self.entries = Some(entries.into_iter());
        }
        Ok(self.entries.as_mut().and_then(Iterator::next))
    }
}

struct LazyJindoClient {
    config: JindoStorageConfig,
    bucket: String,
    client: OnceCell<Arc<JindoClient>>,
}

impl LazyJindoClient {
    fn new(config: JindoStorageConfig, bucket: String) -> Self {
        Self {
            config,
            bucket,
            client: OnceCell::new(),
        }
    }

    async fn get(&self) -> OpendalResult<Arc<JindoClient>> {
        let client = self
            .client
            .get_or_try_init(|| {
                let config = self.config.clone();
                let bucket = self.bucket.clone();
                async move {
                    run_blocking(move || JindoClient::new(&config, &bucket))
                        .await
                        .map(Arc::new)
                }
            })
            .await?;
        Ok(Arc::clone(client))
    }
}

async fn run_blocking<T: Send + 'static>(
    call: impl FnOnce() -> OpendalResult<T> + Send + 'static,
) -> OpendalResult<T> {
    tokio::task::spawn_blocking(call).await.map_err(|source| {
        OpendalError::new(ErrorKind::Unexpected, "Jindo task failed").set_source(source)
    })?
}

fn unsupported<T>(operation: &str) -> OpendalResult<T> {
    Err(OpendalError::new(
        ErrorKind::Unsupported,
        format!("Jindo {operation} is not supported"),
    ))
}

struct JindoApi {
    create_options: CreateOptions,
    free_options: FreeOptions,
    set_option: SetOption,
    create_store: CreateStore,
    destroy_store: DestroyStore,
    free_store: FreeStore,
    create_handle: CreateHandle,
    free_handle: FreeHandle,
    handle_error_code: HandleErrorCode,
    handle_error_message: HandleErrorMessage,
    init: Init,
    get_file_status: GetFileStatus,
    free_file_status: FreeFileStatus,
    file_status_path: FileStatusPath,
    file_status_type: FileStatusType,
    file_status_size: FileStatusSize,
    file_status_mtime: FileStatusMtime,
    list_dir: ListDir,
    free_list_dir_result: FreeListDirResult,
    list_dir_result_size: ListDirResultSize,
    is_list_dir_result_truncated: IsListDirResultTruncated,
    list_dir_result_next_marker: ListDirResultNextMarker,
    list_dir_file_status: ListDirFileStatus,
    get_object: GetObject,
    _library: Library,
}

impl Debug for JindoApi {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("JindoApi")
    }
}

impl JindoApi {
    fn load(explicit_path: Option<&Path>) -> OpendalResult<Arc<Self>> {
        // Jindo owns process-wide services, so keep loaded libraries for the process lifetime.
        static APIS: OnceLock<Mutex<HashMap<PathBuf, Arc<JindoApi>>>> = OnceLock::new();

        let mut apis = APIS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| OpendalError::new(ErrorKind::Unexpected, "Jindo API lock poisoned"))?;
        let candidates = library_candidates(explicit_path);
        let mut errors = Vec::new();
        for path in candidates {
            if let Some(api) = apis.get(&path) {
                return Ok(Arc::clone(api));
            }
            match unsafe { Self::load_path(&path) } {
                Ok(api) => {
                    let api = Arc::new(api);
                    apis.insert(path, Arc::clone(&api));
                    return Ok(api);
                }
                Err(error) => errors.push(format!("{}: {error}", path.display())),
            }
        }
        Err(OpendalError::new(
            ErrorKind::ConfigInvalid,
            "unable to load Jindo C SDK; set fs.jindo.library.path",
        )
        .with_context("attempts", errors.join("; ")))
    }

    unsafe fn load_path(path: &Path) -> OpendalResult<Self> {
        let library = unsafe { Library::new(path) }.map_err(|source| {
            OpendalError::new(ErrorKind::ConfigInvalid, "failed to load Jindo library")
                .set_source(source)
        })?;
        macro_rules! load {
            ($name:literal, $ty:ty) => {{
                let symbol = unsafe { library.get::<$ty>(concat!($name, "\0").as_bytes()) }
                    .map_err(|source| {
                        OpendalError::new(
                            ErrorKind::ConfigInvalid,
                            concat!("missing Jindo symbol ", $name),
                        )
                        .set_source(source)
                    })?;
                *symbol
            }};
        }
        let api = Self {
            create_options: load!("jdo_createOptions", CreateOptions),
            free_options: load!("jdo_freeOptions", FreeOptions),
            set_option: load!("jdo_setOption", SetOption),
            create_store: load!("jdo_createStore", CreateStore),
            destroy_store: load!("jdo_destroyStore", DestroyStore),
            free_store: load!("jdo_freeStore", FreeStore),
            create_handle: load!("jdo_createHandleCtx1", CreateHandle),
            free_handle: load!("jdo_freeHandleCtx", FreeHandle),
            handle_error_code: load!("jdo_getHandleCtxErrorCode", HandleErrorCode),
            handle_error_message: load!("jdo_getHandleCtxErrorMsg", HandleErrorMessage),
            init: load!("jdo_init", Init),
            get_file_status: load!("jdo_getFileStatus", GetFileStatus),
            free_file_status: load!("jdo_freeFileStatus", FreeFileStatus),
            file_status_path: load!("jdo_getFileStatusPath", FileStatusPath),
            file_status_type: load!("jdo_getFileStatusType", FileStatusType),
            file_status_size: load!("jdo_getFileStatusSize", FileStatusSize),
            file_status_mtime: load!("jdo_getFileStatusMtime", FileStatusMtime),
            list_dir: load!("jdo_listDir", ListDir),
            free_list_dir_result: load!("jdo_freeListDirResult", FreeListDirResult),
            list_dir_result_size: load!("jdo_getListDirResultSize", ListDirResultSize),
            is_list_dir_result_truncated: load!(
                "jdo_isListDirResultTruncated",
                IsListDirResultTruncated
            ),
            list_dir_result_next_marker: load!(
                "jdo_getListDirResultNextMarker",
                ListDirResultNextMarker
            ),
            list_dir_file_status: load!("jdo_getListDirFileStatus", ListDirFileStatus),
            get_object: load!("jdo_getObject", GetObject),
            _library: library,
        };
        Ok(api)
    }
}

fn library_candidates(explicit_path: Option<&Path>) -> Vec<PathBuf> {
    if let Some(path) = explicit_path {
        return vec![path.to_path_buf()];
    }
    if let Some(path) = std::env::var_os("JINDOSDK_LIBRARY_PATH") {
        return vec![PathBuf::from(path)];
    }
    if let Some(home) = std::env::var_os("JINDOSDK_HOME") {
        let mut path = PathBuf::from(home);
        path.push("lib");
        path.push("native");
        path.push(jindo_c_library_name());
        return vec![path];
    }
    vec![PathBuf::from(jindo_c_library_name())]
}

fn jindo_c_library_name() -> &'static OsStr {
    #[cfg(target_os = "macos")]
    {
        OsStr::new("libjindosdk_c.dylib")
    }
    #[cfg(not(target_os = "macos"))]
    {
        OsStr::new("libjindosdk_c.so")
    }
}

struct JindoClient {
    api: Arc<JindoApi>,
    options: JdoPtr,
    store: JdoPtr,
    root: String,
    initialized: bool,
}

// SAFETY: Jindo stores support concurrent operations through independent handles.
unsafe impl Send for JindoClient {}
unsafe impl Sync for JindoClient {}

impl Debug for JindoClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JindoClient")
            .field("root", &self.root)
            .finish()
    }
}

impl JindoClient {
    fn new(config: &JindoStorageConfig, bucket: &str) -> OpendalResult<Self> {
        let api = JindoApi::load(config.library_path.as_deref())?;
        let root = format!("oss://{bucket}/");
        let root_c = to_cstring(&root, "OSS root")?;
        let user_c = to_cstring(&config.user, "Jindo user")?;
        let properties = config
            .properties
            .iter()
            .map(|(key, value)| {
                Ok((
                    to_cstring(key, "Jindo option key")?,
                    to_cstring(value, "Jindo option value")?,
                ))
            })
            .collect::<OpendalResult<Vec<_>>>()?;

        let options = unsafe { (api.create_options)() };
        if options.is_null() {
            return Err(OpendalError::new(
                ErrorKind::Unexpected,
                "Jindo failed to create options",
            ));
        }
        for (key, value) in properties {
            unsafe { (api.set_option)(options, key.as_ptr(), value.as_ptr()) };
        }

        let store = unsafe { (api.create_store)(options, root_c.as_ptr()) };
        if store.is_null() {
            unsafe { (api.free_options)(options) };
            return Err(OpendalError::new(
                ErrorKind::Unexpected,
                "Jindo failed to create an OSS store",
            ));
        }
        let mut client = Self {
            api,
            options,
            store,
            root,
            initialized: false,
        };
        client.with_handle(|handle| {
            unsafe { (client.api.init)(handle, user_c.as_ptr()) };
            Ok(())
        })?;
        client.initialized = true;
        Ok(client)
    }

    fn stat(&self, path: &str) -> OpendalResult<Metadata> {
        let path = to_cstring(&self.full_path(path), "Jindo path")?;
        self.with_handle(|handle| {
            let status =
                unsafe { (self.api.get_file_status)(handle, path.as_ptr(), std::ptr::null_mut()) };
            if status.is_null() {
                return Err(OpendalError::new(
                    ErrorKind::Unexpected,
                    "Jindo returned an empty file status",
                ));
            }
            let _guard = FileStatusGuard {
                api: Arc::clone(&self.api),
                status,
            };
            self.file_status_metadata(status)
        })
    }

    fn list(&self, path: &str, recursive: bool) -> OpendalResult<Vec<oio::Entry>> {
        let path = to_cstring(&self.full_path(path), "Jindo path")?;
        self.with_handle(|handle| {
            let mut entries = Vec::new();
            let mut marker: Option<String> = None;
            loop {
                let options = unsafe { (self.api.create_options)() };
                if options.is_null() {
                    return Err(OpendalError::new(
                        ErrorKind::Unexpected,
                        "Jindo failed to create list options",
                    ));
                }
                let _options_guard = OptionsGuard {
                    api: Arc::clone(&self.api),
                    options,
                };
                let marker_c = marker
                    .as_deref()
                    .map(|value| to_cstring(value, "Jindo list marker"))
                    .transpose()?;
                let marker_key = to_cstring("JDO_LIST_OPTS_MARKER", "Jindo list marker option")?;
                if let Some(marker_c) = marker_c.as_ref() {
                    unsafe {
                        (self.api.set_option)(options, marker_key.as_ptr(), marker_c.as_ptr())
                    };
                }

                let result =
                    unsafe { (self.api.list_dir)(handle, path.as_ptr(), recursive, options) };
                if result.is_null() {
                    return Err(OpendalError::new(
                        ErrorKind::Unexpected,
                        "Jindo returned an empty list result",
                    ));
                }
                let _result_guard = ListDirResultGuard {
                    api: Arc::clone(&self.api),
                    result,
                };
                let size = unsafe { (self.api.list_dir_result_size)(result) };
                for index in 0..size.max(0) as usize {
                    let status = unsafe { (self.api.list_dir_file_status)(result, index) };
                    if status.is_null() {
                        continue;
                    }
                    let entry_path = self.file_status_path(status)?;
                    let metadata = self.file_status_metadata(status)?;
                    entries.push(oio::Entry::new(&entry_path, metadata));
                }

                if !unsafe { (self.api.is_list_dir_result_truncated)(result) } {
                    break;
                }
                let next_marker = unsafe {
                    c_string(
                        (self.api.list_dir_result_next_marker)(result),
                        "Jindo list marker",
                    )?
                };
                if next_marker.is_empty() || marker.as_deref() == Some(next_marker.as_str()) {
                    return Err(OpendalError::new(
                        ErrorKind::Unexpected,
                        "Jindo list returned an invalid continuation marker",
                    ));
                }
                marker = Some(next_marker);
            }
            Ok(entries)
        })
    }

    fn file_status_path(&self, status: JdoPtr) -> OpendalResult<String> {
        let full_path = unsafe { c_string((self.api.file_status_path)(status), "Jindo path")? };
        Ok(relative_list_path(
            &self.root,
            &full_path,
            unsafe { (self.api.file_status_type)(status) } == 1,
        ))
    }

    fn file_status_metadata(&self, status: JdoPtr) -> OpendalResult<Metadata> {
        let mode = match unsafe { (self.api.file_status_type)(status) } {
            1 => EntryMode::DIR,
            2 => EntryMode::FILE,
            _ => EntryMode::Unknown,
        };
        let size = unsafe { (self.api.file_status_size)(status) };
        let mtime = unsafe { (self.api.file_status_mtime)(status) };
        let mut metadata = Metadata::new(mode).with_content_length(size.max(0) as u64);
        if mtime > 0 {
            metadata.set_last_modified(Timestamp::from_millisecond(mtime)?);
        }
        Ok(metadata)
    }

    fn read(&self, path: &str, range: BytesRange) -> OpendalResult<Vec<u8>> {
        let object_size = match range {
            BytesRange::Range { size: Some(_), .. } => None,
            _ => Some(self.stat(path)?.content_length()),
        };
        let (offset, size) = match range {
            BytesRange::Range { offset, size } => {
                let size = size.unwrap_or_else(|| object_size.unwrap_or(0).saturating_sub(offset));
                (offset, size)
            }
            BytesRange::Suffix { size } => {
                let object_size = object_size.unwrap_or(0);
                (object_size.saturating_sub(size), size.min(object_size))
            }
        };
        if size == 0 {
            return Ok(Vec::new());
        }
        let offset = i64::try_from(offset).map_err(|source| {
            OpendalError::new(
                ErrorKind::RangeNotSatisfied,
                "Jindo read offset is too large",
            )
            .set_source(source)
        })?;
        let size_i64 = i64::try_from(size).map_err(|source| {
            OpendalError::new(ErrorKind::RangeNotSatisfied, "Jindo read size is too large")
                .set_source(source)
        })?;
        let size_usize = usize::try_from(size).map_err(|source| {
            OpendalError::new(ErrorKind::RangeNotSatisfied, "Jindo read size is too large")
                .set_source(source)
        })?;
        let path = to_cstring(&self.full_path(path), "Jindo path")?;
        let mut buffer = vec![0_u8; size_usize];
        let read = self.with_handle(|handle| {
            Ok(unsafe {
                (self.api.get_object)(
                    handle,
                    path.as_ptr(),
                    buffer.as_mut_ptr().cast(),
                    size_i64,
                    offset,
                    std::ptr::null_mut(),
                )
            })
        })?;
        if read <= 0 {
            return Ok(Vec::new());
        }
        buffer.truncate(read as usize);
        Ok(buffer)
    }

    fn with_handle<T>(&self, call: impl FnOnce(JdoPtr) -> OpendalResult<T>) -> OpendalResult<T> {
        let handle = unsafe { (self.api.create_handle)(self.store) };
        if handle.is_null() {
            return Err(OpendalError::new(
                ErrorKind::Unexpected,
                "Jindo failed to create an operation context",
            ));
        }
        let value = call(handle);
        let error = self.handle_error(handle);
        unsafe { (self.api.free_handle)(handle) };
        error?;
        value
    }

    fn handle_error(&self, handle: JdoPtr) -> OpendalResult<()> {
        let code = unsafe { (self.api.handle_error_code)(handle) };
        if code == 0 {
            return Ok(());
        }
        let message = unsafe {
            let ptr = (self.api.handle_error_message)(handle);
            if ptr.is_null() {
                String::new()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        let kind = match code {
            JDO_FILE_NOT_FOUND_ERROR | JDO_REST_HTTP_404_ERROR => ErrorKind::NotFound,
            JDO_REST_HTTP_403_ERROR => ErrorKind::PermissionDenied,
            JDO_REST_HTTP_503_ERROR => ErrorKind::RateLimited,
            _ => ErrorKind::Unexpected,
        };
        let error = OpendalError::new(kind, "Jindo operation failed")
            .with_context("code", code.to_string())
            .with_context("message", message);
        if matches!(code, 2000..=2003 | 6500 | 6502 | 6503) {
            Err(error.set_temporary())
        } else {
            Err(error)
        }
    }

    fn full_path(&self, path: &str) -> String {
        format!("{}{}", self.root, path.trim_start_matches('/'))
    }
}

impl Drop for JindoClient {
    fn drop(&mut self) {
        if self.initialized {
            let handle = unsafe { (self.api.create_handle)(self.store) };
            if !handle.is_null() {
                unsafe {
                    (self.api.destroy_store)(self.store);
                    (self.api.free_handle)(handle);
                }
            }
        }
        unsafe {
            (self.api.free_store)(self.store);
            (self.api.free_options)(self.options);
        }
    }
}

struct FileStatusGuard {
    api: Arc<JindoApi>,
    status: JdoPtr,
}

impl Drop for FileStatusGuard {
    fn drop(&mut self) {
        unsafe { (self.api.free_file_status)(self.status) };
    }
}

struct OptionsGuard {
    api: Arc<JindoApi>,
    options: JdoPtr,
}

impl Drop for OptionsGuard {
    fn drop(&mut self) {
        unsafe { (self.api.free_options)(self.options) };
    }
}

struct ListDirResultGuard {
    api: Arc<JindoApi>,
    result: JdoPtr,
}

impl Drop for ListDirResultGuard {
    fn drop(&mut self) {
        unsafe { (self.api.free_list_dir_result)(self.result) };
    }
}

unsafe fn c_string(value: *const c_char, name: &str) -> OpendalResult<String> {
    if value.is_null() {
        return Err(OpendalError::new(
            ErrorKind::Unexpected,
            format!("Jindo returned an empty {name}"),
        ));
    }
    Ok(unsafe { CStr::from_ptr(value) }
        .to_string_lossy()
        .into_owned())
}

fn relative_list_path(root: &str, full_path: &str, directory: bool) -> String {
    let mut path = full_path
        .strip_prefix(root)
        .unwrap_or(full_path)
        .trim_start_matches('/')
        .to_string();
    if directory && !path.ends_with('/') {
        path.push('/');
    }
    path
}

fn to_cstring(value: &str, name: &str) -> OpendalResult<CString> {
    CString::new(value).map_err(|source| {
        OpendalError::new(
            ErrorKind::ConfigInvalid,
            format!("{name} contains a NUL byte"),
        )
        .set_source(source)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_use_jindo() {
        assert!(!use_jindo(&HashMap::new()).unwrap());
        assert!(use_jindo(&HashMap::from([(
            OSS_IMPL.to_string(),
            "JINDO".to_string()
        )]))
        .unwrap());
        assert!(!use_jindo(&HashMap::from([(
            OSS_IMPL.to_string(),
            "legacy".to_string()
        )]))
        .unwrap());
        assert!(use_jindo(&HashMap::from([(
            OSS_IMPL.to_string(),
            "unknown".to_string()
        )]))
        .is_err());
    }

    #[test]
    fn test_parse_config() {
        let config = jindo_config_parse(HashMap::from([
            (OSS_IMPL.to_string(), JINDO_IMPL.to_string()),
            (
                JINDO_LIBRARY_PATH.to_string(),
                "/path/to/libjindosdk_c.so".to_string(),
            ),
            (
                "fs.oss.endpoint".to_string(),
                "https://oss-cn-hangzhou.aliyuncs.com".to_string(),
            ),
        ]))
        .unwrap();
        assert_eq!(
            config.library_path.as_deref(),
            Some(Path::new("/path/to/libjindosdk_c.so"))
        );
        assert_eq!(config.user, DEFAULT_JINDO_USER);
        assert_eq!(
            config.properties.get("fs.oss.endpoint").map(String::as_str),
            Some("oss-cn-hangzhou.aliyuncs.com")
        );
        assert_eq!(
            config.properties.get("logger.appender").map(String::as_str),
            Some("file")
        );
        assert!(!config.properties.contains_key(OSS_IMPL));
    }

    #[test]
    fn test_explicit_library_path_is_exclusive() {
        let path = Path::new("/custom/libjindosdk_c.so");
        assert_eq!(library_candidates(Some(path)), vec![path]);
    }

    #[test]
    fn test_build_does_not_load_sdk() {
        let config = JindoStorageConfig {
            library_path: Some(PathBuf::from("/missing/libjindosdk_c.so")),
            user: DEFAULT_JINDO_USER.to_string(),
            properties: HashMap::new(),
        };
        let operator = jindo_config_build(&config, "bucket").unwrap();
        assert!(operator.info().capability().list);
    }

    #[test]
    fn test_relative_list_path() {
        assert_eq!(
            relative_list_path(
                "oss://bucket/",
                "oss://bucket/table/snapshot/snapshot-1",
                false
            ),
            "table/snapshot/snapshot-1"
        );
        assert_eq!(
            relative_list_path("oss://bucket/", "table/snapshot", true),
            "table/snapshot/"
        );
    }
}
