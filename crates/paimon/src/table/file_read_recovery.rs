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

//! File-scoped read recovery, matching Java DataFileRecordReader.

use crate::io::FileIO;
use crate::spec::CoreOptions;
use crate::{Error, Result};
use std::error::Error as StdError;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Clone, Default)]
pub(super) struct FileReadRecoveryState(Arc<AtomicBool>);

impl FileReadRecoveryState {
    pub(super) fn skipped(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub(super) struct FileReadRecovery {
    ignore_lost: bool,
    ignore_corrupt: bool,
    state: Option<FileReadRecoveryState>,
}

impl FileReadRecovery {
    pub(super) fn new(options: &CoreOptions<'_>) -> Self {
        Self {
            ignore_lost: options.scan_ignore_lost_file(),
            ignore_corrupt: options.scan_ignore_corrupt_file(),
            state: None,
        }
    }

    pub(super) fn with_state(mut self, state: Option<FileReadRecoveryState>) -> Self {
        self.state = state;
        self
    }

    fn record_skip(&self) {
        if let Some(state) = &self.state {
            state.0.store(true, Ordering::Relaxed);
        }
    }

    /// Reader creation checks the actual file, like Java createReader. A
    /// missing file needs ignore-lost-files even when corrupt files are ignored.
    /// Status/authorization errors from this check must propagate.
    pub(super) async fn opened<T>(
        &self,
        io: &FileIO,
        path: &str,
        result: Result<T>,
    ) -> Result<Option<T>> {
        match result {
            Ok(reader) => Ok(Some(reader)),
            Err(error) => {
                if !(self.ignore_lost || self.ignore_corrupt) || !recoverable(&error) {
                    return Err(error);
                }
                let exists = io.exists(path).await?;
                if (exists && self.ignore_corrupt) || (!exists && self.ignore_lost) {
                    self.record_skip();
                    log::warn!("Skipping unreadable data file {path}: {error}");
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// After reader creation Java only applies ignore-corrupt-files. Rows
    /// already returned stay visible; the remaining part of this file ends.
    pub(super) fn skip_batch_error(&self, path: &str, error: &Error) -> bool {
        if self.ignore_corrupt && recoverable(error) {
            self.record_skip();
            log::warn!("Stopping unreadable data file {path}: {error}");
            true
        } else {
            false
        }
    }
}

fn recoverable(error: &Error) -> bool {
    // These errors describe file I/O or decoding. Configuration, resource,
    // catalog and unsupported-operation failures are not corrupt-file signals.
    matches!(
        error,
        Error::IoUnexpected { .. }
            | Error::ParquetDataUnexpected { .. }
            | Error::DataUnexpected { .. }
            | Error::DataInvalid { .. }
    ) && !protected_error(error)
}

fn protected_error(error: &(dyn StdError + 'static)) -> bool {
    if let Some(error) = error.downcast_ref::<Error>() {
        if error.is_process_fork_unsupported()
            || matches!(
                error,
                Error::ResourceExhausted { .. }
                    | Error::ConfigInvalid { .. }
                    | Error::Unsupported { .. }
                    | Error::IoUnsupported { .. }
                    | Error::DataTypeInvalid { .. }
            )
        {
            return true;
        }
        // UnexpectedError deliberately has no std::error::Error::source().
        if let Error::UnexpectedError {
            source: Some(source),
            ..
        }
        | Error::DataInvalid {
            source: Some(source),
            ..
        } = error
        {
            return protected_error(source.as_ref());
        }
    }
    error.source().is_some_and(protected_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn opening_protects_internal_errors_and_marks_only_real_recovery() {
        let io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/recovery/present.parquet";
        io.new_output(path)
            .unwrap()
            .write(bytes::Bytes::from_static(b"present"))
            .await
            .unwrap();
        let options = HashMap::from([
            ("scan.ignore-lost-files".into(), "true".into()),
            ("scan.ignore-corrupt-files".into(), "true".into()),
        ]);
        let state = FileReadRecoveryState::default();
        let policy =
            FileReadRecovery::new(&CoreOptions::new(&options)).with_state(Some(state.clone()));
        for error in [
            Error::ResourceExhausted {
                message: "budget".into(),
            },
            Error::ConfigInvalid {
                message: "bad option".into(),
            },
            Error::ProcessForkUnsupported {
                message: "spawn required".into(),
            },
            Error::Unsupported {
                message: "feature".into(),
            },
            Error::DataInvalid {
                message: "wrapped budget".into(),
                source: Some(Box::new(Error::ResourceExhausted {
                    message: "budget".into(),
                })),
            },
            Error::from(parquet::errors::ParquetError::External(Box::new(
                Error::ResourceExhausted {
                    message: "wrapped budget".into(),
                },
            ))),
        ] {
            assert!(!policy.skip_batch_error(path, &error));
            assert!(policy.opened::<()>(&io, path, Err(error)).await.is_err());
            assert!(!state.skipped());
        }
        assert!(policy.opened(&io, path, Ok(7)).await.unwrap() == Some(7));
        assert!(!state.skipped());
        let error = Error::from(parquet::errors::ParquetError::General("bad page".into()));
        assert!(policy
            .opened::<()>(&io, path, Err(error))
            .await
            .unwrap()
            .is_none());
        assert!(state.skipped());
    }

    #[test]
    fn lost_files_option_does_not_ignore_late_io_failures() {
        let options = HashMap::from([("scan.ignore-lost-files".into(), "true".into())]);
        let policy = FileReadRecovery::new(&CoreOptions::new(&options));
        let error = Error::from(opendal::Error::new(
            opendal::ErrorKind::NotFound,
            "deleted after open",
        ));
        assert!(!policy.skip_batch_error("memory:/late.parquet", &error));
        let options = HashMap::from([("scan.ignore-corrupt-files".into(), "true".into())]);
        let policy = FileReadRecovery::new(&CoreOptions::new(&options));
        assert!(policy.skip_batch_error("memory:/late.parquet", &error));
    }
}
