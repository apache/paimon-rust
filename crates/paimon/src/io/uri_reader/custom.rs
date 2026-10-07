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

//! Custom URI readers and Java's consecutive BlobRef source reuse.

use crate::{Error, Result};
use bytes::Bytes;
use std::sync::Arc;

/// Select a URI reader, corresponding to Java's UriReaderFactory.create.
/// Return the same Arc when references share the same reader identity.
pub trait UriReaderFactory: Send + Sync {
    fn create(&self, uri: &str) -> Result<Arc<dyn UriReader>>;
}

/// Open a source stream, corresponding to Java's UriReader.newInputStream.
#[async_trait::async_trait]
pub trait UriReader: Send + Sync {
    async fn new_input_stream(&self, uri: &str) -> Result<Box<dyn UriInputStream>>;
}

/// A Blob source stream. Empty reads mean EOF, and reads must not exceed the
/// requested length. Seek is optional for offset-zero or consecutive windows.
#[async_trait::async_trait]
pub trait UriInputStream: Send {
    async fn read(&mut self, length: usize) -> Result<Bytes>;

    async fn seek(&mut self, _offset: u64) -> Result<()> {
        Err(Error::Unsupported {
            message: "URI stream does not support seek".into(),
        })
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

struct Source {
    reader: Arc<dyn UriReader>,
    uri: String,
    stream: Box<dyn UriInputStream>,
    position: u64,
}

/// Keep one bounded-reference source per physical Blob writer. Unknown-length
/// references open their own streams, as Java ReusingBlobRefStreamProvider does.
#[derive(Default)]
pub(crate) struct ReusingBlobRefStreamProvider {
    source: Option<Source>,
}

impl ReusingBlobRefStreamProvider {
    pub(crate) async fn prepare(
        &mut self,
        reader: Arc<dyn UriReader>,
        uri: &str,
        offset: u64,
    ) -> Result<()> {
        if let Some(source) = &mut self.source {
            if Arc::ptr_eq(&source.reader, &reader) && source.uri == uri {
                if source.position == offset {
                    return Ok(());
                }
                if source.stream.seek(offset).await.is_ok() {
                    source.position = offset;
                    return Ok(());
                }
            }
            // A previous source's close failure must be surfaced before opening
            // this reference; do not mistake cleanup failure for a fetch failure.
            self.close().await?;
        }
        let mut stream = reader.new_input_stream(uri).await?;
        if offset != 0 {
            if let Err(error) = stream.seek(offset).await {
                let _ = stream.close().await;
                return Err(error);
            }
        }
        self.source = Some(Source {
            reader,
            uri: uri.into(),
            stream,
            position: offset,
        });
        Ok(())
    }

    pub(crate) fn stream(&mut self) -> &mut dyn UriInputStream {
        self.source
            .as_mut()
            .expect("source prepared")
            .stream
            .as_mut()
    }

    pub(crate) fn advance(&mut self, length: u64) {
        self.source.as_mut().expect("source prepared").position += length;
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        match self.source.take() {
            Some(mut source) => source.stream.close().await,
            None => Ok(()),
        }
    }
}
