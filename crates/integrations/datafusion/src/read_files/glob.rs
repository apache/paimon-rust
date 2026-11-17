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

use ::glob::Pattern;
use datafusion::error::Result;

use super::error;

/// Match file or directory names at their specified depth. A matched directory
/// includes its descendants, just as a literal directory passed to read_files.
#[derive(Clone, Debug)]
pub(super) struct PathGlob {
    components: Vec<Pattern>,
    directory: bool,
}

impl PathGlob {
    pub(super) fn new(pattern: &str) -> Result<Self> {
        let directory = pattern.ends_with('/');
        let components = pattern
            .trim_end_matches('/')
            .split('/')
            .map(|part| Pattern::new(part).map_err(error))
            .collect::<Result<_>>()?;
        Ok(Self {
            components,
            directory,
        })
    }

    pub(super) fn matches(&self, relative: &str) -> bool {
        let parts = relative.split('/').collect::<Vec<_>>();
        let matched = self.components.len();
        parts.len() >= matched + usize::from(self.directory)
            && self
                .components
                .iter()
                .zip(parts)
                .all(|(pattern, part)| pattern.matches(part))
    }
}
