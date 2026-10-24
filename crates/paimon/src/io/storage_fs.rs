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

use opendal::Operator;
use opendal_service_fs::FsConfig;

use crate::Result;

/// Build new opendal operator from give path.
pub(crate) fn fs_config_build() -> Result<Operator> {
    fs_config_build_with_root("/")
}

pub(crate) fn fs_config_build_with_root(root: &str) -> Result<Operator> {
    let mut cfg = FsConfig::default();
    cfg.root = Some(root.to_string());

    Ok(Operator::from_config(cfg)?)
}
