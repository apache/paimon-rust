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

use datafusion::common::config::ConfigExtension;
use datafusion::common::{config_namespace, extensions_options};

pub const PAIMON_ROW_FILTER: &str = "paimon.read.row_filter";

config_namespace! {
    /// Paimon read options.
    pub struct PaimonReadOptions {
        /// Apply pushed predicates as row filters inside Paimon readers.
        pub row_filter: bool, default = false
    }
}

extensions_options! {
    /// Paimon-specific DataFusion session options.
    pub struct PaimonConfig {
        /// Options that control Paimon reads.
        pub read: PaimonReadOptions, default = PaimonReadOptions::default()
    }
}

impl ConfigExtension for PaimonConfig {
    const PREFIX: &'static str = "paimon";
}

#[cfg(test)]
mod tests {
    use datafusion::config::ConfigOptions;

    use super::*;

    #[test]
    fn paimon_row_filter_defaults_to_false_and_can_be_set() {
        let mut options = ConfigOptions::default();
        options.extensions.insert(PaimonConfig::default());

        let config = options.extensions.get::<PaimonConfig>().unwrap();
        assert!(!config.read.row_filter);

        options.set("paimon.read.row_filter", "true").unwrap();
        let config = options.extensions.get::<PaimonConfig>().unwrap();
        assert!(config.read.row_filter);
    }
}
