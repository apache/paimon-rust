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

use opendal::services::ObsConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::error::Error;
use crate::Result;

const OBS_ENDPOINT: &str = "fs.obs.endpoint";
const OBS_ACCESS_KEY_ID: &str = "fs.obs.access.key";
const OBS_SECRET_ACCESS_KEY: &str = "fs.obs.secret.key";

const CONFIG_PREFIXES: &[&str] = &["fs.obs.", "obs."];
const MIRRORED_KEYS: &[(&str, &str)] = &[
    ("obs.access-key-id", "obs.access.key"),
    ("obs.access_key_id", "obs.access.key"),
    ("obs.secret-access-key", "obs.secret.key"),
    ("obs.secret_access_key", "obs.secret.key"),
];

pub(crate) fn obs_config_parse(props: HashMap<String, String>) -> Result<ObsConfig> {
    let normalized = normalize_config(props);
    let mut cfg = ObsConfig::default();

    cfg.endpoint = normalized.get(OBS_ENDPOINT).cloned();
    cfg.access_key_id = normalized.get(OBS_ACCESS_KEY_ID).cloned();
    cfg.secret_access_key = normalized.get(OBS_SECRET_ACCESS_KEY).cloned();

    if let Some(v) = normalized.get("fs.obs.enable-versioning") {
        if v.eq_ignore_ascii_case("true") {
            cfg.enable_versioning = true;
        }
    }

    Ok(cfg)
}

pub(crate) fn obs_config_build(cfg: &ObsConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid OBS url: {path}"),
    })?;

    let bucket = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid OBS url: {path}, missing bucket"),
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);
    Ok(Operator::new(builder)?.finish())
}

fn normalize_config(props: HashMap<String, String>) -> HashMap<String, String> {
    let mut result = HashMap::new();

    for prefix in CONFIG_PREFIXES {
        for (key, value) in &props {
            if let Some(suffix) = key.strip_prefix(prefix) {
                result.insert(format!("fs.obs.{suffix}"), value.clone());
            }
        }
    }

    let mirrored_additions: Vec<(String, String)> = MIRRORED_KEYS
        .iter()
        .flat_map(|(a, b)| {
            let mut pairs = Vec::new();
            let canonical_a = format!("fs.obs.{}", a.strip_prefix("obs.").unwrap_or(a));
            let canonical_b = format!("fs.obs.{}", b.strip_prefix("obs.").unwrap_or(b));

            if !result.contains_key(&canonical_b) {
                if let Some(v) = result.get(&canonical_a) {
                    pairs.push((canonical_b.clone(), v.clone()));
                }
            }
            if !result.contains_key(&canonical_a) {
                if let Some(v) = result.get(&canonical_b) {
                    pairs.push((canonical_a.clone(), v.clone()));
                }
            }
            pairs
        })
        .collect();

    for (k, v) in mirrored_additions {
        result.insert(k, v);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_obs_config_parse_hadoop_keys() {
        let props = make_props(&[
            (
                "fs.obs.endpoint",
                "https://obs.cn-north-4.myhuaweicloud.com",
            ),
            ("fs.obs.access.key", "ak"),
            ("fs.obs.secret.key", "sk"),
        ]);

        let cfg = obs_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://obs.cn-north-4.myhuaweicloud.com")
        );
        assert_eq!(cfg.access_key_id.as_deref(), Some("ak"));
        assert_eq!(cfg.secret_access_key.as_deref(), Some("sk"));
    }

    #[test]
    fn test_obs_config_parse_canonical_aliases() {
        let props = make_props(&[
            ("obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com"),
            ("obs.access-key-id", "ak"),
            ("obs.secret-access-key", "sk"),
        ]);

        let cfg = obs_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://obs.cn-north-4.myhuaweicloud.com")
        );
        assert_eq!(cfg.access_key_id.as_deref(), Some("ak"));
        assert_eq!(cfg.secret_access_key.as_deref(), Some("sk"));
    }

    #[test]
    fn test_obs_config_build_extracts_bucket() {
        let mut cfg = ObsConfig::default();
        cfg.endpoint = Some("https://obs.cn-north-4.myhuaweicloud.com".to_string());

        let op = obs_config_build(&cfg, "obs://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_obs_config_build_missing_bucket() {
        let cfg = ObsConfig::default();
        let result = obs_config_build(&cfg, "obs:///path/without/bucket");
        assert!(result.is_err());
    }
}
