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

use opendal::services::CosConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::error::Error;
use crate::Result;

const COS_ENDPOINT: &str = "fs.cosn.endpoint";
const COS_SECRET_ID: &str = "fs.cosn.userinfo.secretId";
const COS_SECRET_KEY: &str = "fs.cosn.userinfo.secretKey";

const CONFIG_PREFIXES: &[&str] = &["fs.cosn.", "cosn.", "cos."];
const MIRRORED_KEYS: &[(&str, &str)] = &[
    ("cos.endpoint", "cos.userinfo.endpoint"),
    ("cos.secret_id", "cos.userinfo.secretId"),
    ("cos.secret-id", "cos.userinfo.secretId"),
    ("cos.secret_key", "cos.userinfo.secretKey"),
    ("cos.secret-key", "cos.userinfo.secretKey"),
];

pub(crate) fn cos_config_parse(props: HashMap<String, String>) -> Result<CosConfig> {
    let normalized = normalize_config(props);
    let mut cfg = CosConfig::default();

    cfg.endpoint = normalized.get(COS_ENDPOINT).cloned();
    cfg.secret_id = normalized.get(COS_SECRET_ID).cloned();
    cfg.secret_key = normalized.get(COS_SECRET_KEY).cloned();

    if let Some(v) = normalized.get("fs.cosn.enable-versioning") {
        if v.eq_ignore_ascii_case("true") {
            cfg.enable_versioning = true;
        }
    }

    if let Some(v) = normalized.get("fs.cosn.disable-config-load") {
        if v.eq_ignore_ascii_case("true") {
            cfg.disable_config_load = true;
        }
    }

    Ok(cfg)
}

pub(crate) fn cos_config_build(cfg: &CosConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid COS url: {path}"),
    })?;

    let bucket = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid COS url: {path}, missing bucket"),
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);
    Ok(Operator::new(builder)?.finish())
}

fn normalize_config(props: HashMap<String, String>) -> HashMap<String, String> {
    let mut result = HashMap::new();

    for prefix in CONFIG_PREFIXES {
        for (key, value) in &props {
            if let Some(suffix) = key.strip_prefix(prefix) {
                result.insert(format!("fs.cosn.{suffix}"), value.clone());
            }
        }
    }

    let mirrored_additions: Vec<(String, String)> = MIRRORED_KEYS
        .iter()
        .flat_map(|(a, b)| {
            let mut pairs = Vec::new();
            let canonical_a = format!("fs.cosn.{}", a.strip_prefix("cos.").unwrap_or(a));
            let canonical_b = format!("fs.cosn.{}", b.strip_prefix("cos.").unwrap_or(b));

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
    fn test_cos_config_parse_hadoop_keys() {
        let props = make_props(&[
            ("fs.cosn.endpoint", "https://cos.ap-shanghai.myqcloud.com"),
            ("fs.cosn.userinfo.secretId", "sid"),
            ("fs.cosn.userinfo.secretKey", "skey"),
        ]);

        let cfg = cos_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://cos.ap-shanghai.myqcloud.com")
        );
        assert_eq!(cfg.secret_id.as_deref(), Some("sid"));
        assert_eq!(cfg.secret_key.as_deref(), Some("skey"));
    }

    #[test]
    fn test_cos_config_parse_canonical_aliases() {
        let props = make_props(&[
            ("cos.endpoint", "https://cos.ap-singapore.myqcloud.com"),
            ("cos.secret-id", "sid"),
            ("cos.secret-key", "skey"),
        ]);

        let cfg = cos_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://cos.ap-singapore.myqcloud.com")
        );
        assert_eq!(cfg.secret_id.as_deref(), Some("sid"));
        assert_eq!(cfg.secret_key.as_deref(), Some("skey"));
    }

    #[test]
    fn test_cos_config_build_extracts_bucket() {
        let mut cfg = CosConfig::default();
        cfg.endpoint = Some("https://cos.ap-shanghai.myqcloud.com".to_string());

        let op = cos_config_build(&cfg, "cosn://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_cos_config_build_missing_bucket() {
        let cfg = CosConfig::default();
        let result = cos_config_build(&cfg, "cosn:///path/without/bucket");
        assert!(result.is_err());
    }
}
