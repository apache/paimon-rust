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

use super::storage_config::normalize_storage_config;

const COS_ENDPOINT: &str = "fs.cosn.endpoint";
const COS_SECRET_ID: &str = "fs.cosn.userinfo.secretId";
const COS_SECRET_KEY: &str = "fs.cosn.userinfo.secretKey";

const CONFIG_PREFIXES: &[&str] = &["fs.cosn.", "cosn.", "cos."];
const MIRRORED_KEYS: &[(&str, &str)] = &[
    ("fs.cosn.endpoint", "fs.cosn.userinfo.endpoint"),
    ("fs.cosn.secret_id", "fs.cosn.userinfo.secretId"),
    ("fs.cosn.secret-id", "fs.cosn.userinfo.secretId"),
    ("fs.cosn.secret_key", "fs.cosn.userinfo.secretKey"),
    ("fs.cosn.secret-key", "fs.cosn.userinfo.secretKey"),
];

pub(crate) fn cos_config_parse(props: HashMap<String, String>) -> Result<CosConfig> {
    let normalized = normalize_storage_config(props, CONFIG_PREFIXES, "fs.cosn.", MIRRORED_KEYS);

    let cfg = CosConfig {
        endpoint: normalized.get(COS_ENDPOINT).cloned(),
        secret_id: normalized.get(COS_SECRET_ID).cloned(),
        secret_key: normalized.get(COS_SECRET_KEY).cloned(),
        enable_versioning: normalized
            .get("fs.cosn.enable-versioning")
            .is_some_and(|v| v.eq_ignore_ascii_case("true")),
        disable_config_load: normalized
            .get("fs.cosn.disable-config-load")
            .is_some_and(|v| v.eq_ignore_ascii_case("true")),
        ..Default::default()
    };

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
        let cfg = CosConfig {
            endpoint: Some("https://cos.ap-shanghai.myqcloud.com".to_string()),
            ..Default::default()
        };

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
