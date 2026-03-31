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

use opendal::services::OssConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::error::Error;
use crate::Result;

/// Configuration key for OSS endpoint.
///
/// Compatible with paimon-java's `fs.oss.endpoint`.
pub(crate) const OSS_ENDPOINT: &str = "fs.oss.endpoint";

/// Configuration key for OSS access key ID.
///
/// Compatible with paimon-java's `fs.oss.accessKeyId`.
pub(crate) const OSS_ACCESS_KEY_ID: &str = "fs.oss.accessKeyId";

/// Configuration key for OSS access key secret.
///
/// Compatible with paimon-java's `fs.oss.accessKeySecret`.
pub(crate) const OSS_ACCESS_KEY_SECRET: &str = "fs.oss.accessKeySecret";

/// Configuration key for OSS STS security token (optional).
///
/// Compatible with paimon-java's `fs.oss.securityToken`.
/// Required when using STS temporary credentials (e.g. from REST data tokens).
pub(crate) const OSS_SECURITY_TOKEN: &str = "fs.oss.securityToken";

/// Parse paimon catalog options into an [`OssConfig`].
///
/// Extracts OSS-related configuration keys (endpoint, access key, secret key,
/// and optional security token) from the provided properties map and maps them
/// to the corresponding [`OssConfig`] fields.
///
/// Returns an error if any required configuration key is missing.
pub(crate) fn oss_config_parse(mut props: HashMap<String, String>) -> Result<OssConfig> {
    let mut cfg = OssConfig::default();

    cfg.endpoint = Some(
        props
            .remove(OSS_ENDPOINT)
            .ok_or_else(|| Error::ConfigInvalid {
                message: format!("Missing required OSS config: {OSS_ENDPOINT}"),
            })?,
    );
    cfg.access_key_id =
        Some(
            props
                .remove(OSS_ACCESS_KEY_ID)
                .ok_or_else(|| Error::ConfigInvalid {
                    message: format!("Missing required OSS config: {OSS_ACCESS_KEY_ID}"),
                })?,
        );
    cfg.access_key_secret =
        Some(
            props
                .remove(OSS_ACCESS_KEY_SECRET)
                .ok_or_else(|| Error::ConfigInvalid {
                    message: format!("Missing required OSS config: {OSS_ACCESS_KEY_SECRET}"),
                })?,
        );

    cfg.security_token = props.remove(OSS_SECURITY_TOKEN);
    Ok(cfg)
}

/// Build an [`Operator`] for the given OSS path.
///
/// Parses the bucket name from the `oss://bucket/key` URL and combines it
/// with the provided [`OssConfig`] to construct an OpenDAL operator.
pub(crate) fn oss_config_build(cfg: &OssConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid OSS url: {path}"),
    })?;

    let bucket = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid OSS url: {path}, missing bucket"),
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);
    Ok(Operator::new(builder)?.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oss_config_parse_with_all_keys() {
        let mut props = HashMap::new();
        props.insert(
            OSS_ENDPOINT.to_string(),
            "https://oss-cn-hangzhou.aliyuncs.com".to_string(),
        );
        props.insert(OSS_ACCESS_KEY_ID.to_string(), "test-ak".to_string());
        props.insert(OSS_ACCESS_KEY_SECRET.to_string(), "test-sk".to_string());

        let cfg = oss_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://oss-cn-hangzhou.aliyuncs.com")
        );
        assert_eq!(cfg.access_key_id.as_deref(), Some("test-ak"));
        assert_eq!(cfg.access_key_secret.as_deref(), Some("test-sk"));
    }

    #[test]
    fn test_oss_config_build_extracts_bucket() {
        let mut cfg = OssConfig::default();
        cfg.endpoint = Some("https://oss-cn-hangzhou.aliyuncs.com".to_string());

        let op = oss_config_build(&cfg, "oss://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_oss_config_build_invalid_url() {
        let cfg = OssConfig::default();
        let result = oss_config_build(&cfg, "not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_oss_config_build_missing_bucket() {
        let cfg = OssConfig::default();
        let result = oss_config_build(&cfg, "oss:///path/without/bucket");
        assert!(result.is_err());
    }
}
