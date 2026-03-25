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

use opendal::services::S3Config;
use opendal::{Configurator, Operator};
use url::Url;

use crate::error::Error;
use crate::Result;

/// Configuration key for S3 endpoint.
///
/// Compatible with paimon-java's `s3.endpoint` / `fs.s3a.endpoint`.
const S3_ENDPOINT: &str = "s3.endpoint";

/// Configuration key for S3 access key ID.
///
/// Compatible with paimon-java's `s3.access-key` / `fs.s3a.access.key`.
const S3_ACCESS_KEY: &str = "s3.access-key";

/// Configuration key for S3 secret key.
///
/// Compatible with paimon-java's `s3.secret-key` / `fs.s3a.secret.key`.
const S3_SECRET_KEY: &str = "s3.secret-key";

/// Configuration key for S3 path-style access.
///
/// When set to `true`, uses path-style URLs (`https://s3.endpoint/bucket`)
/// instead of virtual-hosted style (`https://bucket.s3.endpoint`).
///
/// Compatible with paimon-java's `s3.path-style-access` / `fs.s3a.path.style.access`.
const S3_PATH_STYLE_ACCESS: &str = "s3.path-style-access";

/// Configuration key for S3 region.
///
/// Compatible with paimon-java's `s3.region`.
const S3_REGION: &str = "s3.region";

/// Paimon-java config key prefixes that map to S3 configuration.
///
/// Java's `S3FileIO` normalizes all of these prefixes to `fs.s3a.*` before
/// passing them to Hadoop. Here we extract the suffix after matching prefix
/// and map to our canonical `s3.*` keys.
///
/// Reference: `S3FileIO.CONFIG_PREFIXES` in Java Paimon.
const JAVA_CONFIG_PREFIXES: &[&str] = &["fs.s3a.", "s3a.", "s3."];

/// Mirrored config keys — Java Paimon maps these interchangeably.
/// Both directions are applied so users can use either form.
///
/// Reference: `S3FileIO.MIRRORED_CONFIG_KEYS` in Java Paimon.
const MIRRORED_KEYS: &[(&str, &str)] = &[
    ("s3.access-key", "s3.access.key"),
    ("s3.secret-key", "s3.secret.key"),
    ("s3.path-style-access", "s3.path.style.access"),
];

/// Parse paimon catalog options into an [`S3Config`].
///
/// Extracts S3-related configuration keys from the provided properties map.
/// Supports multiple key prefixes for Java compatibility (`s3.`, `s3a.`, `fs.s3a.`).
///
/// By default, virtual-hosted style addressing is enabled (matching AWS
/// and Java Paimon behavior). Set `s3.path-style-access=true` to switch
/// to path-style for S3-compatible stores like MinIO.
pub(crate) fn s3_config_parse(props: HashMap<String, String>) -> Result<S3Config> {
    let normalized = normalize_config(props);

    let mut cfg = S3Config::default();

    // Default to virtual-hosted style, matching AWS and Java Paimon.
    // Only disable when path-style-access is explicitly set to true.
    cfg.enable_virtual_host_style = true;

    // Core connection settings.
    cfg.endpoint = normalized.get(S3_ENDPOINT).cloned();
    cfg.access_key_id = normalized.get(S3_ACCESS_KEY).cloned();
    cfg.secret_access_key = normalized.get(S3_SECRET_KEY).cloned();
    cfg.region = normalized.get(S3_REGION).cloned();

    if let Some(v) = normalized.get(S3_PATH_STYLE_ACCESS) {
        if v.eq_ignore_ascii_case("true") {
            cfg.enable_virtual_host_style = false;
        }
    }

    // Session / assume-role credentials.
    cfg.session_token = normalized.get("s3.session.token").cloned();
    cfg.role_arn = normalized.get("s3.assumed.role.arn").cloned();
    cfg.external_id = normalized.get("s3.assumed.role.externalId").cloned();
    cfg.role_session_name = normalized.get("s3.assumed.role.session.name").cloned();

    // Anonymous access.
    if let Some(v) = normalized.get("s3.anonymous") {
        if v.eq_ignore_ascii_case("true") {
            cfg.allow_anonymous = true;
        }
    }

    // Server-side encryption.
    cfg.server_side_encryption = normalized.get("s3.sse.type").cloned();
    cfg.server_side_encryption_aws_kms_key_id = normalized.get("s3.sse.key").cloned();
    cfg.server_side_encryption_customer_algorithm = normalized.get("s3.sse-c.algorithm").cloned();
    cfg.server_side_encryption_customer_key = normalized.get("s3.sse-c.key").cloned();
    cfg.server_side_encryption_customer_key_md5 = normalized.get("s3.sse-c.key.md5").cloned();

    // Storage class.
    cfg.default_storage_class = normalized.get("s3.storage.class").cloned();

    Ok(cfg)
}

/// Build an [`Operator`] for the given S3 path.
///
/// Parses the bucket name from the `s3://bucket/key` or `s3a://bucket/key`
/// URL and combines it with the provided [`S3Config`] to construct an
/// OpenDAL operator.
pub(crate) fn s3_config_build(cfg: &S3Config, path: &str) -> Result<Operator> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid S3 url: {path}"),
    })?;

    let bucket = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid S3 url: {path}, missing bucket"),
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);
    Ok(Operator::new(builder)?.finish())
}

/// Normalize Java-compatible config keys to canonical `s3.*` form.
///
/// 1. Strips known prefixes (`fs.s3a.`, `s3a.`, `s3.`) and remaps to `s3.*`.
/// 2. Applies mirrored key mappings for cross-compatibility.
/// 3. Earlier prefixes in the list take lower priority (later ones overwrite).
fn normalize_config(props: HashMap<String, String>) -> HashMap<String, String> {
    let mut result = HashMap::new();

    // First pass: normalize prefixes. Process in priority order —
    // `fs.s3a.` (lowest) → `s3a.` → `s3.` (highest, canonical).
    for prefix in JAVA_CONFIG_PREFIXES {
        for (key, value) in &props {
            if let Some(suffix) = key.strip_prefix(prefix) {
                let canonical = format!("s3.{suffix}");
                result.insert(canonical, value.clone());
            }
        }
    }

    // Second pass: apply mirrored keys bidirectionally (only if target not already set).
    let mirrored_additions: Vec<(String, String)> = MIRRORED_KEYS
        .iter()
        .flat_map(|(a, b)| {
            let mut pairs = Vec::new();
            // a → b
            if !result.contains_key(*b) {
                if let Some(v) = result.get(*a) {
                    pairs.push((b.to_string(), v.clone()));
                }
            }
            // b → a
            if !result.contains_key(*a) {
                if let Some(v) = result.get(*b) {
                    pairs.push((a.to_string(), v.clone()));
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
    fn test_s3_config_parse_canonical_keys() {
        let props = make_props(&[
            ("s3.endpoint", "https://s3.us-east-1.amazonaws.com"),
            ("s3.access-key", "AKID"),
            ("s3.secret-key", "SECRET"),
            ("s3.region", "us-east-1"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://s3.us-east-1.amazonaws.com")
        );
        assert_eq!(cfg.access_key_id.as_deref(), Some("AKID"));
        assert_eq!(cfg.secret_access_key.as_deref(), Some("SECRET"));
        assert_eq!(cfg.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn test_s3_config_parse_hadoop_prefix() {
        // Keys with `fs.s3a.` prefix should be normalized to `s3.*`.
        let props = make_props(&[
            ("fs.s3a.endpoint", "https://s3.eu-west-1.amazonaws.com"),
            ("fs.s3a.access.key", "AKID2"),
            ("fs.s3a.secret.key", "SECRET2"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://s3.eu-west-1.amazonaws.com")
        );
        // `fs.s3a.access.key` → `s3.access.key`, then mirrored → `s3.access-key`
        assert_eq!(cfg.access_key_id.as_deref(), Some("AKID2"));
        assert_eq!(cfg.secret_access_key.as_deref(), Some("SECRET2"));
    }

    #[test]
    fn test_s3_config_parse_s3a_prefix() {
        let props = make_props(&[
            ("s3a.endpoint", "https://s3.ap-southeast-1.amazonaws.com"),
            ("s3a.access-key", "AKID3"),
            ("s3a.secret-key", "SECRET3"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://s3.ap-southeast-1.amazonaws.com")
        );
        assert_eq!(cfg.access_key_id.as_deref(), Some("AKID3"));
        assert_eq!(cfg.secret_access_key.as_deref(), Some("SECRET3"));
    }

    #[test]
    fn test_s3_config_default_virtual_hosted_style() {
        // Default should be virtual-hosted style (matching AWS behavior).
        let props = make_props(&[("s3.endpoint", "https://s3.amazonaws.com")]);

        let cfg = s3_config_parse(props).unwrap();
        assert!(cfg.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_parse_path_style_access_true() {
        let props = make_props(&[
            ("s3.endpoint", "https://minio.local:9000"),
            ("s3.path-style-access", "true"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert!(!cfg.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_parse_path_style_access_false() {
        // Explicit false should keep virtual-hosted style enabled.
        let props = make_props(&[
            ("s3.endpoint", "https://s3.amazonaws.com"),
            ("s3.path-style-access", "false"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert!(cfg.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_parse_no_credentials() {
        // Only endpoint, no credentials — valid for IAM role / env var auth.
        let props = make_props(&[("s3.endpoint", "https://s3.amazonaws.com")]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("https://s3.amazonaws.com"));
        assert!(cfg.access_key_id.is_none());
        assert!(cfg.secret_access_key.is_none());
    }

    #[test]
    fn test_s3_config_parse_empty_props() {
        let cfg = s3_config_parse(HashMap::new()).unwrap();
        assert!(cfg.endpoint.is_none());
        assert!(cfg.access_key_id.is_none());
        // Even with empty props, virtual-hosted style should be the default.
        assert!(cfg.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_parse_session_and_role() {
        let props = make_props(&[
            ("s3.endpoint", "https://s3.amazonaws.com"),
            ("s3.session.token", "TOKEN"),
            ("s3.assumed.role.arn", "arn:aws:iam::123456:role/test"),
            ("s3.assumed.role.externalId", "ext-id"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(cfg.session_token.as_deref(), Some("TOKEN"));
        assert_eq!(
            cfg.role_arn.as_deref(),
            Some("arn:aws:iam::123456:role/test")
        );
        assert_eq!(cfg.external_id.as_deref(), Some("ext-id"));
    }

    #[test]
    fn test_s3_config_parse_sse() {
        let props = make_props(&[("s3.sse.type", "aws:kms"), ("s3.sse.key", "my-kms-key-id")]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(cfg.server_side_encryption.as_deref(), Some("aws:kms"));
        assert_eq!(
            cfg.server_side_encryption_aws_kms_key_id.as_deref(),
            Some("my-kms-key-id")
        );
    }

    #[test]
    fn test_s3_config_unrelated_keys_ignored() {
        // Keys that don't match any S3 prefix should not affect config.
        let props = make_props(&[
            ("fs.oss.endpoint", "https://oss.aliyuncs.com"),
            ("hive.metastore.uris", "thrift://localhost:9083"),
            ("s3.endpoint", "https://s3.amazonaws.com"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("https://s3.amazonaws.com"));
    }

    #[test]
    fn test_s3_config_build_extracts_bucket() {
        let mut cfg = S3Config::default();
        cfg.endpoint = Some("https://s3.us-east-1.amazonaws.com".to_string());
        cfg.region = Some("us-east-1".to_string());

        let op = s3_config_build(&cfg, "s3://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_s3_config_build_s3a_scheme() {
        let mut cfg = S3Config::default();
        cfg.endpoint = Some("https://s3.us-east-1.amazonaws.com".to_string());
        cfg.region = Some("us-east-1".to_string());

        let op = s3_config_build(&cfg, "s3a://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_s3_config_build_invalid_url() {
        let cfg = S3Config::default();
        let result = s3_config_build(&cfg, "not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_config_build_missing_bucket() {
        let cfg = S3Config::default();
        let result = s3_config_build(&cfg, "s3:///path/without/bucket");
        assert!(result.is_err());
    }

    #[test]
    fn test_mirrored_keys() {
        // `s3.access.key` (dot form) should be mirrored from `s3.access-key` (dash form)
        let props = make_props(&[("s3.access-key", "AKID")]);
        let normalized = normalize_config(props);
        assert_eq!(
            normalized.get("s3.access.key").map(|s| s.as_str()),
            Some("AKID")
        );
        assert_eq!(
            normalized.get("s3.access-key").map(|s| s.as_str()),
            Some("AKID")
        );
    }

    #[test]
    fn test_canonical_overrides_hadoop_prefix() {
        // `s3.endpoint` should take priority over `fs.s3a.endpoint`.
        let props = make_props(&[
            ("fs.s3a.endpoint", "https://old.endpoint.com"),
            ("s3.endpoint", "https://new.endpoint.com"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("https://new.endpoint.com"));
    }
}
