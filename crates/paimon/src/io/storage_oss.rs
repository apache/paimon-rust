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
use std::time::Duration;

use opendal::{Configurator, Operator};
use opendal_layer_retry::RetryLayer;
use opendal_service_oss::OssConfig;
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

/// Number of retries after an OSS request fails.
pub(crate) const OSS_RETRY_COUNT: &str = "fs.oss.retry.count";

/// Initial exponential retry interval in milliseconds.
pub(crate) const OSS_RETRY_INTERVAL_MILLIS: &str = "fs.oss.retry.interval.millisecond";

const DEFAULT_OSS_RETRY_COUNT: usize = 10;
const DEFAULT_OSS_RETRY_INTERVAL_MILLIS: u64 = 500;

#[derive(Debug)]
pub struct OssStorageConfig {
    service: OssConfig,
    retry_count: usize,
    retry_interval: Duration,
}

/// Parse paimon catalog options into an [`OssStorageConfig`].
///
/// Extracts OSS-related configuration keys (endpoint, access key, secret key,
/// optional security token, and retry settings) from the provided properties.
///
/// Returns an error if any required configuration key is missing.
pub(crate) fn oss_config_parse(mut props: HashMap<String, String>) -> Result<OssStorageConfig> {
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
    let retry_count = parse_retry_option(&mut props, OSS_RETRY_COUNT, DEFAULT_OSS_RETRY_COUNT)?;
    let retry_interval_millis = parse_retry_option(
        &mut props,
        OSS_RETRY_INTERVAL_MILLIS,
        DEFAULT_OSS_RETRY_INTERVAL_MILLIS,
    )?;
    Ok(OssStorageConfig {
        service: cfg,
        retry_count,
        retry_interval: Duration::from_millis(retry_interval_millis),
    })
}

fn parse_retry_option<T>(props: &mut HashMap<String, String>, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
{
    match props.remove(key) {
        Some(value) => value.parse().map_err(|_| Error::ConfigInvalid {
            message: format!("Invalid OSS config {key}: {value}"),
        }),
        None => Ok(default),
    }
}

/// Build an [`Operator`] for the given OSS path.
///
/// Parses the bucket name from the `oss://bucket/key` URL and combines it
/// with the provided [`OssStorageConfig`] to construct an OpenDAL operator.
pub(crate) fn oss_config_build(cfg: &OssStorageConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid OSS url: {path}"),
    })?;

    let bucket = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid OSS url: {path}, missing bucket"),
    })?;

    let builder = cfg.service.clone().into_builder().bucket(bucket);
    let retry = RetryLayer::default()
        .with_min_delay(cfg.retry_interval)
        .with_max_times(cfg.retry_count)
        .with_jitter();
    Ok(super::with_http_transport(Operator::new(builder)?).layer(retry))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{Response, StatusCode};
    use axum::routing::get;
    use axum::Router;

    use super::*;

    fn storage_config(service: OssConfig) -> OssStorageConfig {
        OssStorageConfig {
            service,
            retry_count: DEFAULT_OSS_RETRY_COUNT,
            retry_interval: Duration::from_millis(1),
        }
    }

    fn required_props() -> HashMap<String, String> {
        HashMap::from([
            (
                OSS_ENDPOINT.to_string(),
                "https://oss-cn-hangzhou.aliyuncs.com".to_string(),
            ),
            (OSS_ACCESS_KEY_ID.to_string(), "test-ak".to_string()),
            (OSS_ACCESS_KEY_SECRET.to_string(), "test-sk".to_string()),
        ])
    }

    fn temporary_failure() -> Response<Body> {
        Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from(
                "<Error><Code>QpsLimitExceeded</Code><Message>retry</Message></Error>",
            ))
            .unwrap()
    }

    async fn retry_once(State(attempts): State<Arc<AtomicUsize>>) -> Response<Body> {
        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return temporary_failure();
        }
        Response::new(Body::from("ok"))
    }

    async fn always_fail(State(attempts): State<Arc<AtomicUsize>>) -> Response<Body> {
        attempts.fetch_add(1, Ordering::SeqCst);
        temporary_failure()
    }

    #[test]
    fn test_oss_config_parse_with_all_keys() {
        let mut props = required_props();
        props.insert(OSS_RETRY_COUNT.to_string(), "7".to_string());
        props.insert(OSS_RETRY_INTERVAL_MILLIS.to_string(), "250".to_string());

        let cfg = oss_config_parse(props).unwrap();
        assert_eq!(
            cfg.service.endpoint.as_deref(),
            Some("https://oss-cn-hangzhou.aliyuncs.com")
        );
        assert_eq!(cfg.service.access_key_id.as_deref(), Some("test-ak"));
        assert_eq!(cfg.service.access_key_secret.as_deref(), Some("test-sk"));
        assert_eq!(cfg.retry_count, 7);
        assert_eq!(cfg.retry_interval, Duration::from_millis(250));
    }

    #[test]
    fn test_oss_retry_defaults_and_validation() {
        let cfg = oss_config_parse(required_props()).unwrap();
        assert_eq!(cfg.retry_count, DEFAULT_OSS_RETRY_COUNT);
        assert_eq!(
            cfg.retry_interval,
            Duration::from_millis(DEFAULT_OSS_RETRY_INTERVAL_MILLIS)
        );

        let mut props = required_props();
        props.insert(OSS_RETRY_COUNT.to_string(), "invalid".to_string());
        assert!(oss_config_parse(props).is_err());
    }

    #[test]
    fn test_oss_config_build_extracts_bucket() {
        let mut cfg = OssConfig::default();
        cfg.endpoint = Some("https://oss-cn-hangzhou.aliyuncs.com".to_string());

        let op = oss_config_build(&storage_config(cfg), "oss://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_oss_config_build_invalid_url() {
        let cfg = storage_config(OssConfig::default());
        let result = oss_config_build(&cfg, "not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_oss_config_build_missing_bucket() {
        let cfg = storage_config(OssConfig::default());
        let result = oss_config_build(&cfg, "oss:///path/without/bucket");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_oss_retries_temporary_failure() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(get(retry_once))
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut cfg = OssConfig::default();
        cfg.endpoint = Some(format!("http://{address}"));
        cfg.addressing_style = Some("path".to_string());
        cfg.skip_signature = true;

        let op = oss_config_build(&storage_config(cfg), "oss://bucket/path").unwrap();
        assert_eq!(op.read("object").await.unwrap().to_bytes(), "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_oss_retry_count_is_additional_attempts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(get(always_fail))
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut props = required_props();
        props.insert(OSS_ENDPOINT.to_string(), format!("http://{address}"));
        props.insert(OSS_RETRY_COUNT.to_string(), "1".to_string());
        props.insert(OSS_RETRY_INTERVAL_MILLIS.to_string(), "1".to_string());
        let mut cfg = oss_config_parse(props).unwrap();
        cfg.service.addressing_style = Some("path".to_string());
        cfg.service.skip_signature = true;

        let op = oss_config_build(&cfg, "oss://bucket/path").unwrap();
        assert!(op.read("object").await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
