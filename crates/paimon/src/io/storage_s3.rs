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

use http::{header::IF_NONE_MATCH, Method, Request, Response, StatusCode};
use opendal::{
    Buffer, Configurator, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator,
};
use opendal_layer_retry::RetryLayer;
use opendal_service_s3::S3Config;
use url::Url;

use crate::error::Error;
use crate::Result;

use super::storage_config::normalize_storage_config;
use super::user_agent::StorageHttpTransport;

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

/// Configuration key for S3 session token.
///
/// Compatible with paimon-java's `s3.session-token` / `fs.s3a.session.token`.
const S3_SESSION_TOKEN: &str = "s3.session-token";

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

/// Number of retries after an S3 request fails.
pub(crate) const S3_RETRY_COUNT: &str = "s3.retry.count";

/// Initial exponential retry interval in milliseconds.
pub(crate) const S3_RETRY_INTERVAL_MILLIS: &str = "s3.retry.interval.millisecond";

const DEFAULT_S3_RETRY_COUNT: usize = 10;
const DEFAULT_S3_RETRY_INTERVAL_MILLIS: u64 = 500;

#[derive(Debug)]
pub struct S3StorageConfig {
    service: S3Config,
    retry_count: usize,
    retry_interval: Duration,
    user_agent: String,
}

/// Paimon-java config key prefixes that map to S3 configuration.
///
/// Java's `S3FileIO` normalizes all of these prefixes to `fs.s3a.*` before
/// passing them to Hadoop. Here we extract the suffix after matching prefix
/// and map to our canonical `s3.*` keys.
///
/// Reference: `S3FileIO.CONFIG_PREFIXES` in Java Paimon.
const JAVA_CONFIG_PREFIXES: &[&str] = &["fs.s3a.", "s3a.", "s3."];

/// External aliases mapped to the canonical keys read by [`S3Config`].
///
/// Reference: `S3FileIO.MIRRORED_CONFIG_KEYS` in Java Paimon.
const KEY_ALIASES: &[(&str, &str)] = &[
    ("s3.access.key", "s3.access-key"),
    ("s3.secret.key", "s3.secret-key"),
    ("s3.session.token", "s3.session-token"),
    ("s3.path.style.access", "s3.path-style-access"),
];

/// Parse paimon catalog options into an [`S3StorageConfig`].
///
/// Extracts S3-related configuration keys from the provided properties map.
/// Supports multiple key prefixes for Java compatibility (`s3.`, `s3a.`, `fs.s3a.`).
///
/// By default, virtual-hosted style addressing is enabled (matching AWS
/// and Java Paimon behavior). Set `s3.path-style-access=true` to switch
/// to path-style for S3-compatible stores like MinIO.
#[allow(clippy::field_reassign_with_default)]
pub(crate) fn s3_config_parse(props: HashMap<String, String>) -> Result<S3StorageConfig> {
    let user_agent = super::user_agent::storage_user_agent(&props);
    let normalized = normalize_storage_config(props, JAVA_CONFIG_PREFIXES, "s3.", KEY_ALIASES);

    let mut cfg = S3Config::default();

    // Default to virtual-hosted style, matching AWS and Java Paimon.
    // Only disable when path-style-access is explicitly set to true.
    let path_style_access = normalized
        .get(S3_PATH_STYLE_ACCESS)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    cfg.enable_virtual_host_style = !path_style_access;

    // Core connection settings.
    cfg.endpoint = normalized.get(S3_ENDPOINT).cloned();
    cfg.access_key_id = normalized.get(S3_ACCESS_KEY).cloned();
    cfg.secret_access_key = normalized.get(S3_SECRET_KEY).cloned();
    cfg.region = normalized.get(S3_REGION).cloned();

    // Session / assume-role credentials.
    cfg.session_token = normalized.get(S3_SESSION_TOKEN).cloned();
    cfg.role_arn = normalized.get("s3.assumed.role.arn").cloned();
    cfg.external_id = normalized.get("s3.assumed.role.externalId").cloned();
    cfg.role_session_name = normalized.get("s3.assumed.role.session.name").cloned();

    // Anonymous access.
    cfg.skip_signature = normalized
        .get("s3.anonymous")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    // Server-side encryption.
    cfg.server_side_encryption = normalized.get("s3.sse.type").cloned();
    cfg.server_side_encryption_aws_kms_key_id = normalized.get("s3.sse.key").cloned();
    cfg.server_side_encryption_customer_algorithm = normalized.get("s3.sse-c.algorithm").cloned();
    cfg.server_side_encryption_customer_key = normalized.get("s3.sse-c.key").cloned();
    cfg.server_side_encryption_customer_key_md5 = normalized.get("s3.sse-c.key.md5").cloned();

    // Storage class.
    cfg.default_storage_class = normalized.get("s3.storage.class").cloned();

    let retry_count = match normalized.get(S3_RETRY_COUNT) {
        Some(value) => value.parse().map_err(|_| Error::ConfigInvalid {
            message: format!("Invalid S3 config {S3_RETRY_COUNT}: {value}"),
        })?,
        None => DEFAULT_S3_RETRY_COUNT,
    };
    let retry_interval_millis = match normalized.get(S3_RETRY_INTERVAL_MILLIS) {
        Some(value) => value.parse().map_err(|_| Error::ConfigInvalid {
            message: format!("Invalid S3 config {S3_RETRY_INTERVAL_MILLIS}: {value}"),
        })?,
        None => DEFAULT_S3_RETRY_INTERVAL_MILLIS,
    };
    Ok(S3StorageConfig {
        service: cfg,
        retry_count,
        retry_interval: Duration::from_millis(retry_interval_millis),
        user_agent,
    })
}

/// Build an [`Operator`] for the given S3 path.
///
/// Parses the bucket name from the `s3://bucket/key` or `s3a://bucket/key`
/// URL and combines it with the provided [`S3StorageConfig`] to construct an
/// OpenDAL operator.
pub(crate) fn s3_config_build(cfg: &S3StorageConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid S3 url: {path}"),
    })?;

    let bucket = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid S3 url: {path}, missing bucket"),
    })?;

    let builder = cfg.service.clone().into_builder().bucket(bucket);
    let retry = RetryLayer::default()
        .with_notify(super::file_io::observe_retry)
        .with_min_delay(cfg.retry_interval)
        .with_max_times(cfg.retry_count)
        .with_jitter();
    Ok(with_s3_http_transport(Operator::new(builder)?, &cfg.user_agent).layer(retry))
}

fn with_s3_http_transport(op: Operator, user_agent: &str) -> Operator {
    let transport = HttpTransporter::new(S3HttpTransport(StorageHttpTransport::new(user_agent)));
    op.with_context(OperationContext::new().with_http_transport(transport))
}

#[derive(Debug, snafu::Snafu)]
#[snafu(display("S3 endpoint returned HTTP 501 for conditional PUT"))]
pub(super) struct ConditionalPutNotImplemented;

struct S3HttpTransport(StorageHttpTransport);

impl HttpTransport for S3HttpTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let conditional_put = request.method() == Method::PUT
            && request
                .headers()
                .get(IF_NONE_MATCH)
                .is_some_and(|value| value == "*");
        let response = self.0.fetch(request).await?;
        if conditional_put && response.status() == StatusCode::NOT_IMPLEMENTED {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "S3 conditional PUT is unsupported",
            )
            .set_source(ConditionalPutNotImplemented));
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{header, HeaderMap, Response, StatusCode};
    use axum::routing::get;
    use axum::Router;

    use super::*;

    fn storage_config(service: S3Config) -> S3StorageConfig {
        S3StorageConfig {
            service,
            retry_count: DEFAULT_S3_RETRY_COUNT,
            retry_interval: Duration::from_millis(1),
            user_agent: crate::io::user_agent::default_user_agent(),
        }
    }

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
            (S3_RETRY_COUNT, "7"),
            (S3_RETRY_INTERVAL_MILLIS, "250"),
        ]);

        let cfg = s3_config_parse(props).unwrap();
        assert_eq!(
            cfg.service.endpoint.as_deref(),
            Some("https://s3.us-east-1.amazonaws.com")
        );
        assert_eq!(cfg.service.access_key_id.as_deref(), Some("AKID"));
        assert_eq!(cfg.service.secret_access_key.as_deref(), Some("SECRET"));
        assert_eq!(cfg.service.region.as_deref(), Some("us-east-1"));
        assert_eq!(cfg.retry_count, 7);
        assert_eq!(cfg.retry_interval, Duration::from_millis(250));
    }

    #[test]
    fn test_s3_config_parse_hadoop_prefix() {
        // Keys with `fs.s3a.` prefix should be normalized to `s3.*`.
        let props = make_props(&[
            ("fs.s3a.endpoint", "https://s3.eu-west-1.amazonaws.com"),
            ("fs.s3a.access.key", "AKID2"),
            ("fs.s3a.secret.key", "SECRET2"),
        ]);

        let cfg = s3_config_parse(props).unwrap().service;
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://s3.eu-west-1.amazonaws.com")
        );
        // `fs.s3a.access.key` → `s3.access.key`, then aliased → `s3.access-key`
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

        let cfg = s3_config_parse(props).unwrap().service;
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

        let cfg = s3_config_parse(props).unwrap().service;
        assert!(cfg.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_parse_path_style_access_true() {
        let props = make_props(&[
            ("s3.endpoint", "https://minio.local:9000"),
            ("s3.path-style-access", "true"),
        ]);

        let cfg = s3_config_parse(props).unwrap().service;
        assert!(!cfg.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_parse_path_style_access_false() {
        // Explicit false should keep virtual-hosted style enabled.
        let props = make_props(&[
            ("s3.endpoint", "https://s3.amazonaws.com"),
            ("s3.path-style-access", "false"),
        ]);

        let cfg = s3_config_parse(props).unwrap().service;
        assert!(cfg.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_parse_no_credentials() {
        // Only endpoint, no credentials — valid for IAM role / env var auth.
        let props = make_props(&[("s3.endpoint", "https://s3.amazonaws.com")]);

        let cfg = s3_config_parse(props).unwrap().service;
        assert_eq!(cfg.endpoint.as_deref(), Some("https://s3.amazonaws.com"));
        assert!(cfg.access_key_id.is_none());
        assert!(cfg.secret_access_key.is_none());
    }

    #[test]
    fn test_s3_config_parse_empty_props() {
        let cfg = s3_config_parse(HashMap::new()).unwrap().service;
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

        let cfg = s3_config_parse(props).unwrap().service;
        assert_eq!(cfg.session_token.as_deref(), Some("TOKEN"));
        assert_eq!(
            cfg.role_arn.as_deref(),
            Some("arn:aws:iam::123456:role/test")
        );
        assert_eq!(cfg.external_id.as_deref(), Some("ext-id"));
    }

    #[test]
    fn test_s3_config_parse_session_token_follows_access_key_precedence() {
        let props = make_props(&[
            ("s3.access-key", "AK_A"),
            ("s3.secret-key", "SK_A"),
            ("s3.session-token", "TOKEN_A"),
            ("fs.s3a.access.key", "AK_B"),
            ("fs.s3a.secret.key", "SK_B"),
            ("fs.s3a.session.token", "TOKEN_B"),
        ]);

        let cfg = s3_config_parse(props).unwrap().service;
        assert_eq!(cfg.access_key_id.as_deref(), Some("AK_A"));
        assert_eq!(cfg.secret_access_key.as_deref(), Some("SK_A"));
        assert_eq!(cfg.session_token.as_deref(), Some("TOKEN_A"));
    }

    #[test]
    fn test_s3_config_parse_hadoop_session_token() {
        let props = make_props(&[("fs.s3a.session.token", "TOKEN")]);

        let cfg = s3_config_parse(props).unwrap().service;
        assert_eq!(cfg.session_token.as_deref(), Some("TOKEN"));
    }

    #[test]
    fn test_s3_config_parse_sse() {
        let props = make_props(&[("s3.sse.type", "aws:kms"), ("s3.sse.key", "my-kms-key-id")]);

        let cfg = s3_config_parse(props).unwrap().service;
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

        let cfg = s3_config_parse(props).unwrap().service;
        assert_eq!(cfg.endpoint.as_deref(), Some("https://s3.amazonaws.com"));
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_s3_config_build_extracts_bucket() {
        let mut cfg = S3Config::default();
        cfg.endpoint = Some("https://s3.us-east-1.amazonaws.com".to_string());
        cfg.region = Some("us-east-1".to_string());

        let op = s3_config_build(&storage_config(cfg), "s3://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_s3_config_build_s3a_scheme() {
        let mut cfg = S3Config::default();
        cfg.endpoint = Some("https://s3.us-east-1.amazonaws.com".to_string());
        cfg.region = Some("us-east-1".to_string());

        let op = s3_config_build(&storage_config(cfg), "s3a://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_s3_config_build_invalid_url() {
        let cfg = S3Config::default();
        let result = s3_config_build(&storage_config(cfg), "not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_config_build_missing_bucket() {
        let cfg = S3Config::default();
        let result = s3_config_build(&storage_config(cfg), "s3:///path/without/bucket");
        assert!(result.is_err());
    }

    #[test]
    fn test_key_aliases() {
        let props = make_props(&[("s3.access.key", "AKID")]);
        let normalized = normalize_storage_config(props, JAVA_CONFIG_PREFIXES, "s3.", KEY_ALIASES);
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

        let cfg = s3_config_parse(props).unwrap().service;
        assert_eq!(cfg.endpoint.as_deref(), Some("https://new.endpoint.com"));
    }

    #[test]
    fn s3_storage_uses_generic_user_agent_keys() {
        use crate::io::{FileIOBuilder, Storage};

        let storage = Storage::build(FileIOBuilder::new("s3").with_props([
            ("user-agent.features", "Flink"),
            ("user-agent.extended", "vvr"),
            ("dlf.access-tracking.extended-info", "uid/123"),
        ]))
        .unwrap();
        let Storage::S3 { config, .. } = storage else {
            panic!("expected S3 storage");
        };
        assert_eq!(
            config.user_agent,
            format!(
                "paimon-rust/{}(opendal/{};Flink) vvr uid/123",
                env!("CARGO_PKG_VERSION"),
                opendal::raw::VERSION
            )
        );
    }

    fn temporary_failure() -> Response<Body> {
        Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from(
                "<Error><Code>SlowDown</Code><Message>retry</Message></Error>",
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

    async fn record_user_agent(
        State(user_agents): State<Arc<Mutex<Vec<String>>>>,
        headers: HeaderMap,
    ) -> StatusCode {
        let user_agent = headers
            .get(header::USER_AGENT)
            .map(|value| value.to_str().unwrap().to_string())
            .unwrap_or_default();
        user_agents.lock().unwrap().push(user_agent);
        StatusCode::NOT_FOUND
    }

    #[test]
    fn test_s3_retry_defaults_and_validation() {
        let cfg = s3_config_parse(make_props(&[(S3_REGION, "us-east-1")])).unwrap();
        assert_eq!(cfg.retry_count, DEFAULT_S3_RETRY_COUNT);
        assert_eq!(
            cfg.retry_interval,
            Duration::from_millis(DEFAULT_S3_RETRY_INTERVAL_MILLIS)
        );

        let mut props = make_props(&[(S3_REGION, "us-east-1")]);
        props.insert(S3_RETRY_COUNT.to_string(), "invalid".to_string());
        assert!(s3_config_parse(props).is_err());
    }

    #[tokio::test]
    async fn test_s3_retries_temporary_failure() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(get(retry_once))
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut cfg = S3Config::default();
        cfg.endpoint = Some(format!("http://{address}"));
        cfg.enable_virtual_host_style = false;
        cfg.region = Some("us-east-1".to_string());
        cfg.skip_signature = true;

        let op = s3_config_build(&storage_config(cfg), "s3://bucket/path").unwrap();
        assert_eq!(op.read("object").await.unwrap().to_bytes(), "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn conditional_put_conflict_after_a_retry_has_an_unknown_outcome() {
        use axum::routing::put;
        use bytes::Bytes;

        let object = Arc::new(Mutex::new(None));
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/bucket/table/schema/schema-0",
            put({
                let object = object.clone();
                let attempts = attempts.clone();
                move |headers: HeaderMap, body: Bytes| {
                    let object = object.clone();
                    let attempts = attempts.clone();
                    async move {
                        assert_eq!(headers[header::IF_NONE_MATCH], "*");
                        attempts.fetch_add(1, Ordering::SeqCst);
                        let mut stored = object.lock().unwrap();
                        if stored.is_none() {
                            *stored = Some(body);
                            return temporary_failure();
                        }
                        Response::builder()
                            .status(StatusCode::PRECONDITION_FAILED)
                            .body(Body::from("<Error><Code>PreconditionFailed</Code></Error>"))
                            .unwrap()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let properties = [
            (S3_ENDPOINT, endpoint.as_str()),
            (S3_REGION, "us-east-1"),
            (S3_PATH_STYLE_ACCESS, "true"),
            ("s3.anonymous", "true"),
            (S3_RETRY_COUNT, "1"),
            (S3_RETRY_INTERVAL_MILLIS, "1"),
        ];
        let io = crate::io::FileIOBuilder::new("s3")
            .with_props(properties)
            .build()
            .unwrap();
        let path = "s3://bucket/table/schema/schema-0";
        let schema = Bytes::from_static(b"schema");
        let error = io
            .write_atomic_if_not_exists(path, schema.clone())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("outcome is unknown"), "{error}");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(object.lock().unwrap().as_ref(), Some(&schema));

        assert!(!io
            .write_atomic_if_not_exists(path, schema.clone())
            .await
            .unwrap());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        *object.lock().unwrap() = None;
        let cfg = s3_config_parse(make_props(&properties)).unwrap();
        let op = Operator::new(cfg.service.into_builder().bucket("bucket")).unwrap();
        let op = with_s3_http_transport(op, &cfg.user_agent).layer(
            RetryLayer::default()
                .with_max_times(1)
                .with_min_delay(Duration::from_millis(1)),
        );
        let custom = crate::io::FileIOBuilder::new("file")
            .with_fs_operator(op)
            .build()
            .unwrap();
        let error = custom
            .write_atomic_if_not_exists("/table/schema/schema-0", schema.clone())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("outcome is unknown"), "{error}");
        assert_eq!(attempts.load(Ordering::SeqCst), 5);
        assert_eq!(object.lock().unwrap().as_ref(), Some(&schema));
        server.abort();
    }

    #[tokio::test]
    async fn create_table_falls_back_only_after_definite_s3_conditional_put_rejection() {
        use axum::extract::{Path, Query};
        use axum::routing::{head, put};
        use bytes::Bytes;

        use crate::catalog::{Catalog, FileSystemCatalog, Identifier};
        use crate::common::{CatalogOptions, Options};
        use crate::spec::{DataType, IntType, Schema};

        let writes = Arc::new(Mutex::new(Vec::<(String, bool, Bytes)>::new()));
        let redirected_requests = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/publication-status",
                get({
                    let redirected_requests = redirected_requests.clone();
                    move || async move {
                        redirected_requests.fetch_add(1, Ordering::SeqCst);
                        StatusCode::NOT_IMPLEMENTED
                    }
                })
                .put({
                    let redirected_requests = redirected_requests.clone();
                    move || async move {
                        redirected_requests.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/bucket",
                get(|Query(query): Query<HashMap<String, String>>| async move {
                    if query.get("prefix").is_some_and(|p| p == "warehouse/db.db/") {
                        "<ListBucketResult><IsTruncated>false</IsTruncated><Contents>\
                         <Key>warehouse/db.db/</Key><Size>0</Size>\
                         <LastModified>2026-01-01T00:00:00Z</LastModified>\
                         </Contents></ListBucketResult>"
                    } else {
                        "<ListBucketResult/>"
                    }
                }),
            )
            .route(
                "/bucket/warehouse/db.db/:table/schema/",
                put(|| async { StatusCode::OK }),
            )
            .route(
                "/bucket/warehouse/db.db/:table/schema/schema-0",
                head(|| async { StatusCode::NOT_FOUND }).put({
                    let writes = writes.clone();
                    move |Path(table): Path<String>, headers: HeaderMap, body: Bytes| {
                        let writes = writes.clone();
                        async move {
                            let mut writes = writes.lock().unwrap();
                            let first = !writes.iter().any(|(name, _, _)| name == &table);
                            let conditional = headers.contains_key(header::IF_NONE_MATCH);
                            writes.push((table.clone(), conditional, body));
                            if !conditional {
                                return Response::new(Body::empty());
                            }
                            assert_eq!(headers[header::IF_NONE_MATCH], "*");
                            if table == "uncertain" && first {
                                return temporary_failure();
                            }
                            if table == "mislabeled_conflict" {
                                return Response::builder()
                                    .status(StatusCode::SERVICE_UNAVAILABLE)
                                    .body(Body::from(
                                        "<Error><Code>PreconditionFailed</Code></Error>",
                                    ))
                                    .unwrap();
                            }
                            let redirect = match table.as_str() {
                                "redirected" => Some(StatusCode::SEE_OTHER),
                                "misdirected" => Some(StatusCode::TEMPORARY_REDIRECT),
                                _ => None,
                            };
                            if let Some(status) = redirect {
                                return Response::builder()
                                    .status(status)
                                    .header(header::LOCATION, "/publication-status")
                                    .body(Body::empty())
                                    .unwrap();
                            }
                            Response::builder()
                                .status(StatusCode::NOT_IMPLEMENTED)
                                .body(Body::from("<Error><Code>NotImplemented</Code></Error>"))
                                .unwrap()
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut options = Options::new();
        for (key, value) in [
            (CatalogOptions::WAREHOUSE, "s3://bucket/warehouse"),
            (S3_ENDPOINT, endpoint.as_str()),
            (S3_REGION, "us-east-1"),
            (S3_PATH_STYLE_ACCESS, "true"),
            ("s3.anonymous", "true"),
            (S3_RETRY_COUNT, "1"),
            (S3_RETRY_INTERVAL_MILLIS, "1"),
        ] {
            options.set(key, value);
        }
        let catalog = FileSystemCatalog::new(options).unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let id = Identifier::new("db", "plain");
        let error = catalog
            .create_table_exclusive(&id, schema.clone())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::IoUnsupported { .. }), "{error:?}");
        catalog
            .create_table(&id, schema.clone(), false)
            .await
            .unwrap();
        for table in ["uncertain", "redirected", "mislabeled_conflict"] {
            let error = catalog
                .create_table(&Identifier::new("db", table), schema.clone(), true)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("outcome is unknown"), "{error}");
        }
        let error = catalog
            .create_table_exclusive(&Identifier::new("db", "misdirected"), schema)
            .await
            .expect_err("a successful write to a different object cannot confirm table creation");
        assert!(error.to_string().contains("outcome is unknown"), "{error}");

        let writes = writes.lock().unwrap();
        assert_eq!(
            writes
                .iter()
                .map(|(table, conditional, _)| (table.as_str(), *conditional))
                .collect::<Vec<_>>(),
            [
                ("plain", true),
                ("plain", true),
                ("plain", false),
                ("uncertain", true),
                ("uncertain", true),
                ("redirected", true),
                ("mislabeled_conflict", true),
                ("misdirected", true),
            ]
        );
        assert_eq!(writes[1].2, writes[2].2);
        assert_eq!(redirected_requests.load(Ordering::SeqCst), 0);
        server.abort();
    }

    #[tokio::test]
    async fn test_s3_retry_count_is_additional_attempts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(get(always_fail))
            .with_state(attempts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut props = make_props(&[(S3_REGION, "us-east-1")]);
        props.insert(S3_ENDPOINT.to_string(), format!("http://{address}"));
        props.insert(S3_RETRY_COUNT.to_string(), "1".to_string());
        props.insert(S3_RETRY_INTERVAL_MILLIS.to_string(), "1".to_string());
        let mut cfg = s3_config_parse(props).unwrap();
        cfg.service.enable_virtual_host_style = false;
        cfg.service.skip_signature = true;

        let op = s3_config_build(&cfg, "s3://bucket/path").unwrap();
        assert!(op.read("object").await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_s3_sends_configured_user_agent() {
        use crate::io::user_agent::{
            DLF_ACCESS_TRACKING_EXTENDED_INFO, USER_AGENT_EXTENDED, USER_AGENT_FEATURES,
        };

        let user_agents = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(record_user_agent)
            .with_state(user_agents.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut props = make_props(&[(S3_REGION, "us-east-1")]);
        props.insert(S3_ENDPOINT.to_string(), format!("http://{address}"));
        props.insert(USER_AGENT_FEATURES.to_string(), "DataFusion".to_string());
        props.insert(USER_AGENT_EXTENDED.to_string(), "vvr".to_string());
        props.insert(
            DLF_ACCESS_TRACKING_EXTENDED_INFO.to_string(),
            "uid/123".to_string(),
        );
        let mut cfg = s3_config_parse(props).unwrap();
        cfg.service.enable_virtual_host_style = false;
        cfg.service.skip_signature = true;

        let op = s3_config_build(&cfg, "s3://bucket/path").unwrap();
        assert!(!op.exists("object").await.unwrap());
        assert_eq!(
            *user_agents.lock().unwrap(),
            vec![format!(
                "paimon-rust/{}(opendal/{};DataFusion) vvr uid/123",
                env!("CARGO_PKG_VERSION"),
                opendal::raw::VERSION
            )]
        );
    }
}
