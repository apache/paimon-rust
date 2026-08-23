// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod auth;

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, FromRequestParts, Path as AxumPath, Request, State};
use axum::http::request::Parts;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use paimon::{CatalogFactory, Options};
use paimon_query_service::{
    BatchGetRequest, BatchGetResponse, BlobLookupOptions, BlobLookupService, DescriptorCacheStats,
    DescriptorFormat, LookupError, LookupKey, LookupStatus, TableCacheStats, TableLookupPolicy,
    TableRef,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::sync::{watch, Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::Instrument;
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::EnvFilter;

use crate::auth::{AuthPolicy, Principal};

pub use crate::auth::{PrincipalConfig, TableGrant};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

const REQUEST_ID_HEADER: &str = "x-request-id";
const HTTP_DURATION_BUCKETS: [(&str, u64); 10] = [
    ("0.001", 1_000),
    ("0.005", 5_000),
    ("0.01", 10_000),
    ("0.025", 25_000),
    ("0.05", 50_000),
    ("0.1", 100_000),
    ("0.25", 250_000),
    ("0.5", 500_000),
    ("1", 1_000_000),
    ("5", 5_000_000),
];
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static LOG_ERROR_COUNTER: OnceLock<ErrorCounter> = OnceLock::new();

pub struct LoggingGuard {
    _guard: WorkerGuard,
}

pub fn init_logging() -> Result<LoggingGuard, BoxError> {
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(8_192)
        .lossy(true)
        .finish(std::io::stderr());
    let error_counter = writer.error_counter();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_writer(writer)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    let _ = LOG_ERROR_COUNTER.set(error_counter);
    Ok(LoggingGuard { _guard: guard })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    pub catalog: HashMap<String, String>,
    pub policies: Vec<TableLookupPolicy>,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    #[serde(default)]
    pub principals: Vec<PrincipalConfig>,
    #[serde(default)]
    pub allow_anonymous: bool,
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    #[serde(default = "default_http_request_timeout_ms")]
    pub http_request_timeout_ms: u64,
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,
    #[serde(default = "default_max_concurrent_index_reads")]
    pub max_concurrent_index_reads: usize,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_http_header_read_timeout_ms")]
    pub http_header_read_timeout_ms: u64,
    #[serde(default = "default_http_connection_idle_timeout_ms")]
    pub http_connection_idle_timeout_ms: u64,
    #[serde(default = "default_http_connection_max_age_ms")]
    pub http_connection_max_age_ms: u64,
    #[serde(default = "default_http2_max_concurrent_streams")]
    pub http2_max_concurrent_streams: u32,
    #[serde(default = "default_graceful_shutdown_timeout_ms")]
    pub graceful_shutdown_timeout_ms: u64,
    #[serde(default = "default_queue_timeout_ms")]
    pub queue_timeout_ms: u64,
    #[serde(default = "default_max_request_body_bytes")]
    pub max_request_body_bytes: usize,
    #[serde(default = "default_max_response_body_bytes")]
    pub max_response_body_bytes: usize,
    #[serde(default = "default_readiness_cache_ttl_ms")]
    pub readiness_cache_ttl_ms: u64,
    #[serde(default = "default_table_metadata_cache_ttl_ms")]
    pub table_metadata_cache_ttl_ms: u64,
    #[serde(default = "default_descriptor_cache_ttl_ms")]
    pub descriptor_cache_ttl_ms: u64,
    #[serde(default = "default_descriptor_cache_max_bytes")]
    pub descriptor_cache_max_bytes: u64,
}

fn default_listen() -> String {
    "127.0.0.1:8081".to_string()
}

fn default_query_timeout_ms() -> u64 {
    5_000
}

fn default_http_request_timeout_ms() -> u64 {
    10_000
}

fn default_max_concurrent_queries() -> usize {
    64
}

fn default_max_concurrent_index_reads() -> usize {
    64
}

fn default_max_connections() -> usize {
    1_024
}

fn default_http_header_read_timeout_ms() -> u64 {
    5_000
}

fn default_http_connection_idle_timeout_ms() -> u64 {
    60_000
}

fn default_http_connection_max_age_ms() -> u64 {
    300_000
}

fn default_http2_max_concurrent_streams() -> u32 {
    64
}

fn default_graceful_shutdown_timeout_ms() -> u64 {
    10_000
}

fn default_queue_timeout_ms() -> u64 {
    100
}

fn default_max_request_body_bytes() -> usize {
    1024 * 1024
}

fn default_max_response_body_bytes() -> usize {
    4 * 1024 * 1024
}

fn default_readiness_cache_ttl_ms() -> u64 {
    1_000
}

fn default_table_metadata_cache_ttl_ms() -> u64 {
    30_000
}

fn default_descriptor_cache_ttl_ms() -> u64 {
    60_000
}

fn default_descriptor_cache_max_bytes() -> u64 {
    64 * 1024 * 1024
}

impl ServerConfig {
    fn validate(&self) -> Result<(), BoxError> {
        if self.query_timeout_ms == 0 {
            return Err(invalid_config("queryTimeoutMs must be positive"));
        }
        if self.http_request_timeout_ms == 0 {
            return Err(invalid_config("httpRequestTimeoutMs must be positive"));
        }
        if self.max_concurrent_queries == 0 || self.max_concurrent_queries > Semaphore::MAX_PERMITS
        {
            return Err(invalid_config(format!(
                "maxConcurrentQueries must be between 1 and {}",
                Semaphore::MAX_PERMITS
            )));
        }
        if self.max_concurrent_index_reads < self.max_concurrent_queries {
            return Err(invalid_config(
                "maxConcurrentIndexReads must be at least maxConcurrentQueries",
            ));
        }
        if self.max_connections == 0 || self.max_connections > Semaphore::MAX_PERMITS {
            return Err(invalid_config(format!(
                "maxConnections must be between 1 and {}",
                Semaphore::MAX_PERMITS
            )));
        }
        if self.http_header_read_timeout_ms == 0 {
            return Err(invalid_config("httpHeaderReadTimeoutMs must be positive"));
        }
        if self.http_connection_idle_timeout_ms <= self.http_request_timeout_ms {
            return Err(invalid_config(
                "httpConnectionIdleTimeoutMs must be greater than httpRequestTimeoutMs",
            ));
        }
        if self.http_connection_max_age_ms <= self.http_connection_idle_timeout_ms {
            return Err(invalid_config(
                "httpConnectionMaxAgeMs must be greater than httpConnectionIdleTimeoutMs",
            ));
        }
        if self.http2_max_concurrent_streams == 0 {
            return Err(invalid_config("http2MaxConcurrentStreams must be positive"));
        }
        if self.graceful_shutdown_timeout_ms == 0 {
            return Err(invalid_config("gracefulShutdownTimeoutMs must be positive"));
        }
        if self.queue_timeout_ms == 0 {
            return Err(invalid_config("queueTimeoutMs must be positive"));
        }
        if self.http_request_timeout_ms
            <= self.query_timeout_ms.saturating_add(self.queue_timeout_ms)
        {
            return Err(invalid_config(
                "httpRequestTimeoutMs must be greater than queryTimeoutMs + queueTimeoutMs",
            ));
        }
        if self.max_request_body_bytes == 0 {
            return Err(invalid_config("maxRequestBodyBytes must be positive"));
        }
        if self.max_response_body_bytes == 0 {
            return Err(invalid_config("maxResponseBodyBytes must be positive"));
        }
        Ok(())
    }
}

fn invalid_config(message: impl Into<String>) -> BoxError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into()).into()
}

pub fn load_config(path: impl AsRef<Path>) -> Result<ServerConfig, BoxError> {
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub async fn build_app(config: &ServerConfig) -> Result<Router, BoxError> {
    config.validate()?;
    let descriptor_cache_max_entry_bytes = u64::try_from(config.max_response_body_bytes)
        .map_err(|_| invalid_config("maxResponseBodyBytes does not fit the cache size type"))?;
    let auth = AuthPolicy::build(
        config.allow_anonymous,
        config.bearer_token.as_deref(),
        config.bearer_token_env.as_deref(),
        &config.principals,
        &config.policies,
    )?;
    let catalog = CatalogFactory::create(Options::from_map(config.catalog.clone())).await?;
    let lookup = BlobLookupService::new_with_options(
        catalog,
        config.policies.clone(),
        BlobLookupOptions {
            table_cache_ttl: Duration::from_millis(config.table_metadata_cache_ttl_ms),
            descriptor_cache_ttl: Duration::from_millis(config.descriptor_cache_ttl_ms),
            descriptor_cache_max_bytes: config.descriptor_cache_max_bytes,
            descriptor_cache_max_entry_bytes,
            global_index_thread_num: config.max_concurrent_index_reads
                / config.max_concurrent_queries,
        },
    )?;
    let metrics = Arc::new(ServerMetrics::default());
    Ok(router(
        AppState {
            lookup,
            auth,
            query_permits: Arc::new(Semaphore::new(config.max_concurrent_queries)),
            query_timeout: Duration::from_millis(config.query_timeout_ms),
            queue_timeout: Duration::from_millis(config.queue_timeout_ms),
            max_response_body_bytes: config.max_response_body_bytes,
            readiness: ReadinessCache::new(Duration::from_millis(config.readiness_cache_ttl_ms)),
            metrics,
        },
        config.max_request_body_bytes,
        Duration::from_millis(config.http_request_timeout_ms),
    ))
}

pub async fn serve(config: ServerConfig) -> Result<(), BoxError> {
    let address: SocketAddr = config.listen.parse()?;
    let app = build_app(&config).await?;
    let transport = TransportConfig {
        max_connections: config.max_connections,
        header_read_timeout: Duration::from_millis(config.http_header_read_timeout_ms),
        connection_idle_timeout: Duration::from_millis(config.http_connection_idle_timeout_ms),
        connection_max_age: Duration::from_millis(config.http_connection_max_age_ms),
        request_drain_timeout: Duration::from_millis(config.http_request_timeout_ms),
        http2_max_concurrent_streams: config.http2_max_concurrent_streams,
        graceful_shutdown_timeout: Duration::from_millis(config.graceful_shutdown_timeout_ms),
    };
    let listener = tokio::net::TcpListener::bind(address).await?;
    let bound_address = listener.local_addr()?;
    tracing::info!(
        event = "server_listening",
        address = %bound_address,
        max_connections = transport.max_connections,
        "query service is listening"
    );
    serve_listener(listener, app, transport).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct TransportConfig {
    max_connections: usize,
    header_read_timeout: Duration,
    connection_idle_timeout: Duration,
    connection_max_age: Duration,
    request_drain_timeout: Duration,
    http2_max_concurrent_streams: u32,
    graceful_shutdown_timeout: Duration,
}

async fn serve_listener(
    listener: tokio::net::TcpListener,
    app: Router,
    transport: TransportConfig,
) -> Result<(), BoxError> {
    let connection_permits = Arc::new(Semaphore::new(transport.max_connections));
    let (connection_shutdown, shutdown_receiver) = watch::channel(false);
    let mut connections = JoinSet::new();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => Some(accepted),
            _ = &mut shutdown => None,
            completed = connections.join_next(), if !connections.is_empty() => {
                log_connection_task_result(completed);
                continue;
            }
        };
        let Some(accepted) = accepted else {
            break;
        };
        let (stream, peer_address) = match accepted {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(%error, "failed to accept TCP connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let permit = match connection_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!(%peer_address, "connection limit reached");
                drop(stream);
                continue;
            }
        };
        let shutdown_receiver = shutdown_receiver.clone();
        let service = TowerToHyperService::new(app.clone());
        connections.spawn(async move {
            let _permit = permit;
            let stream = match read_protocol_prefix(stream, transport.header_read_timeout).await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(%peer_address, %error, "HTTP protocol detection failed");
                    return;
                }
            };
            let io = TokioIo::new(ReadIdleTimeout::new(
                stream,
                transport.connection_idle_timeout,
            ));
            let mut server = auto::Builder::new(TokioExecutor::new());
            server
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(transport.header_read_timeout)
                .max_headers(64);
            server
                .http2()
                .timer(TokioTimer::new())
                .max_concurrent_streams(transport.http2_max_concurrent_streams);
            let connection = server.serve_connection_with_upgrades(io, service);
            let result = drive_connection(
                connection,
                transport.connection_max_age,
                transport.request_drain_timeout,
                transport.graceful_shutdown_timeout,
                shutdown_receiver,
                |connection| connection.graceful_shutdown(),
            )
            .await;
            match result {
                ConnectionEnd::Completed(Ok(())) => {}
                ConnectionEnd::Drained {
                    reason,
                    output: Ok(()),
                } => {
                    tracing::debug!(%peer_address, ?reason, "HTTP connection drained");
                }
                ConnectionEnd::Completed(Err(error)) => {
                    tracing::debug!(%peer_address, %error, "HTTP connection closed with an error");
                }
                ConnectionEnd::Drained {
                    reason,
                    output: Err(error),
                } => {
                    tracing::debug!(%peer_address, ?reason, %error, "HTTP connection drain ended with an error");
                }
                ConnectionEnd::DrainTimedOut(ConnectionShutdownReason::MaxAge) => {
                    tracing::debug!(%peer_address, "HTTP connection max-age drain timed out");
                }
                ConnectionEnd::DrainTimedOut(ConnectionShutdownReason::ServerShutdown) => {
                    tracing::debug!(%peer_address, "HTTP connection shutdown drain timed out");
                }
            }
        });
    }

    let _ = connection_shutdown.send(true);
    let drain_connections = async {
        while let Some(completed) = connections.join_next().await {
            log_connection_task_result(Some(completed));
        }
    };
    if tokio::time::timeout(transport.graceful_shutdown_timeout, drain_connections)
        .await
        .is_err()
    {
        tracing::warn!("graceful shutdown deadline expired");
        connections.abort_all();
        while let Some(completed) = connections.join_next().await {
            log_connection_task_result(Some(completed));
        }
    }
    Ok(())
}

fn log_connection_task_result(result: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(error)) = result {
        if !error.is_cancelled() {
            tracing::warn!(%error, "HTTP connection task failed");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionShutdownReason {
    MaxAge,
    ServerShutdown,
}

enum ConnectionEnd<T> {
    Completed(T),
    Drained {
        reason: ConnectionShutdownReason,
        output: T,
    },
    DrainTimedOut(ConnectionShutdownReason),
}

async fn drive_connection<F, G>(
    connection: F,
    max_age: Duration,
    max_age_drain_timeout: Duration,
    server_drain_timeout: Duration,
    mut shutdown: watch::Receiver<bool>,
    graceful_shutdown: G,
) -> ConnectionEnd<F::Output>
where
    F: Future,
    G: FnOnce(Pin<&mut F>),
{
    let mut connection = Box::pin(connection);
    let max_age = tokio::time::sleep(max_age);
    tokio::pin!(max_age);
    let reason = tokio::select! {
        output = connection.as_mut() => return ConnectionEnd::Completed(output),
        _ = &mut max_age => ConnectionShutdownReason::MaxAge,
        _ = shutdown.changed() => ConnectionShutdownReason::ServerShutdown,
    };
    graceful_shutdown(connection.as_mut());
    let drain_timeout = match reason {
        ConnectionShutdownReason::MaxAge => max_age_drain_timeout,
        ConnectionShutdownReason::ServerShutdown => server_drain_timeout,
    };
    match tokio::time::timeout(drain_timeout, connection.as_mut()).await {
        Ok(output) => ConnectionEnd::Drained { reason, output },
        Err(_) => ConnectionEnd::DrainTimedOut(reason),
    }
}

const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

async fn read_protocol_prefix<T>(mut stream: T, timeout: Duration) -> std::io::Result<PrefixedIo<T>>
where
    T: AsyncRead + Unpin,
{
    let mut prefix = Vec::with_capacity(HTTP2_PREFACE.len());
    tokio::time::timeout(timeout, async {
        loop {
            let mut byte = [0_u8; 1];
            if stream.read(&mut byte).await? == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed before HTTP protocol detection",
                ));
            }
            prefix.push(byte[0]);
            if !HTTP2_PREFACE.starts_with(&prefix) || prefix.len() == HTTP2_PREFACE.len() {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "HTTP protocol preface exceeded its deadline",
        )
    })??;
    Ok(PrefixedIo {
        inner: stream,
        prefix,
        prefix_offset: 0,
    })
}

struct PrefixedIo<T> {
    inner: T,
    prefix: Vec<u8>,
    prefix_offset: usize,
}

impl<T: AsyncRead + Unpin> AsyncRead for PrefixedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.prefix_offset < self.prefix.len() && buffer.remaining() > 0 {
            let remaining = &self.prefix[self.prefix_offset..];
            let length = remaining.len().min(buffer.remaining());
            buffer.put_slice(&remaining[..length]);
            self.prefix_offset += length;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for PrefixedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

struct ReadIdleTimeout<T> {
    inner: T,
    timeout: Duration,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl<T> ReadIdleTimeout<T> {
    fn new(inner: T, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            sleep: Box::pin(tokio::time::sleep(timeout)),
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ReadIdleTimeout<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        match Pin::new(&mut self.inner).poll_read(context, buffer) {
            Poll::Ready(result) => {
                if result.is_ok() && buffer.filled().len() > before {
                    let deadline = tokio::time::Instant::now() + self.timeout;
                    self.sleep.as_mut().reset(deadline);
                }
                Poll::Ready(result)
            }
            Poll::Pending => match self.sleep.as_mut().poll(context) {
                Poll::Ready(()) => Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "HTTP connection exceeded its read-idle timeout",
                ))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ReadIdleTimeout<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

#[derive(Clone)]
struct AppState {
    lookup: BlobLookupService,
    auth: AuthPolicy,
    query_permits: Arc<Semaphore>,
    query_timeout: Duration,
    queue_timeout: Duration,
    max_response_body_bytes: usize,
    readiness: ReadinessCache,
    metrics: Arc<ServerMetrics>,
}

#[derive(Clone)]
struct ReadinessCache {
    ttl: Duration,
    cached: Arc<AsyncMutex<Option<CachedReadiness>>>,
}

#[derive(Clone, Copy)]
struct CachedReadiness {
    checked_at: Instant,
    ready: bool,
}

impl ReadinessCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            cached: Arc::new(AsyncMutex::new(None)),
        }
    }

    async fn check(&self, lookup: &BlobLookupService, metrics: &ServerMetrics) -> bool {
        // Keep the guard during refresh so concurrent probes collapse into a
        // single catalog traversal. Probe traffic must not amplify metadata I/O.
        let mut cached = self.cached.lock().await;
        if cached
            .as_ref()
            .is_some_and(|value| value.checked_at.elapsed() < self.ttl)
        {
            metrics.readiness_cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached.as_ref().is_some_and(|value| value.ready);
        }

        metrics.readiness_checks.fetch_add(1, Ordering::Relaxed);
        let ready = lookup.check_ready().await.is_ok();
        *cached = Some(CachedReadiness {
            checked_at: Instant::now(),
            ready,
        });
        ready
    }
}

#[derive(Clone)]
struct HttpTimeoutState {
    timeout: Duration,
    metrics: Arc<ServerMetrics>,
}

fn router(
    state: AppState,
    max_request_body_bytes: usize,
    http_request_timeout: Duration,
) -> Router {
    let metrics = state.metrics.clone();
    let timeout = HttpTimeoutState {
        timeout: http_request_timeout,
        metrics: metrics.clone(),
    };
    let query_admission = state.clone();
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics_endpoint))
        .route(
            "/api/blob/v1/databases/:database/tables/:table/descriptors:batchGet",
            post(batch_get)
                .route_layer(middleware::from_fn_with_state(query_admission, admit_query)),
        )
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state)
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .layer(middleware::from_fn_with_state(
            timeout,
            enforce_http_timeout,
        ))
        .layer(middleware::from_fn_with_state(metrics, request_context))
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<AppState>) -> Response {
    match tokio::time::timeout(
        state.query_timeout,
        state.readiness.check(&state.lookup, &state.metrics),
    )
    .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "NOT_READY",
            "one or more configured tables are unavailable or incompatible",
        ),
        Err(_) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "READINESS_TIMEOUT",
            "table readiness verification exceeded its deadline",
        ),
    }
}

async fn metrics_endpoint(State(state): State<AppState>) -> Response {
    let body = state.metrics.render(
        state.lookup.table_cache_stats(),
        state.lookup.descriptor_cache_stats(),
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn not_found() -> Response {
    api_error(StatusCode::NOT_FOUND, "ROUTE_NOT_FOUND", "route not found")
}

async fn method_not_allowed() -> Response {
    api_error(
        StatusCode::METHOD_NOT_ALLOWED,
        "METHOD_NOT_ALLOWED",
        "HTTP method is not allowed for this route",
    )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApiBatchGetRequest {
    keys: Vec<LookupKey>,
    blob_fields: Vec<String>,
    #[serde(default)]
    snapshot_id: Option<i64>,
    #[serde(default)]
    descriptor_format: DescriptorFormat,
}

#[derive(Clone)]
struct Authenticated(Principal);

#[axum::async_trait]
impl FromRequestParts<AppState> for Authenticated {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        let principal = state.auth.authenticate(token).ok_or_else(|| {
            state
                .metrics
                .auth_unauthorized
                .fetch_add(1, Ordering::Relaxed);
            unauthorized_response()
        })?;
        if let Some(context) = parts.extensions.get::<AccessContext>() {
            context.set_principal(principal.name());
        }
        Ok(Self(principal))
    }
}

async fn batch_get(
    Authenticated(principal): Authenticated,
    AxumPath((database, table)): AxumPath<(String, String)>,
    State(state): State<AppState>,
    request: Result<Json<ApiBatchGetRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let table = TableRef::new(database, table);
    if !principal.allows(&table, &request.blob_fields) {
        state.metrics.auth_forbidden.fetch_add(1, Ordering::Relaxed);
        return api_error(
            StatusCode::FORBIDDEN,
            "FORBIDDEN",
            "principal is not authorized for the requested table or BLOB fields",
        );
    }

    let request = BatchGetRequest {
        table,
        keys: request.keys,
        blob_fields: request.blob_fields,
        snapshot_id: request.snapshot_id,
        descriptor_format: request.descriptor_format,
    };
    let response =
        match tokio::time::timeout(state.query_timeout, state.lookup.batch_get(request)).await {
            Ok(Ok(response)) => {
                state.metrics.record_lookup(&response);
                success_response(response, state.max_response_body_bytes, &state.metrics)
            }
            Ok(Err(error)) => lookup_error_response(error),
            Err(_) => {
                state.metrics.query_timeouts.fetch_add(1, Ordering::Relaxed);
                api_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    "QUERY_TIMEOUT",
                    "blob descriptor query exceeded its deadline",
                )
            }
        };
    response
}

async fn admit_query(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let _permit = match acquire_query_permit(state.query_permits.clone(), state.queue_timeout).await
    {
        Ok(permit) => permit,
        Err(AdmissionError::TimedOut) => {
            state.metrics.query_rejected.fetch_add(1, Ordering::Relaxed);
            let mut response = api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "SERVER_BUSY",
                "query concurrency limit reached",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            return response;
        }
        Err(AdmissionError::Closed) => {
            state.metrics.query_rejected.fetch_add(1, Ordering::Relaxed);
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "SERVER_SHUTTING_DOWN",
                "query admission is closed",
            );
        }
    };
    state.metrics.query_started.fetch_add(1, Ordering::Relaxed);
    let _inflight = InflightQueryGuard::new(state.metrics.clone());
    next.run(request).await
}

fn success_response(
    response: BatchGetResponse,
    max_response_body_bytes: usize,
    metrics: &ServerMetrics,
) -> Response {
    let body = match serde_json::to_vec(&response) {
        Ok(body) => body,
        Err(_) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RESPONSE_SERIALIZATION_FAILED",
                "failed to serialize blob descriptor response",
            )
        }
    };
    if body.len() > max_response_body_bytes {
        metrics.responses_too_large.fetch_add(1, Ordering::Relaxed);
        return api_error(
            StatusCode::INSUFFICIENT_STORAGE,
            "RESPONSE_TOO_LARGE",
            "blob descriptor response exceeds the configured limit",
        );
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

fn json_rejection_response(rejection: JsonRejection) -> Response {
    match rejection {
        JsonRejection::JsonSyntaxError(_) => api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_JSON",
            "request body is not valid JSON",
        ),
        JsonRejection::JsonDataError(error) => api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            &error.body_text(),
        ),
        JsonRejection::MissingJsonContentType(_) => api_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            "Content-Type must be application/json",
        ),
        JsonRejection::BytesRejection(error) if error.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            api_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "REQUEST_TOO_LARGE",
                "JSON request body exceeds the configured limit",
            )
        }
        other => api_error(other.status(), "INVALID_REQUEST", &other.body_text()),
    }
}

fn unauthorized_response() -> Response {
    let mut response = api_error(
        StatusCode::UNAUTHORIZED,
        "UNAUTHORIZED",
        "missing or invalid bearer token",
    );
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

#[derive(Debug, Default)]
struct ServerMetrics {
    http_requests: AtomicU64,
    http_status_2xx: AtomicU64,
    http_status_3xx: AtomicU64,
    http_status_4xx: AtomicU64,
    http_status_5xx: AtomicU64,
    http_duration_micros: AtomicU64,
    http_duration_buckets: [AtomicU64; HTTP_DURATION_BUCKETS.len()],
    http_timeouts: AtomicU64,
    query_started: AtomicU64,
    query_rejected: AtomicU64,
    query_timeouts: AtomicU64,
    query_inflight: AtomicU64,
    auth_unauthorized: AtomicU64,
    auth_forbidden: AtomicU64,
    lookup_keys_found: AtomicU64,
    lookup_keys_not_found: AtomicU64,
    lookup_keys_non_unique: AtomicU64,
    planned_files: AtomicU64,
    planned_bytes: AtomicU64,
    responses_too_large: AtomicU64,
    readiness_checks: AtomicU64,
    readiness_cache_hits: AtomicU64,
}

impl ServerMetrics {
    fn record_http(&self, status: StatusCode, elapsed: Duration) {
        self.http_requests.fetch_add(1, Ordering::Relaxed);
        self.http_duration_micros.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let elapsed_micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        for (index, (_, upper_bound)) in HTTP_DURATION_BUCKETS.iter().enumerate() {
            if elapsed_micros <= *upper_bound {
                self.http_duration_buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        match status.as_u16() / 100 {
            2 => &self.http_status_2xx,
            3 => &self.http_status_3xx,
            4 => &self.http_status_4xx,
            5 => &self.http_status_5xx,
            _ => return,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_lookup(&self, response: &BatchGetResponse) {
        for result in &response.results {
            match result.status {
                LookupStatus::Found => &self.lookup_keys_found,
                LookupStatus::NotFound => &self.lookup_keys_not_found,
                LookupStatus::NonUnique => &self.lookup_keys_non_unique,
            }
            .fetch_add(1, Ordering::Relaxed);
        }
        if !response.cache_hit {
            self.planned_files.fetch_add(
                u64::try_from(response.scan.planned_files).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            self.planned_bytes
                .fetch_add(response.scan.planned_bytes, Ordering::Relaxed);
        }
    }

    fn render(
        &self,
        table_cache: TableCacheStats,
        descriptor_cache: DescriptorCacheStats,
    ) -> String {
        let requests = self.http_requests.load(Ordering::Relaxed);
        let duration_seconds =
            self.http_duration_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let duration_buckets = HTTP_DURATION_BUCKETS
            .iter()
            .enumerate()
            .map(|(index, (label, _))| {
                format!(
                    "paimon_query_service_http_request_duration_seconds_bucket{{le=\"{label}\"}} {}\n",
                    self.http_duration_buckets[index].load(Ordering::Relaxed)
                )
            })
            .collect::<String>();
        format!(
            concat!(
                "# HELP paimon_query_service_http_requests_total HTTP requests handled by this process.\n",
                "# TYPE paimon_query_service_http_requests_total counter\n",
                "paimon_query_service_http_requests_total {requests}\n",
                "# HELP paimon_query_service_http_responses_total HTTP responses by status class.\n",
                "# TYPE paimon_query_service_http_responses_total counter\n",
                "paimon_query_service_http_responses_total{{status_class=\"2xx\"}} {status_2xx}\n",
                "paimon_query_service_http_responses_total{{status_class=\"3xx\"}} {status_3xx}\n",
                "paimon_query_service_http_responses_total{{status_class=\"4xx\"}} {status_4xx}\n",
                "paimon_query_service_http_responses_total{{status_class=\"5xx\"}} {status_5xx}\n",
                "# HELP paimon_query_service_http_request_duration_seconds Total HTTP request duration.\n",
                "# TYPE paimon_query_service_http_request_duration_seconds histogram\n",
                "{duration_buckets}",
                "paimon_query_service_http_request_duration_seconds_bucket{{le=\"+Inf\"}} {requests}\n",
                "paimon_query_service_http_request_duration_seconds_sum {duration_seconds:.6}\n",
                "paimon_query_service_http_request_duration_seconds_count {requests}\n",
                "# HELP paimon_query_service_http_request_timeouts_total Requests terminated by the total HTTP deadline.\n",
                "# TYPE paimon_query_service_http_request_timeouts_total counter\n",
                "paimon_query_service_http_request_timeouts_total {http_timeouts}\n",
                "# HELP paimon_query_service_queries_started_total Admitted descriptor queries.\n",
                "# TYPE paimon_query_service_queries_started_total counter\n",
                "paimon_query_service_queries_started_total {query_started}\n",
                "# HELP paimon_query_service_queries_rejected_total Queries rejected by admission control.\n",
                "# TYPE paimon_query_service_queries_rejected_total counter\n",
                "paimon_query_service_queries_rejected_total {query_rejected}\n",
                "# HELP paimon_query_service_query_timeouts_total Descriptor query timeouts.\n",
                "# TYPE paimon_query_service_query_timeouts_total counter\n",
                "paimon_query_service_query_timeouts_total {query_timeouts}\n",
                "# HELP paimon_query_service_queries_inflight Currently executing descriptor queries.\n",
                "# TYPE paimon_query_service_queries_inflight gauge\n",
                "paimon_query_service_queries_inflight {query_inflight}\n",
                "# HELP paimon_query_service_auth_failures_total Authentication and authorization failures.\n",
                "# TYPE paimon_query_service_auth_failures_total counter\n",
                "paimon_query_service_auth_failures_total{{reason=\"unauthorized\"}} {auth_unauthorized}\n",
                "paimon_query_service_auth_failures_total{{reason=\"forbidden\"}} {auth_forbidden}\n",
                "# HELP paimon_query_service_lookup_keys_total Requested keys by lookup result.\n",
                "# TYPE paimon_query_service_lookup_keys_total counter\n",
                "paimon_query_service_lookup_keys_total{{status=\"found\"}} {keys_found}\n",
                "paimon_query_service_lookup_keys_total{{status=\"not_found\"}} {keys_not_found}\n",
                "paimon_query_service_lookup_keys_total{{status=\"non_unique\"}} {keys_non_unique}\n",
                "# HELP paimon_query_service_planned_files_total Data files selected by lookup plans.\n",
                "# TYPE paimon_query_service_planned_files_total counter\n",
                "paimon_query_service_planned_files_total {planned_files}\n",
                "# HELP paimon_query_service_planned_bytes_total Known data-file bytes selected by lookup plans.\n",
                "# TYPE paimon_query_service_planned_bytes_total counter\n",
                "paimon_query_service_planned_bytes_total {planned_bytes}\n",
                "# HELP paimon_query_service_responses_rejected_total Successful query results rejected by the response resource envelope.\n",
                "# TYPE paimon_query_service_responses_rejected_total counter\n",
                "paimon_query_service_responses_rejected_total{{reason=\"too_large\"}} {responses_too_large}\n",
                "# HELP paimon_query_service_readiness_checks_total Catalog readiness traversals executed.\n",
                "# TYPE paimon_query_service_readiness_checks_total counter\n",
                "paimon_query_service_readiness_checks_total {readiness_checks}\n",
                "# HELP paimon_query_service_readiness_cache_hits_total Readiness probes served from the short-lived cache.\n",
                "# TYPE paimon_query_service_readiness_cache_hits_total counter\n",
                "paimon_query_service_readiness_cache_hits_total {readiness_cache_hits}\n",
                "# HELP paimon_query_service_log_events_dropped_total Log events dropped because the non-blocking queue was full.\n",
                "# TYPE paimon_query_service_log_events_dropped_total counter\n",
                "paimon_query_service_log_events_dropped_total {log_events_dropped}\n",
                "# HELP paimon_query_service_table_cache_hits_total Table metadata cache hits.\n",
                "# TYPE paimon_query_service_table_cache_hits_total counter\n",
                "paimon_query_service_table_cache_hits_total {cache_hits}\n",
                "# HELP paimon_query_service_table_cache_misses_total Table metadata cache misses.\n",
                "# TYPE paimon_query_service_table_cache_misses_total counter\n",
                "paimon_query_service_table_cache_misses_total {cache_misses}\n",
                "# HELP paimon_query_service_table_cache_entries Table metadata cache entries.\n",
                "# TYPE paimon_query_service_table_cache_entries gauge\n",
                "paimon_query_service_table_cache_entries {table_cache_entries}\n",
                "# HELP paimon_query_service_descriptor_cache_hits_total Snapshot-pinned descriptor cache hits.\n",
                "# TYPE paimon_query_service_descriptor_cache_hits_total counter\n",
                "paimon_query_service_descriptor_cache_hits_total {descriptor_cache_hits}\n",
                "# HELP paimon_query_service_descriptor_cache_misses_total Snapshot-pinned descriptor cache misses.\n",
                "# TYPE paimon_query_service_descriptor_cache_misses_total counter\n",
                "paimon_query_service_descriptor_cache_misses_total {descriptor_cache_misses}\n",
                "# HELP paimon_query_service_descriptor_cache_entries Snapshot-pinned descriptor cache entries.\n",
                "# TYPE paimon_query_service_descriptor_cache_entries gauge\n",
                "paimon_query_service_descriptor_cache_entries {descriptor_cache_entries}\n",
                "# HELP paimon_query_service_descriptor_cache_bytes Approximate weighted descriptor cache bytes.\n",
                "# TYPE paimon_query_service_descriptor_cache_bytes gauge\n",
                "paimon_query_service_descriptor_cache_bytes {descriptor_cache_bytes}\n",
            ),
            requests = requests,
            duration_seconds = duration_seconds,
            duration_buckets = duration_buckets,
            status_2xx = self.http_status_2xx.load(Ordering::Relaxed),
            status_3xx = self.http_status_3xx.load(Ordering::Relaxed),
            status_4xx = self.http_status_4xx.load(Ordering::Relaxed),
            status_5xx = self.http_status_5xx.load(Ordering::Relaxed),
            http_timeouts = self.http_timeouts.load(Ordering::Relaxed),
            query_started = self.query_started.load(Ordering::Relaxed),
            query_rejected = self.query_rejected.load(Ordering::Relaxed),
            query_timeouts = self.query_timeouts.load(Ordering::Relaxed),
            query_inflight = self.query_inflight.load(Ordering::Relaxed),
            auth_unauthorized = self.auth_unauthorized.load(Ordering::Relaxed),
            auth_forbidden = self.auth_forbidden.load(Ordering::Relaxed),
            keys_found = self.lookup_keys_found.load(Ordering::Relaxed),
            keys_not_found = self.lookup_keys_not_found.load(Ordering::Relaxed),
            keys_non_unique = self.lookup_keys_non_unique.load(Ordering::Relaxed),
            planned_files = self.planned_files.load(Ordering::Relaxed),
            planned_bytes = self.planned_bytes.load(Ordering::Relaxed),
            responses_too_large = self.responses_too_large.load(Ordering::Relaxed),
            readiness_checks = self.readiness_checks.load(Ordering::Relaxed),
            readiness_cache_hits = self.readiness_cache_hits.load(Ordering::Relaxed),
            log_events_dropped = LOG_ERROR_COUNTER
                .get()
                .map(ErrorCounter::dropped_lines)
                .unwrap_or_default(),
            cache_hits = table_cache.hits,
            cache_misses = table_cache.misses,
            table_cache_entries = table_cache.entries,
            descriptor_cache_hits = descriptor_cache.hits,
            descriptor_cache_misses = descriptor_cache.misses,
            descriptor_cache_entries = descriptor_cache.entries,
            descriptor_cache_bytes = descriptor_cache.weighted_bytes,
        )
    }
}

struct InflightQueryGuard {
    metrics: Arc<ServerMetrics>,
}

impl InflightQueryGuard {
    fn new(metrics: Arc<ServerMetrics>) -> Self {
        metrics.query_inflight.fetch_add(1, Ordering::Relaxed);
        Self { metrics }
    }
}

impl Drop for InflightQueryGuard {
    fn drop(&mut self) {
        self.metrics.query_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionError {
    TimedOut,
    Closed,
}

async fn acquire_query_permit(
    permits: Arc<Semaphore>,
    queue_timeout: Duration,
) -> Result<OwnedSemaphorePermit, AdmissionError> {
    match tokio::time::timeout(queue_timeout, permits.acquire_owned()).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => Err(AdmissionError::Closed),
        Err(_) => Err(AdmissionError::TimedOut),
    }
}

async fn enforce_http_timeout(
    State(state): State<HttpTimeoutState>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(state.timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => {
            state.metrics.http_timeouts.fetch_add(1, Ordering::Relaxed);
            api_error(
                StatusCode::REQUEST_TIMEOUT,
                "REQUEST_TIMEOUT",
                "HTTP request exceeded its total deadline",
            )
        }
    }
}

async fn request_context(
    State(metrics): State<Arc<ServerMetrics>>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid_request_id(value))
        .map(str::to_string)
        .unwrap_or_else(new_request_id);
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let access_context = AccessContext::default();
    request.extensions_mut().insert(access_context.clone());
    let started = Instant::now();
    let span = tracing::info_span!(
        "http_request",
        request_id = %request_id,
        method = %method,
        path = %path,
    );
    let mut response = next.run(request).instrument(span.clone()).await;
    let elapsed = started.elapsed();
    let status = response.status().as_u16();
    let principal = access_context.principal();
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id).expect("generated request ID must be a header value"),
    );
    metrics.record_http(response.status(), elapsed);
    tracing::info!(
        parent: &span,
        event = "http_request_complete",
        principal = principal.as_deref().unwrap_or(""),
        status,
        elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        "HTTP request completed"
    );
    response
}

#[derive(Clone, Default)]
struct AccessContext(Arc<Mutex<Option<String>>>);

impl AccessContext {
    fn set_principal(&self, principal: &str) {
        if let Ok(mut current) = self.0.lock() {
            *current = Some(principal.to_string());
        }
    }

    fn principal(&self) -> Option<String> {
        self.0.lock().ok().and_then(|current| current.clone())
    }
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
}

fn new_request_id() -> String {
    let epoch_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{epoch_millis:x}-{:x}-{sequence:x}", std::process::id())
}

fn lookup_error_response(error: LookupError) -> Response {
    let mut root = &error;
    while let LookupError::Shared(inner) = root {
        root = inner;
    }
    let (status, code, message) = match root {
        LookupError::InvalidRequest(_) | LookupError::InvalidKeyValue { .. } => (
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            error.to_string(),
        ),
        LookupError::UnsupportedKeyType { .. } => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "UNSUPPORTED_KEY_TYPE",
            error.to_string(),
        ),
        LookupError::QueryBudgetExceeded { .. } => (
            StatusCode::TOO_MANY_REQUESTS,
            "QUERY_BUDGET_EXCEEDED",
            error.to_string(),
        ),
        LookupError::Paimon(paimon::Error::TableNotExist { .. }) => (
            StatusCode::NOT_FOUND,
            "TABLE_NOT_FOUND",
            "requested table does not exist".to_string(),
        ),
        LookupError::Paimon(paimon::Error::DatabaseNotExist { .. }) => (
            StatusCode::NOT_FOUND,
            "DATABASE_NOT_FOUND",
            "requested database does not exist".to_string(),
        ),
        LookupError::Paimon(paimon::Error::SnapshotNotExist { .. }) => (
            StatusCode::NOT_FOUND,
            "SNAPSHOT_NOT_FOUND",
            "requested snapshot does not exist".to_string(),
        ),
        LookupError::Paimon(_) | LookupError::PaimonUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "PAIMON_ERROR",
            "table metadata or data is temporarily unavailable".to_string(),
        ),
        LookupError::LoadCancelled => (
            StatusCode::SERVICE_UNAVAILABLE,
            "LOOKUP_CANCELLED",
            "the shared lookup attempt was cancelled; retry the request".to_string(),
        ),
        LookupError::InvalidPolicy(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "INVALID_POLICY",
            "the configured lookup policy is incompatible with the table".to_string(),
        ),
        LookupError::SnapshotMismatch { .. }
        | LookupError::InvalidDescriptor { .. }
        | LookupError::UnexpectedResult(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "LOOKUP_INVARIANT_VIOLATION",
            "the lookup result failed an internal consistency check".to_string(),
        ),
        LookupError::Shared(_) => unreachable!("shared lookup errors are unwrapped above"),
    };
    if status.is_server_error() {
        tracing::error!(%error, status = status.as_u16(), code, "blob descriptor lookup failed");
    }
    api_error(status, code, &message)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiError<'a> {
    code: &'a str,
    message: &'a str,
}

fn api_error(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(ApiError { code, message })).into_response()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn parses_minimal_config_with_defaults() {
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {"warehouse": "/tmp/warehouse"},
            "policies": [{
                "table": {"database": "db", "table": "assets"},
                "keyFields": ["id"],
                "blobFields": ["picture"],
                "strategy": "GLOBAL_BTREE"
            }]
        }))
        .unwrap();
        assert_eq!(config.listen, "127.0.0.1:8081");
        assert_eq!(config.policies[0].budget, Default::default());
        assert_eq!(config.query_timeout_ms, 5_000);
        assert_eq!(config.http_request_timeout_ms, 10_000);
        assert_eq!(config.max_concurrent_queries, 64);
        assert_eq!(config.max_concurrent_index_reads, 64);
        assert_eq!(config.max_connections, 1_024);
        assert_eq!(config.http_header_read_timeout_ms, 5_000);
        assert_eq!(config.http_connection_idle_timeout_ms, 60_000);
        assert_eq!(config.http_connection_max_age_ms, 300_000);
        assert_eq!(config.http2_max_concurrent_streams, 64);
        assert_eq!(config.graceful_shutdown_timeout_ms, 10_000);
        assert_eq!(config.queue_timeout_ms, 100);
        assert_eq!(config.max_request_body_bytes, 1024 * 1024);
        assert_eq!(config.max_response_body_bytes, 4 * 1024 * 1024);
        assert_eq!(config.readiness_cache_ttl_ms, 1_000);
        assert_eq!(config.table_metadata_cache_ttl_ms, 30_000);
        assert_eq!(config.descriptor_cache_ttl_ms, 60_000);
        assert_eq!(config.descriptor_cache_max_bytes, 64 * 1024 * 1024);
        assert!(config.principals.is_empty());
        assert!(!config.allow_anonymous);
    }

    #[test]
    fn rejects_unknown_security_and_policy_config_fields() {
        let root_typo = serde_json::from_value::<ServerConfig>(serde_json::json!({
            "catalog": {},
            "policies": [],
            "principlas": []
        }))
        .unwrap_err();
        assert!(root_typo.to_string().contains("principlas"));

        let grant_typo = serde_json::from_value::<ServerConfig>(serde_json::json!({
            "catalog": {},
            "policies": [{
                "table": {"database": "db", "table": "assets"},
                "keyFields": ["id"],
                "blobFields": ["picture"],
                "strategy": "GLOBAL_BTREE"
            }],
            "principals": [{
                "name": "reader",
                "bearerToken": "long-test-token-1234",
                "grants": [{
                    "table": {"database": "db", "table": "assets"},
                    "blobFeilds": ["picture"]
                }]
            }]
        }))
        .unwrap_err();
        assert!(grant_typo.to_string().contains("blobFeilds"));
    }

    #[tokio::test]
    async fn rejects_implicit_anonymous_access_before_catalog_creation() {
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {},
            "policies": []
        }))
        .unwrap();
        let error = match build_app(&config).await {
            Ok(_) => panic!("anonymous access must require an explicit opt-in"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("allowAnonymous"));
    }

    #[tokio::test]
    async fn rejects_zero_resource_limits_before_catalog_creation() {
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {},
            "policies": [],
            "maxConcurrentQueries": 0
        }))
        .unwrap();
        let error = match build_app(&config).await {
            Ok(_) => panic!("zero concurrency must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("maxConcurrentQueries"));

        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {},
            "policies": [],
            "maxConcurrentQueries": 2,
            "maxConcurrentIndexReads": 1
        }))
        .unwrap();
        let error = match build_app(&config).await {
            Ok(_) => panic!("an incoherent index-read envelope must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("maxConcurrentIndexReads"));
    }

    #[tokio::test]
    async fn rejects_total_deadline_that_cannot_contain_queue_and_query() {
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {},
            "policies": [],
            "queryTimeoutMs": 5000,
            "queueTimeoutMs": 100,
            "httpRequestTimeoutMs": 5100
        }))
        .unwrap();
        let error = match build_app(&config).await {
            Ok(_) => panic!("incoherent HTTP deadline must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("httpRequestTimeoutMs"));
    }

    #[tokio::test]
    async fn rejects_connection_max_age_shorter_than_idle_timeout() {
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {},
            "policies": [],
            "httpConnectionIdleTimeoutMs": 60_000,
            "httpConnectionMaxAgeMs": 60_000
        }))
        .unwrap();
        let error = match build_app(&config).await {
            Ok(_) => panic!("connection max age must be an absolute outer bound"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("httpConnectionMaxAgeMs"));
    }

    #[tokio::test]
    async fn http_route_enforces_auth_and_maps_missing_table() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {
                "warehouse": warehouse.path().to_str().unwrap()
            },
            "bearerToken": "legacy-secret-token-1234",
            "policies": [{
                "table": {"database": "db", "table": "assets"},
                "keyFields": ["id"],
                "blobFields": ["picture"],
                "strategy": "GLOBAL_BTREE"
            }]
        }))
        .unwrap();
        let app = build_app(&config).await.unwrap();
        let uri = "/api/blob/v1/databases/db/tables/assets/descriptors:batchGet";
        let body = r#"{"keys":[{"id":1}],"blobFields":["picture"]}"#;

        let unauthorized = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(REQUEST_ID_HEADER, "caller-123")
                    .body(Body::from("{not-json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(unauthorized.headers()[REQUEST_ID_HEADER], "caller-123");
        assert_eq!(unauthorized.headers()[header::WWW_AUTHENTICATE], "Bearer");

        let missing = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer legacy-secret-token-1234")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert!(valid_request_id(
            missing.headers()[REQUEST_ID_HEADER].to_str().unwrap()
        ));

        let not_ready = app
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn principal_grants_enforce_table_and_blob_field_access() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {
                "warehouse": warehouse.path().to_str().unwrap()
            },
            "policies": [{
                "table": {"database": "db", "table": "assets"},
                "keyFields": ["id"],
                "blobFields": ["picture", "thumbnail"],
                "strategy": "GLOBAL_BTREE"
            }],
            "principals": [{
                "name": "image-reader",
                "bearerToken": "image-reader-token-1234",
                "grants": [{
                    "table": {"database": "db", "table": "assets"},
                    "blobFields": ["picture"]
                }]
            }]
        }))
        .unwrap();
        let app = build_app(&config).await.unwrap();
        let uri = "/api/blob/v1/databases/db/tables/assets/descriptors:batchGet";

        let forbidden = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer image-reader-token-1234")
                    .body(Body::from(
                        r#"{"keys":[{"id":1}],"blobFields":["thumbnail"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer image-reader-token-1234")
                    .body(Body::from(
                        r#"{"keys":[{"id":1}],"blobFields":["picture"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::NOT_FOUND);

        let metrics = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = to_bytes(metrics.into_body(), 64 * 1024).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("paimon_query_service_auth_failures_total{reason=\"forbidden\"} 1"));
    }

    #[tokio::test]
    async fn rejects_oversized_json_body_and_adds_request_id() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {
                "warehouse": warehouse.path().to_str().unwrap()
            },
            "allowAnonymous": true,
            "policies": [],
            "maxRequestBodyBytes": 16
        }))
        .unwrap();
        let response = build_app(&config)
            .await
            .unwrap()
            .oneshot(
                Request::post("/api/blob/v1/databases/db/tables/assets/descriptors:batchGet")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"keys":[{"id":1}],"blobFields":["picture"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(valid_request_id(
            response.headers()[REQUEST_ID_HEADER].to_str().unwrap()
        ));
        assert_api_error(response, StatusCode::PAYLOAD_TOO_LARGE, "REQUEST_TOO_LARGE").await;
    }

    #[tokio::test]
    async fn returns_json_errors_for_invalid_json_content_type_route_and_method() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {
                "warehouse": warehouse.path().to_str().unwrap()
            },
            "allowAnonymous": true,
            "policies": []
        }))
        .unwrap();
        let app = build_app(&config).await.unwrap();
        let uri = "/api/blob/v1/databases/db/tables/assets/descriptors:batchGet";

        let invalid_json = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{not-json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_api_error(invalid_json, StatusCode::BAD_REQUEST, "INVALID_JSON").await;

        let missing_content_type = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .body(Body::from(r#"{"keys":[],"blobFields":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_api_error(
            missing_content_type,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
        )
        .await;

        let unknown_request_field = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"keys":[{"id":1}],"blobFields":["picture"],"snapshotID":1}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_api_error(
            unknown_request_field,
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
        )
        .await;

        let unknown_route = app
            .clone()
            .oneshot(Request::get("/unknown").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_api_error(unknown_route, StatusCode::NOT_FOUND, "ROUTE_NOT_FOUND").await;

        let wrong_method = app
            .oneshot(Request::post("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_api_error(
            wrong_method,
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
        )
        .await;
    }

    #[tokio::test]
    async fn admission_times_out_when_all_query_slots_are_held() {
        let permits = Arc::new(Semaphore::new(1));
        let _held = permits.clone().acquire_owned().await.unwrap();
        let error = match acquire_query_permit(permits, Duration::from_millis(1)).await {
            Ok(_) => panic!("admission should time out"),
            Err(error) => error,
        };
        assert_eq!(error, AdmissionError::TimedOut);
    }

    #[tokio::test]
    async fn protocol_detection_times_out_before_hyper_protocol_sniffing() {
        let (_client, server) = tokio::io::duplex(64);
        let error = match read_protocol_prefix(server, Duration::from_millis(1)).await {
            Ok(_) => panic!("an idle connection must not occupy a permit indefinitely"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn connection_max_age_gracefully_drains_active_work() {
        let (_shutdown_sender, shutdown_receiver) = watch::channel(false);
        let (graceful_sender, graceful_receiver) = tokio::sync::oneshot::channel();
        let connection = async move {
            graceful_receiver.await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            42
        };
        let result = drive_connection(
            connection,
            Duration::from_millis(1),
            Duration::from_millis(100),
            Duration::from_millis(100),
            shutdown_receiver,
            move |_| graceful_sender.send(()).unwrap(),
        )
        .await;
        match result {
            ConnectionEnd::Drained { reason, output } => {
                assert_eq!(reason, ConnectionShutdownReason::MaxAge);
                assert_eq!(output, 42);
            }
            _ => panic!("max age must signal graceful shutdown and await active work"),
        }
    }

    #[tokio::test]
    async fn connection_max_age_still_has_a_hard_drain_bound() {
        let (_shutdown_sender, shutdown_receiver) = watch::channel(false);
        let result = drive_connection(
            std::future::pending::<()>(),
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(100),
            shutdown_receiver,
            |_| {},
        )
        .await;
        assert!(matches!(
            result,
            ConnectionEnd::DrainTimedOut(ConnectionShutdownReason::MaxAge)
        ));
    }

    #[tokio::test]
    async fn admission_limit_is_acquired_before_json_body_parsing() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {"warehouse": warehouse.path().to_str().unwrap()},
            "policies": [],
            "allowAnonymous": true,
            "maxConcurrentQueries": 1,
            "maxConcurrentIndexReads": 1,
            "queueTimeoutMs": 1,
            "queryTimeoutMs": 100,
            "httpRequestTimeoutMs": 1000
        }))
        .unwrap();
        let app = build_app(&config).await.unwrap();
        let uri = "/api/blob/v1/databases/db/tables/assets/descriptors:batchGet";
        let pending_body = Body::from_stream(futures::stream::pending::<
            Result<Vec<u8>, std::convert::Infallible>,
        >());
        let first = tokio::spawn(
            app.clone().oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(pending_body)
                    .unwrap(),
            ),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;

        let rejected = app
            .oneshot(
                Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"keys":[],"blobFields":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_api_error(rejected, StatusCode::TOO_MANY_REQUESTS, "SERVER_BUSY").await;
        first.abort();
    }

    #[tokio::test]
    async fn total_http_deadline_returns_json_error_and_request_id() {
        let metrics = Arc::new(ServerMetrics::default());
        let timeout = HttpTimeoutState {
            timeout: Duration::from_millis(1),
            metrics: metrics.clone(),
        };
        let app = Router::new()
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    StatusCode::NO_CONTENT
                }),
            )
            .layer(middleware::from_fn_with_state(
                timeout,
                enforce_http_timeout,
            ))
            .layer(middleware::from_fn_with_state(
                metrics.clone(),
                request_context,
            ));

        let response = app
            .oneshot(Request::get("/slow").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_api_error(response, StatusCode::REQUEST_TIMEOUT, "REQUEST_TIMEOUT").await;
        assert_eq!(metrics.http_timeouts.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.http_status_4xx.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn rejects_success_response_over_configured_limit() {
        let metrics = ServerMetrics::default();
        let result = BatchGetResponse {
            table: TableRef::new("db", "assets"),
            snapshot_id: Some(1),
            schema_id: 1,
            cache_hit: false,
            scan: Default::default(),
            results: vec![],
        };

        let accepted = success_response(result.clone(), 4096, &metrics);
        assert_eq!(accepted.status(), StatusCode::OK);
        assert_eq!(accepted.headers()[header::CONTENT_TYPE], "application/json");
        let body = to_bytes(accepted.into_body(), 64 * 1024).await.unwrap();
        let decoded: BatchGetResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded, result);

        let response = success_response(result, 1, &metrics);

        assert_eq!(response.status(), StatusCode::INSUFFICIENT_STORAGE);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "RESPONSE_TOO_LARGE");
        assert_eq!(metrics.responses_too_large.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn hides_internal_paimon_errors_and_maps_missing_snapshots() {
        let internal = lookup_error_response(LookupError::Paimon(paimon::Error::DataInvalid {
            message: "secret path /warehouse/db/table".to_string(),
            source: None,
        }));
        assert_eq!(internal.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(internal.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "PAIMON_ERROR");
        assert!(!body["message"].as_str().unwrap().contains("/warehouse"));

        let missing = lookup_error_response(LookupError::Paimon(paimon::Error::SnapshotNotExist {
            snapshot_id: 17,
        }));
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(missing.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "SNAPSHOT_NOT_FOUND");

        let cancelled = lookup_error_response(LookupError::LoadCancelled);
        assert_eq!(cancelled.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(cancelled.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "LOOKUP_CANCELLED");
    }

    #[test]
    fn validates_caller_request_ids() {
        assert!(valid_request_id("trace-123:attempt_2"));
        assert!(!valid_request_id(""));
        assert!(!valid_request_id("has spaces"));
        assert!(!valid_request_id(&"x".repeat(129)));
    }

    #[tokio::test]
    async fn exposes_liveness_readiness_and_prometheus_metrics() {
        let warehouse = tempfile::TempDir::new().unwrap();
        let config: ServerConfig = serde_json::from_value(serde_json::json!({
            "catalog": {
                "warehouse": warehouse.path().to_str().unwrap()
            },
            "allowAnonymous": true,
            "policies": []
        }))
        .unwrap();
        let app = build_app(&config).await.unwrap();

        let health = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::NO_CONTENT);

        let ready = app
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::NO_CONTENT);

        let cached_ready = app
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(cached_ready.status(), StatusCode::NO_CONTENT);

        let metrics = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);
        assert_eq!(
            metrics.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = to_bytes(metrics.into_body(), 64 * 1024).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("paimon_query_service_http_requests_total 3"));
        assert!(body.contains("paimon_query_service_http_responses_total{status_class=\"2xx\"} 3"));
        assert!(body.contains("paimon_query_service_readiness_checks_total 1"));
        assert!(body.contains("paimon_query_service_readiness_cache_hits_total 1"));
        assert!(body.contains("paimon_query_service_table_cache_entries 0"));
    }

    #[test]
    fn inflight_metric_is_released_by_guard_drop() {
        let metrics = Arc::new(ServerMetrics::default());
        {
            let _guard = InflightQueryGuard::new(metrics.clone());
            assert_eq!(metrics.query_inflight.load(Ordering::Relaxed), 1);
        }
        assert_eq!(metrics.query_inflight.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn records_lookup_outcomes_and_planned_work() {
        use paimon_query_service::{LookupResult, LookupScanStats};

        let metrics = ServerMetrics::default();
        let response = BatchGetResponse {
            table: TableRef::new("db", "assets"),
            snapshot_id: Some(1),
            schema_id: 1,
            cache_hit: false,
            scan: LookupScanStats {
                planned_files: 3,
                planned_bytes: 1024,
            },
            results: vec![
                LookupResult {
                    key: BTreeMap::new(),
                    status: LookupStatus::Found,
                    blobs: BTreeMap::new(),
                },
                LookupResult {
                    key: BTreeMap::new(),
                    status: LookupStatus::NotFound,
                    blobs: BTreeMap::new(),
                },
                LookupResult {
                    key: BTreeMap::new(),
                    status: LookupStatus::NonUnique,
                    blobs: BTreeMap::new(),
                },
            ],
        };
        metrics.record_lookup(&response);
        metrics.record_lookup(&BatchGetResponse {
            cache_hit: true,
            ..response
        });

        let rendered = metrics.render(
            TableCacheStats::default(),
            DescriptorCacheStats {
                hits: 7,
                misses: 2,
                entries: 3,
                weighted_bytes: 4096,
            },
        );
        assert!(rendered.contains("lookup_keys_total{status=\"found\"} 2"));
        assert!(rendered.contains("lookup_keys_total{status=\"not_found\"} 2"));
        assert!(rendered.contains("lookup_keys_total{status=\"non_unique\"} 2"));
        assert!(rendered.contains("planned_files_total 3"));
        assert!(rendered.contains("planned_bytes_total 1024"));
        assert!(rendered.contains("descriptor_cache_hits_total 7"));
        assert!(rendered.contains("descriptor_cache_misses_total 2"));
        assert!(rendered.contains("descriptor_cache_entries 3"));
        assert!(rendered.contains("descriptor_cache_bytes 4096"));
    }

    async fn assert_api_error(response: Response, status: StatusCode, code: &str) {
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert!(valid_request_id(
            response.headers()[REQUEST_ID_HEADER].to_str().unwrap()
        ));
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], code);
        assert!(body["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()));
    }
}
