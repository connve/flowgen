//! Cluster-wide logs and flow metrics: each pod serves its own data on an
//! internal port, and the stores here merge it across every peer.

use crate::cache::{Cache, CacheError};
use crate::flow::activity::{self, FlowMetricsSnapshot, MetricsStore, RecordedEvent};
use crate::peer::{Peer, PeerRegistry, RegisteredPeer};
use crate::telemetry::query::{LogFilter, LogsStore, LogsStoreError, MAX_QUERY_LIMIT};
use crate::telemetry::StoredLog;
use async_trait::async_trait;
use axum::extract::{Query, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, Mutex};
use tokio::task::{AbortHandle, JoinSet};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tracing::{debug, info, warn};

/// Internal endpoint answering with the [`Ping`] of a reachable peer.
const PING_PATH: &str = "/internal/ping";

/// Internal endpoint returning retained log records as a JSON array.
const LOGS_PATH: &str = "/internal/logs";

/// Internal endpoint streaming new log records as newline-delimited JSON.
const LOGS_STREAM_PATH: &str = "/internal/logs/stream";

/// Internal endpoint returning flow metrics as a JSON array.
const METRICS_PATH: &str = "/internal/metrics";

/// Internal endpoint streaming flow metrics as newline-delimited JSON.
const METRICS_STREAM_PATH: &str = "/internal/metrics/stream";

/// How long a read waits for one peer before leaving it out.
const PEER_QUERY_TIMEOUT: Duration = Duration::from_secs(3);

/// How long connecting to a peer may take.
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Empty-line heartbeat interval on idle streams.
const STREAM_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Silence after which a stream reconnects: three missed heartbeats.
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(45);

/// How often a peer sends the latest metrics of each changed flow.
const METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(500);

/// How often a live stream re-reads the peer list.
const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Items buffered between peer streams and one live subscriber.
const STREAM_CHANNEL_CAPACITY: usize = 1024;

/// System cache key holding the token pods present to each other.
pub const TOKEN_KEY: &str = "cluster.token";

/// Random bytes in a generated token.
const TOKEN_BYTES: usize = 32;

/// How long a pod reuses the token it read.
const TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Minimum age before a rejected token triggers another cache read.
const TOKEN_RECHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Errors returned by the cluster server and client setup.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// The HTTP client for reaching peers could not be built.
    #[error("Failed to build cluster HTTP client: {source}")]
    Client {
        #[source]
        source: reqwest::Error,
    },
    /// The internal endpoint could not bind its port.
    #[error("Failed to bind cluster listener on port {port}: {source}")]
    BindListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    /// The internal endpoint stopped with an error.
    #[error("Cluster server failed: {source}")]
    Serve {
        #[source]
        source: std::io::Error,
    },
    /// The cluster token could not be read from or written to the cache.
    #[error("Failed to access the cluster token in the cache: {source}")]
    TokenCache {
        #[source]
        source: CacheError,
    },
    /// The operating system could not supply random bytes for a new token.
    #[error("Failed to generate a cluster token: {source}")]
    TokenRandom {
        #[source]
        source: getrandom::Error,
    },
    /// The cached token is not valid UTF-8.
    #[error("Cluster token in the cache is not valid UTF-8: {source}")]
    TokenEncoding {
        #[source]
        source: std::string::FromUtf8Error,
    },
    /// Another pod created the token and it vanished before it was read.
    #[error("Cluster token disappeared from the cache while it was being created")]
    TokenVanished,
}

/// A token read from the cache, with when it was read.
struct KeptToken {
    token: SecretString,
    read_at: tokio::time::Instant,
}

/// Token pods present to each other, shared through the system cache.
pub struct ClusterToken {
    cache: Arc<dyn Cache>,
    kept: Mutex<Option<KeptToken>>,
}

impl std::fmt::Debug for ClusterToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterToken").finish_non_exhaustive()
    }
}

impl ClusterToken {
    /// `cache` must be shared by every pod and out of reach of `ctx.cache`.
    pub fn new(cache: Arc<dyn Cache>) -> Self {
        Self {
            cache,
            kept: Mutex::new(None),
        }
    }

    /// Returns the current token, creating it when no pod has yet.
    pub async fn current(&self) -> Result<SecretString, Error> {
        self.read(TOKEN_REFRESH_INTERVAL).await
    }

    async fn recheck(&self) -> Result<SecretString, Error> {
        self.read(TOKEN_RECHECK_INTERVAL).await
    }

    /// Replaces the token with a new random one.
    pub async fn regenerate(&self) -> Result<(), Error> {
        let mut kept = self.kept.lock().await;
        let token = generate_token()?;
        self.cache
            .put(TOKEN_KEY, Bytes::from(token.clone()), None)
            .await
            .map_err(|source| Error::TokenCache { source })?;
        *kept = Some(KeptToken {
            token: SecretString::from(token),
            read_at: tokio::time::Instant::now(),
        });
        Ok(())
    }

    async fn read(&self, max_age: Duration) -> Result<SecretString, Error> {
        let mut kept = self.kept.lock().await;
        if let Some(current) = kept.as_ref() {
            if current.read_at.elapsed() < max_age {
                return Ok(current.token.clone());
            }
        }
        let token = match (self.load_or_create().await, kept.as_ref()) {
            (Ok(token), _) => token,
            (Err(e), Some(previous)) => {
                warn!(error = %e, "Failed to read the cluster token, using the previous one");
                previous.token.clone()
            }
            (Err(e), None) => return Err(e),
        };
        *kept = Some(KeptToken {
            token: token.clone(),
            read_at: tokio::time::Instant::now(),
        });
        Ok(token)
    }

    async fn load_or_create(&self) -> Result<SecretString, Error> {
        if let Some(value) = self.load().await? {
            return Ok(value);
        }
        let token = generate_token()?;
        match self
            .cache
            .create(TOKEN_KEY, Bytes::from(token.clone()), None)
            .await
        {
            Ok(_) => {
                info!("Generated the cluster token");
                Ok(SecretString::from(token))
            }
            Err(CacheError::AlreadyExists) => match self.load().await? {
                Some(value) => Ok(value),
                None => Err(Error::TokenVanished),
            },
            Err(source) => Err(Error::TokenCache { source }),
        }
    }

    async fn load(&self) -> Result<Option<SecretString>, Error> {
        match self
            .cache
            .get(TOKEN_KEY)
            .await
            .map_err(|source| Error::TokenCache { source })?
        {
            Some(value) => match String::from_utf8(value.to_vec()) {
                Ok(token) => Ok(Some(SecretString::from(token))),
                Err(source) => Err(Error::TokenEncoding { source }),
            },
            None => Ok(None),
        }
    }
}

fn generate_token() -> Result<String, Error> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|source| Error::TokenRandom { source })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

/// [`LogFilter`] and `limit` as internal endpoint query parameters.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
struct FilterParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    flow: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    levels: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    since_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    until_ms: Option<u64>,
}

impl FilterParams {
    fn new(filter: &LogFilter, limit: Option<usize>) -> Self {
        Self {
            limit,
            flow: filter.flow.clone(),
            task: filter.task.clone(),
            levels: if filter.levels.is_empty() {
                None
            } else {
                Some(filter.levels.join(","))
            },
            since_ms: filter.since_ms,
            until_ms: filter.until_ms,
        }
    }

    fn flow(flow: &str) -> Self {
        Self {
            flow: Some(flow.to_string()),
            ..Self::default()
        }
    }

    fn into_filter(self) -> (LogFilter, Option<usize>) {
        let filter = LogFilter {
            flow: self.flow,
            task: self.task,
            levels: match self.levels {
                Some(levels) => LogFilter::parse_levels(&levels),
                None => Vec::new(),
            },
            since_ms: self.since_ms,
            until_ms: self.until_ms,
        };
        (filter, self.limit)
    }
}

/// Counts the flows running on this pod.
#[async_trait]
pub trait RunningFlows: Send + Sync {
    /// Flows without leader election plus those whose lease this pod holds.
    async fn count(&self) -> usize;
}

/// A peer's answer to a ping.
#[derive(Debug, Serialize, Deserialize)]
struct Ping {
    flows: usize,
}

#[derive(Clone)]
struct ServerState {
    logs: Arc<dyn LogsStore>,
    metrics: Arc<dyn MetricsStore>,
    token: Arc<ClusterToken>,
    flows: Arc<dyn RunningFlows>,
}

/// Builds the token-protected internal endpoint.
pub fn router(
    logs: Arc<dyn LogsStore>,
    metrics: Arc<dyn MetricsStore>,
    token: Arc<ClusterToken>,
    flows: Arc<dyn RunningFlows>,
) -> Router {
    let state = ServerState {
        logs,
        metrics,
        token,
        flows,
    };
    Router::new()
        .route(PING_PATH, get(serve_ping))
        .route(LOGS_PATH, get(serve_logs))
        .route(LOGS_STREAM_PATH, get(serve_logs_stream))
        .route(METRICS_PATH, get(serve_metrics))
        .route(METRICS_STREAM_PATH, get(serve_metrics_stream))
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        .with_state(state)
}

/// Binds the internal endpoint on `0.0.0.0:port`.
pub async fn bind(port: u16) -> Result<tokio::net::TcpListener, Error> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|source| Error::BindListener { port, source })?;
    info!(port, "Bound cluster server");
    Ok(listener)
}

/// Serves [`router`] on `listener` until the server fails.
pub async fn serve(
    listener: tokio::net::TcpListener,
    logs: Arc<dyn LogsStore>,
    metrics: Arc<dyn MetricsStore>,
    token: Arc<ClusterToken>,
    flows: Arc<dyn RunningFlows>,
) -> Result<(), Error> {
    axum::serve(listener, router(logs, metrics, token, flows))
        .await
        .map_err(|source| Error::Serve { source })
}

async fn require_token(State(state): State<ServerState>, request: Request, next: Next) -> Response {
    let provided = match request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        Some(token) => token.to_string(),
        None => return StatusCode::UNAUTHORIZED.into_response(),
    };
    match state.token.current().await {
        Ok(expected) if token_matches(expected.expose_secret(), &provided) => {
            return next.run(request).await
        }
        Ok(_) => {}
        Err(e) => return token_unavailable(&e),
    }
    match state.token.recheck().await {
        Ok(expected) if token_matches(expected.expose_secret(), &provided) => {
            next.run(request).await
        }
        Ok(_) => StatusCode::UNAUTHORIZED.into_response(),
        Err(e) => token_unavailable(&e),
    }
}

fn token_unavailable(error: &Error) -> Response {
    warn!(error = %error, "Failed to read the cluster token for a peer request");
    StatusCode::SERVICE_UNAVAILABLE.into_response()
}

fn token_matches(expected: &str, provided: &str) -> bool {
    expected.as_bytes().ct_eq(provided.as_bytes()).into()
}

async fn serve_ping(State(state): State<ServerState>) -> Json<Ping> {
    Json(Ping {
        flows: state.flows.count().await,
    })
}

async fn serve_logs(
    State(state): State<ServerState>,
    Query(params): Query<FilterParams>,
) -> Response {
    let (filter, limit) = params.into_filter();
    let limit = match limit {
        Some(n) => n.min(MAX_QUERY_LIMIT),
        None => MAX_QUERY_LIMIT,
    };
    match state.logs.query(filter, limit).await {
        Ok(records) => Json(records).into_response(),
        Err(e) => {
            warn!(error = %e, "Failed to read local logs for a peer");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn serve_logs_stream(
    State(state): State<ServerState>,
    Query(params): Query<FilterParams>,
) -> Response {
    let (filter, _) = params.into_filter();
    match state.logs.tail(filter).await {
        Ok(records) => ndjson(records),
        Err(e) => {
            warn!(error = %e, "Failed to tail local logs for a peer");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn serve_metrics(
    State(state): State<ServerState>,
    Query(params): Query<FilterParams>,
) -> Response {
    let snapshots = match params.flow {
        Some(flow) => match state.metrics.snapshot(&flow).await {
            Ok(Some(snapshot)) => Ok(vec![snapshot]),
            Ok(None) => Ok(Vec::new()),
            Err(e) => Err(e),
        },
        None => state.metrics.snapshot_all().await,
    };
    match snapshots {
        Ok(snapshots) => Json(snapshots).into_response(),
        Err(e) => {
            warn!(error = %e, "Failed to read local metrics for a peer");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Subscribes before the snapshot so no update falls between the two.
async fn serve_metrics_stream(State(state): State<ServerState>) -> Response {
    let updates = match state.metrics.watch_all().await {
        Ok(updates) => updates,
        Err(e) => {
            warn!(error = %e, "Failed to watch local metrics for a peer");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let current = match state.metrics.snapshot_all().await {
        Ok(current) => current,
        Err(e) => {
            warn!(error = %e, "Failed to read local metrics for a peer");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    ndjson(futures_util::stream::iter(current).chain(coalesce(updates)))
}

fn coalesce(
    mut updates: BoxStream<'static, FlowMetricsSnapshot>,
) -> ReceiverStream<FlowMetricsSnapshot> {
    let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut pending: HashMap<String, FlowMetricsSnapshot> = HashMap::new();
        let mut flush = tokio::time::interval_at(
            tokio::time::Instant::now() + METRICS_FLUSH_INTERVAL,
            METRICS_FLUSH_INTERVAL,
        );
        loop {
            tokio::select! {
                _ = tx.closed() => return,
                update = updates.next() => match update {
                    Some(snapshot) => {
                        pending.insert(snapshot.flow.clone(), snapshot);
                    }
                    None => return,
                },
                _ = flush.tick() => {
                    for (_, snapshot) in pending.drain() {
                        if tx.send(snapshot).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    });
    ReceiverStream::new(rx)
}

fn ndjson<T, S>(items: S) -> Response
where
    T: Serialize + Send + 'static,
    S: Stream<Item = T> + Send + 'static,
{
    let lines = items.filter_map(|item| async move {
        match serde_json::to_vec(&item) {
            Ok(mut line) => {
                line.push(b'\n');
                Some(line)
            }
            Err(e) => {
                warn!(error = %e, "Failed to encode an item for a peer");
                None
            }
        }
    });
    let heartbeats = IntervalStream::new(tokio::time::interval_at(
        tokio::time::Instant::now() + STREAM_HEARTBEAT_INTERVAL,
        STREAM_HEARTBEAT_INTERVAL,
    ))
    .map(|_| b"\n".to_vec());
    let body = futures_util::stream::select(lines, heartbeats).map(Ok::<_, Infallible>);
    axum::body::Body::from_stream(body).into_response()
}

/// Client for the internal endpoint of every peer.
#[derive(Debug)]
pub struct ClusterPeers {
    peers: Arc<PeerRegistry>,
    http: reqwest::Client,
    token: Arc<ClusterToken>,
}

/// One registered pod, as seen from the pod that ran the check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodStatus {
    /// Pod identity.
    pub identity: String,
    /// `host:port` the pod advertises, if any.
    pub address: Option<String>,
    /// Whether the pod answered.
    pub reachability: Reachability,
}

/// Outcome of pinging a pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reachability {
    /// The pod answered with the number of flows it runs.
    Reachable { flows: usize },
    /// The pod's logs and counters are missing from merged views.
    Unreachable { reason: String },
}

/// Reason reported for a registered pod that advertises no address.
pub const NO_ADDRESS_REASON: &str =
    "Advertises no address: another telemetry backend, or POD_IP not set";

/// A failed request to a peer.
#[derive(thiserror::Error, Debug)]
enum PeerError {
    #[error(transparent)]
    Token(#[from] Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

/// One item from a peer stream, or notice that the peer left the registry.
enum PeerUpdate<T> {
    Item { peer: String, item: T },
    Gone { peer: String },
}

impl ClusterPeers {
    /// Builds a client that reaches `peers` with `token`.
    pub fn new(peers: Arc<PeerRegistry>, token: Arc<ClusterToken>) -> Result<Self, Error> {
        let http = reqwest::Client::builder()
            .connect_timeout(PEER_CONNECT_TIMEOUT)
            .read_timeout(PEER_READ_TIMEOUT)
            .build()
            .map_err(|source| Error::Client { source })?;
        Ok(Self { peers, http, token })
    }

    /// Replaces the cluster token; see [`ClusterToken::regenerate`].
    pub async fn regenerate_token(&self) -> Result<(), Error> {
        self.token.regenerate().await
    }

    /// Pings every registered peer, sorted by identity; this pod runs `this_pod_flows` flows.
    pub async fn status(&self, this_pod_flows: usize) -> Result<Vec<PodStatus>, CacheError> {
        let peers = self.peers.list_registered_peers().await?;
        let this_pod = self.peers.identity();
        let pods = futures_util::future::join_all(peers.into_iter().map(|peer| async move {
            let reachability = if peer.identity == this_pod {
                Reachability::Reachable {
                    flows: this_pod_flows,
                }
            } else {
                self.ping(&peer).await
            };
            PodStatus {
                identity: peer.identity,
                address: peer.address,
                reachability,
            }
        }))
        .await;
        Ok(pods)
    }

    async fn ping(&self, peer: &RegisteredPeer) -> Reachability {
        let Some(address) = &peer.address else {
            return Reachability::Unreachable {
                reason: NO_ADDRESS_REASON.to_string(),
            };
        };
        let answer = match self
            .send(
                address,
                PING_PATH,
                &FilterParams::default(),
                Some(PEER_QUERY_TIMEOUT),
            )
            .await
        {
            Ok(response) => response.json::<Ping>().await.map_err(PeerError::Http),
            Err(e) => Err(e),
        };
        match answer {
            Ok(ping) => Reachability::Reachable { flows: ping.flows },
            Err(e) => Reachability::Unreachable {
                reason: describe(&e),
            },
        }
    }

    async fn list(&self) -> Vec<Peer> {
        match self.peers.list_peer_addresses().await {
            Ok(peers) => peers,
            Err(e) => {
                warn!(error = %e, "Failed to list peers, showing this pod's data only");
                Vec::new()
            }
        }
    }

    async fn gather<T: DeserializeOwned>(&self, path: &str, params: &FilterParams) -> Vec<T> {
        let peers = self.list().await;
        let answers = futures_util::future::join_all(peers.iter().map(|peer| async move {
            let response = self
                .send(&peer.address, path, params, Some(PEER_QUERY_TIMEOUT))
                .await?;
            Ok::<Vec<T>, PeerError>(response.json().await?)
        }))
        .await;
        let mut items = Vec::new();
        for (peer, answer) in peers.iter().zip(answers) {
            match answer {
                Ok(peer_items) => items.extend(peer_items),
                Err(e) => warn!(
                    peer = %peer.identity,
                    error = %e,
                    "Failed to read from peer, leaving it out"
                ),
            }
        }
        items
    }

    /// Retries once when the peer rejects a token that has since changed.
    async fn send(
        &self,
        address: &str,
        path: &str,
        params: &FilterParams,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response, PeerError> {
        let token = self.token.current().await?;
        let response = self.request(address, path, params, timeout, &token).await?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(response.error_for_status()?);
        }
        let fresh = self.token.recheck().await?;
        if fresh.expose_secret() == token.expose_secret() {
            return Ok(response.error_for_status()?);
        }
        let retried = self.request(address, path, params, timeout, &fresh).await?;
        Ok(retried.error_for_status()?)
    }

    async fn request(
        &self,
        address: &str,
        path: &str,
        params: &FilterParams,
        timeout: Option<Duration>,
        token: &SecretString,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let request = self
            .http
            .get(format!("http://{address}{path}"))
            .bearer_auth(token.expose_secret())
            .query(params);
        let request = match timeout {
            Some(timeout) => request.timeout(timeout),
            None => request,
        };
        request.send().await
    }

    async fn forward<T: DeserializeOwned>(
        peer: &Peer,
        response: reqwest::Response,
        tx: &mpsc::Sender<PeerUpdate<T>>,
    ) -> Result<(), PeerError> {
        let mut body = response.bytes_stream();
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(chunk) = body.next().await {
            buffer.extend_from_slice(&chunk?);
            while let Some(newline) = buffer.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buffer.drain(..=newline).collect();
                let json = &line[..newline];
                if json.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<T>(json) {
                    Ok(item) => {
                        let update = PeerUpdate::Item {
                            peer: peer.identity.clone(),
                            item,
                        };
                        if tx.send(update).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => warn!(
                        peer = %peer.identity,
                        error = %e,
                        "Failed to decode peer stream item"
                    ),
                }
            }
        }
        Ok(())
    }

    fn follow<T>(
        self: &Arc<Self>,
        path: &'static str,
        params: FilterParams,
    ) -> mpsc::Receiver<PeerUpdate<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        tokio::spawn(follow_peers(Arc::clone(self), path, params, tx));
        rx
    }
}

fn describe(error: &PeerError) -> String {
    match error {
        PeerError::Token(e) => format!("This pod cannot read the cluster token: {e}"),
        PeerError::Http(e) => match e.status().map(|status| status.as_u16()) {
            Some(401) => "Rejected the cluster token".to_string(),
            Some(503) => "Cannot read the cluster token".to_string(),
            Some(status) => format!("Answered HTTP {status}"),
            None if e.is_timeout() => "Timed out".to_string(),
            None if e.is_connect() => "Connection failed".to_string(),
            None => "Request failed".to_string(),
        },
    }
}

async fn follow_peers<T>(
    client: Arc<ClusterPeers>,
    path: &'static str,
    params: FilterParams,
    tx: mpsc::Sender<PeerUpdate<T>>,
) where
    T: DeserializeOwned + Send + 'static,
{
    let mut followers: HashMap<String, (String, AbortHandle)> = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut refresh = tokio::time::interval(PEER_REFRESH_INTERVAL);
    loop {
        tokio::select! {
            _ = tx.closed() => return,
            _ = refresh.tick() => {
                let peers = client.list().await;
                let mut gone = Vec::new();
                followers.retain(|identity, (address, handle)| {
                    let current = peers
                        .iter()
                        .any(|p| &p.identity == identity && &p.address == address);
                    if !current {
                        handle.abort();
                        gone.push(identity.clone());
                    }
                    current
                });
                for peer in gone {
                    if tx.send(PeerUpdate::Gone { peer }).await.is_err() {
                        return;
                    }
                }
                for peer in peers {
                    if followers.contains_key(&peer.identity) {
                        continue;
                    }
                    let handle = tasks.spawn(follow_peer(
                        Arc::clone(&client),
                        peer.clone(),
                        path,
                        params.clone(),
                        tx.clone(),
                    ));
                    followers.insert(peer.identity, (peer.address, handle));
                }
            }
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
}

/// Follows one peer, reconnecting after a dropped stream, until
/// [`follow_peers`] aborts it or the subscriber goes away. A failure is
/// logged as a warning once per outage; retries until the next successful
/// connection log at debug level.
async fn follow_peer<T>(
    client: Arc<ClusterPeers>,
    peer: Peer,
    path: &'static str,
    params: FilterParams,
    tx: mpsc::Sender<PeerUpdate<T>>,
) where
    T: DeserializeOwned + Send + 'static,
{
    let mut outage_reported = false;
    let mut backoff = crate::retry::RetryConfig::default().reconnect_strategy();
    loop {
        let outcome = match client.send(&peer.address, path, &params, None).await {
            Ok(response) => {
                outage_reported = false;
                backoff = crate::retry::RetryConfig::default().reconnect_strategy();
                debug!(peer = %peer.identity, path, "Following peer stream");
                ClusterPeers::forward(&peer, response, &tx).await
            }
            Err(e) => Err(e),
        };
        match outcome {
            Ok(()) => debug!(peer = %peer.identity, path, "Peer stream ended"),
            Err(e) if !outage_reported => {
                warn!(peer = %peer.identity, path, error = %e, "Failed to follow peer, retrying");
                outage_reported = true;
            }
            Err(e) => debug!(peer = %peer.identity, path, error = %e, "Failed to follow peer"),
        }
        if tx.is_closed() {
            return;
        }
        let delay = match backoff.next() {
            Some(delay) => delay,
            None => crate::retry::DEFAULT_INITIAL_BACKOFF,
        };
        tokio::time::sleep(delay).await;
    }
}

/// [`LogsStore`] that merges this pod's records with every peer's.
///
/// A peer that does not answer in time is left out of a history query and
/// logged; the result then covers the pods that did answer.
pub struct ClusterLogsStore {
    local: Arc<dyn LogsStore>,
    peers: Arc<ClusterPeers>,
}

impl ClusterLogsStore {
    /// Wraps `local` so reads also cover every peer.
    pub fn new(local: Arc<dyn LogsStore>, peers: Arc<ClusterPeers>) -> Self {
        Self { local, peers }
    }
}

#[async_trait]
impl LogsStore for ClusterLogsStore {
    async fn query(
        &self,
        filter: LogFilter,
        limit: usize,
    ) -> Result<Vec<StoredLog>, LogsStoreError> {
        let params = FilterParams::new(&filter, Some(limit));
        let (local, remote) = tokio::join!(
            self.local.query(filter, limit),
            self.peers.gather::<StoredLog>(LOGS_PATH, &params)
        );
        let mut records = local?;
        records.extend(remote);
        Ok(newest(records, limit))
    }

    async fn tail(
        &self,
        filter: LogFilter,
    ) -> Result<BoxStream<'static, StoredLog>, LogsStoreError> {
        let params = FilterParams::new(&filter, None);
        let local = self.local.tail(filter).await?;
        let remote = ReceiverStream::new(self.peers.follow::<StoredLog>(LOGS_STREAM_PATH, params))
            .filter_map(|update| async move {
                match update {
                    PeerUpdate::Item { item, .. } => Some(item),
                    PeerUpdate::Gone { .. } => None,
                }
            });
        Ok(futures_util::stream::select(local, remote).boxed())
    }
}

/// Orders `records` by timestamp and keeps the newest `limit`, oldest first.
fn newest(mut records: Vec<StoredLog>, limit: usize) -> Vec<StoredLog> {
    records.sort_by_cached_key(timestamp_ms);
    let start = records.len().saturating_sub(limit);
    records.split_off(start)
}

/// Record timestamp in epoch milliseconds; records without one sort first.
fn timestamp_ms(record: &StoredLog) -> i64 {
    match record
        .timestamp
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
    {
        Some(Ok(dt)) => dt.timestamp_millis(),
        Some(Err(_)) | None => i64::MIN,
    }
}

/// [`MetricsStore`] that sums each flow's counters across this pod and
/// every peer. Recording stays local: each pod counts its own events.
#[derive(Debug)]
pub struct ClusterMetricsStore {
    local: Arc<dyn MetricsStore>,
    peers: Arc<ClusterPeers>,
}

impl ClusterMetricsStore {
    /// Wraps `local` so reads also cover every peer.
    pub fn new(local: Arc<dyn MetricsStore>, peers: Arc<ClusterPeers>) -> Self {
        Self { local, peers }
    }
}

/// The pod a flow snapshot was counted on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Source {
    /// This pod.
    Local,
    /// A peer, by identity.
    Peer(String),
}

#[async_trait]
impl MetricsStore for ClusterMetricsStore {
    fn record(&self, flow: &str, event: RecordedEvent) {
        self.local.record(flow, event);
    }

    async fn snapshot_all(&self) -> Result<Vec<FlowMetricsSnapshot>, activity::Error> {
        let params = FilterParams::default();
        let (local, remote) = tokio::join!(
            self.local.snapshot_all(),
            self.peers
                .gather::<FlowMetricsSnapshot>(METRICS_PATH, &params)
        );
        Ok(activity::merge_by_flow(local?.into_iter().chain(remote)))
    }

    async fn snapshot(&self, flow: &str) -> Result<Option<FlowMetricsSnapshot>, activity::Error> {
        let params = FilterParams::flow(flow);
        let (local, remote) = tokio::join!(
            self.local.snapshot(flow),
            self.peers
                .gather::<FlowMetricsSnapshot>(METRICS_PATH, &params)
        );
        Ok(activity::merge_by_flow(local?.into_iter().chain(remote))
            .into_iter()
            .next())
    }

    /// Emits a flow's cluster-wide snapshot whenever any pod updates it,
    /// and for every flow a departed peer had counted.
    async fn watch_all(&self) -> Result<BoxStream<'static, FlowMetricsSnapshot>, activity::Error> {
        let mut local = self.local.watch_all().await?;
        let mut by_source: HashMap<Source, HashMap<String, FlowMetricsSnapshot>> = HashMap::new();
        by_source.insert(
            Source::Local,
            self.local
                .snapshot_all()
                .await?
                .into_iter()
                .map(|s| (s.flow.clone(), s))
                .collect(),
        );
        let mut remote = self
            .peers
            .follow::<FlowMetricsSnapshot>(METRICS_STREAM_PATH, FilterParams::default());
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            loop {
                let changed: Vec<String> = tokio::select! {
                    _ = tx.closed() => return,
                    update = local.next() => match update {
                        Some(snapshot) => {
                            let flow = snapshot.flow.clone();
                            by_source.entry(Source::Local).or_default().insert(flow.clone(), snapshot);
                            vec![flow]
                        }
                        None => return,
                    },
                    update = remote.recv() => match update {
                        Some(PeerUpdate::Item { peer, item }) => {
                            let flow = item.flow.clone();
                            by_source.entry(Source::Peer(peer)).or_default().insert(flow.clone(), item);
                            vec![flow]
                        }
                        Some(PeerUpdate::Gone { peer }) => match by_source.remove(&Source::Peer(peer)) {
                            Some(flows) => flows.into_keys().collect(),
                            None => Vec::new(),
                        },
                        None => return,
                    },
                };
                for flow in changed {
                    let merged = activity::merge_by_flow(
                        by_source
                            .values()
                            .filter_map(|flows| flows.get(&flow))
                            .cloned(),
                    );
                    let snapshot = match merged.into_iter().next() {
                        Some(snapshot) => snapshot,
                        None => FlowMetricsSnapshot::empty(&flow),
                    };
                    if tx.send(snapshot).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(ReceiverStream::new(rx).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_cache() -> Arc<dyn Cache> {
        Arc::new(crate::cache::memory::MemoryCache::new())
    }

    #[tokio::test]
    async fn token_is_generated_once_and_stored() {
        let cache = memory_cache();
        let token = ClusterToken::new(Arc::clone(&cache));

        let first = token.current().await.unwrap();
        let second = token.current().await.unwrap();
        let stored = cache.get(TOKEN_KEY).await.unwrap().unwrap();

        assert_eq!(first.expose_secret(), second.expose_secret());
        assert_eq!(stored, first.expose_secret().as_bytes());
        assert_eq!(first.expose_secret().len(), 43);
    }

    #[tokio::test]
    async fn regenerate_replaces_stored_token() {
        let cache = memory_cache();
        let token = ClusterToken::new(Arc::clone(&cache));
        let before = token.current().await.unwrap();

        token.regenerate().await.unwrap();
        let after = token.current().await.unwrap();
        let stored = cache.get(TOKEN_KEY).await.unwrap().unwrap();

        assert_ne!(before.expose_secret(), after.expose_secret());
        assert_eq!(stored, after.expose_secret().as_bytes());
    }

    #[tokio::test(start_paused = true)]
    async fn kept_token_follows_regeneration_after_recheck_interval() {
        let cache = memory_cache();
        let this_pod = ClusterToken::new(Arc::clone(&cache));
        let other_pod = ClusterToken::new(Arc::clone(&cache));
        let original = this_pod.current().await.unwrap();

        other_pod.regenerate().await.unwrap();
        let before_refresh = this_pod.current().await.unwrap();
        let rechecked_early = this_pod.recheck().await.unwrap();
        tokio::time::advance(TOKEN_RECHECK_INTERVAL).await;
        let rechecked = this_pod.recheck().await.unwrap();

        assert_eq!(before_refresh.expose_secret(), original.expose_secret());
        assert_eq!(rechecked_early.expose_secret(), original.expose_secret());
        assert_ne!(rechecked.expose_secret(), original.expose_secret());
    }

    #[tokio::test]
    async fn deleted_token_is_generated_again() {
        let cache = memory_cache();
        let original = ClusterToken::new(Arc::clone(&cache))
            .current()
            .await
            .unwrap();
        cache.delete(TOKEN_KEY).await.unwrap();

        let fresh = ClusterToken::new(Arc::clone(&cache))
            .current()
            .await
            .unwrap();

        assert_ne!(fresh.expose_secret(), original.expose_secret());
    }

    #[test]
    fn token_matches_only_identical_tokens() {
        assert!(token_matches("secret", "secret"));
        assert!(!token_matches("secret", "secreT"));
        assert!(!token_matches("secret", "secret2"));
        assert!(!token_matches("secret", ""));
    }

    #[tokio::test(start_paused = true)]
    async fn ndjson_sends_heartbeat_while_idle() {
        let response = ndjson(futures_util::stream::pending::<FlowMetricsSnapshot>());
        let mut body = response.into_body().into_data_stream();

        let first = body.next().await.unwrap().unwrap();

        assert_eq!(&first[..], b"\n");
    }

    #[tokio::test(start_paused = true)]
    async fn coalesce_sends_latest_snapshot_per_flow() {
        let snapshot = |flow: &str, events_total: u64| FlowMetricsSnapshot {
            events_total,
            ..FlowMetricsSnapshot::empty(flow)
        };
        let updates = futures_util::stream::iter(vec![
            snapshot("orders", 1),
            snapshot("orders", 2),
            snapshot("payments", 1),
            snapshot("orders", 3),
        ])
        .chain(futures_util::stream::pending())
        .boxed();

        let mut frames: Vec<FlowMetricsSnapshot> = coalesce(updates).take(2).collect().await;
        frames.sort_by(|a, b| a.flow.cmp(&b.flow));

        assert_eq!(frames, vec![snapshot("orders", 3), snapshot("payments", 1)]);
    }

    #[test]
    fn newest_orders_by_timestamp_and_keeps_limit() {
        let record = |body: &str, timestamp: Option<&str>| StoredLog {
            body: body.to_string(),
            level: "info".to_string(),
            timestamp: timestamp.map(str::to_string),
            target: String::new(),
            spans: Vec::new(),
            fields: Vec::new(),
        };
        let records = vec![
            record("late", Some("2026-09-23T10:00:03Z")),
            record("untimed", None),
            record("early", Some("2026-09-23T10:00:01Z")),
            record("middle", Some("2026-09-23T10:00:02.500+02:00")),
        ];

        let kept: Vec<String> = newest(records, 3).into_iter().map(|r| r.body).collect();

        assert_eq!(kept, vec!["middle", "early", "late"]);
    }
}
