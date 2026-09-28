//! Integration tests for the `cluster` telemetry backend over localhost pods.
//!
//! No external dependency, not `#[ignore]`d.

use flowgen_core::cache::memory::MemoryCache;
use flowgen_core::cache::Cache;
use flowgen_core::flow::activity::{
    ActivityLevel, FlowStatus, MetricsStore, OtlpMetricsStore, RecordedEvent,
};
use flowgen_core::peer::PeerRegistry;
use flowgen_core::telemetry::cluster::{
    router, ClusterLogsStore, ClusterMetricsStore, ClusterPeers, ClusterToken, PodStatus,
    Reachability, RunningFlows, NO_ADDRESS_REASON, TOKEN_KEY,
};
use flowgen_core::telemetry::query::{pair, LogFilter, LogsStore, MemoryLogsStoreWriter};
use flowgen_core::telemetry::StoredLog;
use futures_util::StreamExt;
use secrecy::ExposeSecret;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::fmt::MakeWriter;

struct Pod {
    writer: MemoryLogsStoreWriter,
    logs: Arc<dyn LogsStore>,
    metrics: Arc<dyn MetricsStore>,
    token: Arc<ClusterToken>,
    registry: Arc<PeerRegistry>,
    address: String,
}

impl Pod {
    fn peers(&self) -> Arc<ClusterPeers> {
        self.peers_with(Arc::clone(&self.token))
    }

    fn peers_with(&self, token: Arc<ClusterToken>) -> Arc<ClusterPeers> {
        Arc::new(ClusterPeers::new(Arc::clone(&self.registry), token).unwrap())
    }

    fn cluster_logs(&self) -> ClusterLogsStore {
        ClusterLogsStore::new(Arc::clone(&self.logs), self.peers())
    }

    fn cluster_metrics(&self) -> ClusterMetricsStore {
        ClusterMetricsStore::new(Arc::clone(&self.metrics), self.peers())
    }

    fn emit(&self, flow: &str, body: &str, timestamp: &str) {
        let line = format!(
            r#"{{"timestamp":"{timestamp}","level":"INFO","fields":{{"message":"{body}"}},"target":"t","spans":[{{"flow":"{flow}","name":"flow.run"}}]}}"#
        );
        let mut writer = self.writer.make_writer();
        writer.write_all(line.as_bytes()).unwrap();
        writer.write_all(b"\n").unwrap();
    }

    fn count(&self, flow: &str, level: ActivityLevel, ts_ms: u64) {
        self.metrics.record(
            flow,
            RecordedEvent {
                task: None,
                task_type: None,
                level,
                ts_ms,
                message: String::new(),
                duration_ms: None,
                event_id: None,
            },
        );
    }
}

struct Running(usize);

#[async_trait::async_trait]
impl RunningFlows for Running {
    async fn count(&self) -> usize {
        self.0
    }
}

fn shared_cache() -> Arc<dyn Cache> {
    Arc::new(MemoryCache::new())
}

async fn start_pod(cache: &Arc<dyn Cache>, identity: &str) -> Pod {
    start_pod_running(cache, identity, 0).await
}

async fn start_pod_running(cache: &Arc<dyn Cache>, identity: &str, flows: usize) -> Pod {
    let (writer, logs) = pair(100);
    let logs: Arc<dyn LogsStore> = Arc::new(logs);
    let metrics: Arc<dyn MetricsStore> = OtlpMetricsStore::builder().build();
    let token = Arc::new(ClusterToken::new(Arc::clone(cache)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let app = router(
        Arc::clone(&logs),
        Arc::clone(&metrics),
        Arc::clone(&token),
        Arc::new(Running(flows)),
    );
    tokio::spawn(async move { axum::serve(listener, app).await });
    let registry = Arc::new(
        PeerRegistry::new(Arc::clone(cache), identity.to_string())
            .with_address(Some(address.clone())),
    );
    registry.register().await.unwrap();
    Pod {
        writer,
        logs,
        metrics,
        token,
        registry,
        address,
    }
}

async fn register_dead_peer(cache: &Arc<dyn Cache>, identity: &str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_address = listener.local_addr().unwrap().to_string();
    drop(listener);
    PeerRegistry::new(Arc::clone(cache), identity.to_string())
        .with_address(Some(dead_address.clone()))
        .register()
        .await
        .unwrap();
    dead_address
}

fn foreign_token() -> Arc<ClusterToken> {
    Arc::new(ClusterToken::new(shared_cache()))
}

async fn query_until(store: &ClusterLogsStore, expected: usize) -> Vec<StoredLog> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let records = store.query(LogFilter::default(), 100).await.unwrap();
        if records.len() == expected || tokio::time::Instant::now() >= deadline {
            return records;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn bodies(records: &[StoredLog]) -> Vec<&str> {
    records.iter().map(|r| r.body.as_str()).collect()
}

fn orders() -> LogFilter {
    LogFilter {
        flow: Some("orders".to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn query_merges_peers_by_timestamp_and_keeps_newest() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod(&cache, "pod-b").await;
    a.emit("orders", "a1", "2026-09-23T10:00:01Z");
    b.emit("orders", "b1", "2026-09-23T10:00:02Z");
    a.emit("orders", "a2", "2026-09-23T10:00:03Z");
    b.emit("orders", "b2", "2026-09-23T10:00:04Z");
    b.emit("payments", "b3", "2026-09-23T10:00:05Z");

    let store = a.cluster_logs();
    let all = store.query(LogFilter::default(), 100).await.unwrap();
    let newest_orders = store.query(orders(), 3).await.unwrap();

    assert_eq!(bodies(&all), vec!["a1", "b1", "a2", "b2", "b3"]);
    assert_eq!(bodies(&newest_orders), vec!["b1", "a2", "b2"]);
}

#[tokio::test]
async fn query_leaves_out_unreachable_peer() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    register_dead_peer(&cache, "pod-dead").await;
    a.emit("orders", "a1", "2026-09-23T10:00:01Z");

    let records = a
        .cluster_logs()
        .query(LogFilter::default(), 100)
        .await
        .unwrap();

    assert_eq!(bodies(&records), vec!["a1"]);
}

#[tokio::test]
async fn internal_endpoint_rejects_wrong_token() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod(&cache, "pod-b").await;
    b.emit("orders", "b1", "2026-09-23T10:00:01Z");
    b.count("orders", ActivityLevel::Info, 100);

    let logs = ClusterLogsStore::new(Arc::clone(&a.logs), a.peers_with(foreign_token()));
    let metrics = ClusterMetricsStore::new(Arc::clone(&a.metrics), a.peers_with(foreign_token()));

    assert!(logs
        .query(LogFilter::default(), 100)
        .await
        .unwrap()
        .is_empty());
    assert!(metrics.snapshot_all().await.unwrap().is_empty());
}

#[tokio::test]
async fn tail_receives_peer_records() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod(&cache, "pod-b").await;
    let mut tail = a.cluster_logs().tail(orders()).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            b.emit("payments", "skip", "2026-09-23T10:00:01Z");
            b.emit("orders", "b1", "2026-09-23T10:00:02Z");
            match tokio::time::timeout(Duration::from_millis(100), tail.next()).await {
                Ok(Some(record)) => return record,
                Ok(None) => panic!("tail ended"),
                Err(_) => continue,
            }
        }
    })
    .await
    .expect("no peer record within 5s");

    assert_eq!(received.body, "b1");
}

#[tokio::test]
async fn snapshots_sum_flow_counters_across_pods() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod(&cache, "pod-b").await;
    a.count("orders", ActivityLevel::Info, 100);
    a.count("orders", ActivityLevel::Info, 200);
    b.count("orders", ActivityLevel::Error, 300);
    b.count("payments", ActivityLevel::Info, 150);

    let store = a.cluster_metrics();
    let mut all = store.snapshot_all().await.unwrap();
    all.sort_by(|x, y| x.flow.cmp(&y.flow));
    let orders = store.snapshot("orders").await.unwrap().unwrap();
    let unknown = store.snapshot("unknown").await.unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(all[0], orders);
    assert_eq!(orders.events_total, 2);
    assert_eq!(orders.errors_total, 1);
    assert_eq!(orders.last_error_at_ms, Some(300));
    assert_eq!(orders.status, FlowStatus::Error);
    assert_eq!(all[1].flow, "payments");
    assert_eq!(all[1].events_total, 1);
    assert!(unknown.is_none());
}

#[tokio::test]
async fn watch_all_emits_cluster_wide_snapshot_on_peer_update() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod(&cache, "pod-b").await;
    a.count("orders", ActivityLevel::Error, 100);
    let mut watch = a.cluster_metrics().watch_all().await.unwrap();

    let frame = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            b.count("orders", ActivityLevel::Info, 200);
            match tokio::time::timeout(Duration::from_millis(600), watch.next()).await {
                Ok(Some(frame)) => return frame,
                Ok(None) => panic!("watch ended"),
                Err(_) => continue,
            }
        }
    })
    .await
    .expect("no metrics frame within 5s");

    assert_eq!(frame.flow, "orders");
    assert_eq!(frame.errors_total, 1);
    assert!(frame.events_total >= 1);
    assert_eq!(frame.status, FlowStatus::Ok);
}

#[tokio::test]
async fn status_lists_every_pod_with_reachability_and_flow_count() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod_running(&cache, "pod-b", 3).await;
    let c_address = register_dead_peer(&cache, "pod-c").await;
    PeerRegistry::new(Arc::clone(&cache), "pod-d".to_string())
        .register()
        .await
        .unwrap();

    let status = a.peers().status(5).await.unwrap();
    let wrong_token = a.peers_with(foreign_token()).status(5).await.unwrap();

    let pod = |identity: &str, address: Option<&str>, reachability| PodStatus {
        identity: identity.to_string(),
        address: address.map(str::to_string),
        reachability,
    };
    assert_eq!(
        status,
        vec![
            pod(
                "pod-a",
                Some(&a.address),
                Reachability::Reachable { flows: 5 }
            ),
            pod(
                "pod-b",
                Some(&b.address),
                Reachability::Reachable { flows: 3 }
            ),
            pod(
                "pod-c",
                Some(&c_address),
                Reachability::Unreachable {
                    reason: "Connection failed".to_string()
                }
            ),
            pod(
                "pod-d",
                None,
                Reachability::Unreachable {
                    reason: NO_ADDRESS_REASON.to_string()
                }
            ),
        ]
    );
    assert_eq!(
        wrong_token[1].reachability,
        Reachability::Unreachable {
            reason: "Rejected the cluster token".to_string()
        }
    );
}

#[tokio::test]
async fn pods_sharing_a_cache_share_one_generated_token() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod(&cache, "pod-b").await;

    let (token_a, token_b) = tokio::join!(a.token.current(), b.token.current());
    let stored = cache.get(TOKEN_KEY).await.unwrap().unwrap();

    assert_eq!(
        token_a.unwrap().expose_secret(),
        token_b.unwrap().expose_secret()
    );
    assert!(stored.len() >= 43);
}

#[tokio::test]
async fn regenerated_token_reaches_peers_and_rejects_the_leaked_one() {
    let cache = shared_cache();
    let a = start_pod(&cache, "pod-a").await;
    let b = start_pod(&cache, "pod-b").await;
    b.emit("orders", "b1", "2026-09-23T10:00:01Z");
    let store = a.cluster_logs();
    assert_eq!(
        bodies(&store.query(LogFilter::default(), 100).await.unwrap()),
        vec!["b1"]
    );
    let leaked_cache = shared_cache();
    leaked_cache
        .put(
            TOKEN_KEY,
            bytes::Bytes::from(a.token.current().await.unwrap().expose_secret().to_string()),
            None,
        )
        .await
        .unwrap();
    let leaked = ClusterLogsStore::new(
        Arc::clone(&a.logs),
        a.peers_with(Arc::new(ClusterToken::new(leaked_cache))),
    );

    a.token.regenerate().await.unwrap();
    let after_regeneration = query_until(&store, 1).await;
    let with_leaked_token = leaked.query(LogFilter::default(), 100).await.unwrap();

    assert_eq!(bodies(&after_regeneration), vec!["b1"]);
    assert!(
        with_leaked_token.is_empty(),
        "peer accepted the leaked token"
    );
}
