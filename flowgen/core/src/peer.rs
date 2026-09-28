//! Peer discovery and flow distribution via rendezvous hashing.
//!
//! Each worker pod registers itself in the shared cache under `peers.{identity}`
//! with the time of its last renewal; a registration not renewed within the TTL
//! is ignored. Before every lease attempt, the task manager hashes the flow with
//! each live peer to find the preferred owner. Other pods wait out the
//! deferral window first, so the preferred pod takes a free lease unless it is
//! not trying.

use crate::identity::FlowIdentity;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Prefix for all peer registration keys in the cache.
const PEER_KEY_PREFIX: &str = "peers.";

/// Age after which a peer registration is ignored (30 seconds).
/// Must be longer than the renewal interval to survive one missed renewal.
const DEFAULT_PEER_TTL_SECS: u64 = 30;

/// Default renewal interval for peer heartbeats (10 seconds).
const DEFAULT_PEER_RENEWAL_SECS: u64 = 10;

/// Percentage of this pod's leases handed over per rebalance round, at least one.
const REBALANCE_LIMIT_PERCENT: usize = 10;

/// How long a non-preferred pod waits before falling back to normal lease
/// acquisition (seconds). Gives the preferred pod time to win the race.
const DEFAULT_DEFERRAL_SECS: u64 = 5;

/// Value stored under `peers.{identity}`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PeerRecord {
    /// `host:port` of the pod's internal endpoint, when it serves one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    /// Unix milliseconds of the last renewal.
    #[serde(default)]
    renewed_at_ms: Option<i64>,
    /// Whether the pod has started its flows.
    #[serde(default)]
    ready: bool,
}

/// A registered peer that advertises an internal endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    /// Pod identity (`peers.{identity}` key suffix).
    pub identity: String,
    /// `host:port` of the peer's internal endpoint.
    pub address: String,
}

/// A registered peer, with the address it advertises if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredPeer {
    /// Pod identity (`peers.{identity}` key suffix).
    pub identity: String,
    /// `host:port` of the peer's internal endpoint, if it advertises one.
    pub address: Option<String>,
    /// Whether the peer has started its flows.
    pub ready: bool,
}

/// Peer registry for automatic pod discovery and flow distribution.
#[derive(Clone)]
pub struct PeerRegistry {
    cache: Arc<dyn crate::cache::Cache>,
    identity: String,
    address: Option<String>,
    ttl_secs: u64,
    renewal_secs: u64,
    deferral_secs: u64,
    ready: Arc<std::sync::atomic::AtomicBool>,
    informer: Arc<std::sync::RwLock<Option<Records>>>,
    rebalance: Arc<std::sync::Mutex<Rebalance>>,
}

type Records = std::collections::HashMap<String, PeerRecord>;

#[derive(Debug, Default)]
struct Rebalance {
    membership: Vec<String>,
    membership_since: Option<tokio::time::Instant>,
    leading: std::collections::HashSet<String>,
    round_started_at: Option<tokio::time::Instant>,
    handed_over_in_round: usize,
    handed_over: std::collections::HashSet<String>,
}

#[derive(thiserror::Error, Debug)]
enum InformerError {
    #[error("Failed to watch peer registrations: {source}")]
    Watch {
        #[source]
        source: crate::cache::Error,
    },
    #[error("Failed to read peer registrations: {source}")]
    Read {
        #[source]
        source: crate::cache::Error,
    },
    #[error("Peer registration watch ended")]
    Ended,
}

impl std::fmt::Debug for PeerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerRegistry")
            .field("identity", &self.identity)
            .field("address", &self.address)
            .field("ttl_secs", &self.ttl_secs)
            .field("renewal_secs", &self.renewal_secs)
            .finish()
    }
}

impl PeerRegistry {
    /// Creates a new peer registry.
    ///
    /// `identity` should match the executor's `holder_identity` (typically
    /// `$POD_NAME` or `$HOSTNAME`).
    pub fn new(cache: Arc<dyn crate::cache::Cache>, identity: String) -> Self {
        Self {
            cache,
            identity,
            address: None,
            ttl_secs: DEFAULT_PEER_TTL_SECS,
            renewal_secs: DEFAULT_PEER_RENEWAL_SECS,
            deferral_secs: DEFAULT_DEFERRAL_SECS,
            ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            informer: Arc::new(std::sync::RwLock::new(None)),
            rebalance: Arc::new(std::sync::Mutex::new(Rebalance::default())),
        }
    }

    /// Returns this pod's identity.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Sets the `host:port` peers use to reach this pod.
    pub fn with_address(mut self, address: Option<String>) -> Self {
        self.address = address;
        self
    }

    /// Overrides the deferral window. Used by tests so the deferral-vs-race
    /// behavior can be exercised without waiting out the production 5s
    /// default.
    #[cfg(test)]
    pub(crate) fn with_deferral_secs(mut self, deferral_secs: u64) -> Self {
        self.deferral_secs = deferral_secs;
        self
    }

    /// Returns the deferral duration non-preferred pods should wait before
    /// falling back to normal lease acquisition.
    pub fn deferral_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.deferral_secs)
    }

    /// Registers this pod in the cache. Call once at startup.
    pub async fn register(&self) -> Result<(), crate::cache::Error> {
        self.renew_internal().await?;
        info!(identity = %self.identity, "Registered peer");
        Ok(())
    }

    /// Marks this pod as running its flows, so other pods hand leases over to it.
    pub async fn mark_ready(&self) -> Result<(), crate::cache::Error> {
        self.ready.store(true, std::sync::atomic::Ordering::Relaxed);
        self.renew_internal().await
    }

    /// Renews this pod's registration in the cache without logging at INFO
    /// level, so background heartbeats don't spam the logs.
    async fn renew(&self) -> Result<(), crate::cache::Error> {
        self.renew_internal().await?;
        debug!(identity = %self.identity, "Renewed peer registration");
        Ok(())
    }

    async fn renew_internal(&self) -> Result<(), crate::cache::Error> {
        let key = format!("{PEER_KEY_PREFIX}{}", self.identity);
        let record = PeerRecord {
            address: self.address.clone(),
            renewed_at_ms: Some(chrono::Utc::now().timestamp_millis()),
            ready: self.ready.load(std::sync::atomic::Ordering::Relaxed),
        };
        let value = Bytes::from(
            serde_json::to_vec(&record)
                .map_err(|e| crate::cache::CacheError::PutFailed(Box::new(e)))?,
        );
        self.cache
            .put(&key, value.clone(), Some(self.ttl_secs))
            .await?;
        self.inform(&key, Some(&value));
        Ok(())
    }

    /// Spawns a background task that renews the peer registration at the
    /// configured interval. Stops when the cancellation token is cancelled.
    pub fn spawn_renewal(&self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        let registry = self.clone();
        tokio::spawn(async move {
            let renewal_duration = std::time::Duration::from_secs(registry.renewal_secs);
            let start = tokio::time::Instant::now() + renewal_duration;
            let mut interval = tokio::time::interval_at(start, renewal_duration);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!(identity = %registry.identity, "Peer renewal cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = registry.renew().await {
                            warn!(error = %e, "Failed to renew peer registration");
                        }
                    }
                }
            }
        })
    }

    /// Deregisters this pod from the cache. Call on graceful shutdown.
    pub async fn deregister(&self) -> Result<(), crate::cache::Error> {
        let key = format!("{PEER_KEY_PREFIX}{}", self.identity);
        self.cache.delete(&key).await?;
        self.inform(&key, None);
        info!(identity = %self.identity, "Deregistered peer");
        Ok(())
    }

    /// Keeps registrations in memory from a cache watch; reads go to the cache
    /// until it syncs, or always when the cache cannot watch.
    pub fn spawn_informer(&self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut backoff = crate::retry::RetryConfig::default().reconnect_strategy();
            loop {
                let outcome = tokio::select! {
                    _ = cancel.cancelled() => break,
                    outcome = registry.follow_registrations(&mut backoff) => outcome,
                };
                registry.set_informed(None);
                let Err(e) = outcome;
                if let InformerError::Watch {
                    source: crate::cache::Error::WatchNotSupported,
                } = e
                {
                    debug!("Cache cannot watch, reading peer registrations from it directly");
                    break;
                }
                warn!(error = %e, "Peer informer stopped, reading registrations from the cache until it resumes");
                let delay = match backoff.next() {
                    Some(delay) => delay,
                    None => crate::retry::DEFAULT_INITIAL_BACKOFF,
                };
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        })
    }

    /// Whether reads are served by the informer instead of the cache.
    pub fn informer_synced(&self) -> bool {
        match self.informer.read() {
            Ok(informed) => informed.is_some(),
            Err(poisoned) => poisoned.into_inner().is_some(),
        }
    }

    async fn follow_registrations(
        &self,
        backoff: &mut Box<dyn Iterator<Item = std::time::Duration> + Send>,
    ) -> Result<std::convert::Infallible, InformerError> {
        let mut events = self
            .cache
            .watch(PEER_KEY_PREFIX.trim_end_matches('.'), false)
            .await
            .map_err(|source| InformerError::Watch { source })?;
        let records = self
            .read_records()
            .await
            .map_err(|source| InformerError::Read { source })?;
        self.set_informed(Some(records));
        *backoff = crate::retry::RetryConfig::default().reconnect_strategy();
        debug!(identity = %self.identity, "Peer informer synced");
        while let Some(event) = events.next().await {
            match event.map_err(|source| InformerError::Watch { source })? {
                crate::cache::WatchEvent::Put { key, value } => self.inform(&key, Some(&value)),
                crate::cache::WatchEvent::Delete { key } => self.inform(&key, None),
            }
        }
        Err(InformerError::Ended)
    }

    fn set_informed(&self, records: Option<Records>) {
        match self.informer.write() {
            Ok(mut informed) => *informed = records,
            Err(poisoned) => *poisoned.into_inner() = records,
        }
    }

    fn inform(&self, key: &str, value: Option<&Bytes>) {
        let Some(identity) = key.strip_prefix(PEER_KEY_PREFIX) else {
            return;
        };
        let mut informed = match self.informer.write() {
            Ok(informed) => informed,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(records) = informed.as_mut() else {
            return;
        };
        match value.map(|value| serde_json::from_slice::<PeerRecord>(value)) {
            Some(Ok(record)) => {
                let newer = records
                    .get(identity)
                    .is_none_or(|known| record.renewed_at_ms >= known.renewed_at_ms);
                if newer {
                    records.insert(identity.to_string(), record);
                }
            }
            Some(Err(_)) | None => {
                records.remove(identity);
            }
        }
    }

    async fn read_records(&self) -> Result<Records, crate::cache::Error> {
        let mut records = Records::new();
        for key in self.cache.list_keys(PEER_KEY_PREFIX).await? {
            let Some(identity) = key.strip_prefix(PEER_KEY_PREFIX) else {
                continue;
            };
            let Some(value) = self.cache.get(&key).await? else {
                continue;
            };
            if let Ok(record) = serde_json::from_slice::<PeerRecord>(&value) {
                records.insert(identity.to_string(), record);
            }
        }
        Ok(records)
    }

    /// Returns the sorted list of currently registered peer identities.
    pub async fn list_peers(&self) -> Result<Vec<String>, crate::cache::Error> {
        Ok(self
            .read_live()
            .await?
            .into_iter()
            .map(|peer| peer.identity)
            .collect())
    }

    async fn read_live(&self) -> Result<Vec<RegisteredPeer>, crate::cache::Error> {
        let informed = match self.informer.read() {
            Ok(informed) => informed.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let records = match informed {
            Some(records) => records,
            None => self.read_records().await?,
        };
        let now_ms = chrono::Utc::now().timestamp_millis();
        let max_age_ms = self.ttl_secs as i64 * 1000;
        let mut peers: Vec<RegisteredPeer> = records
            .into_iter()
            .filter_map(|(identity, record)| match record.renewed_at_ms {
                Some(renewed_at_ms) if now_ms.saturating_sub(renewed_at_ms) <= max_age_ms => {
                    Some(RegisteredPeer {
                        identity,
                        address: record.address,
                        ready: record.ready,
                    })
                }
                _ => None,
            })
            .collect();
        peers.sort_by(|a, b| a.identity.cmp(&b.identity));
        self.observe_membership(peers.iter().map(|peer| peer.identity.as_str()));
        Ok(peers)
    }

    fn rebalance(&self) -> std::sync::MutexGuard<'_, Rebalance> {
        match self.rebalance.lock() {
            Ok(rebalance) => rebalance,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn observe_membership<'a>(&self, identities: impl Iterator<Item = &'a str>) {
        let identities: Vec<&str> = identities.collect();
        let mut rebalance = self.rebalance();
        if rebalance.membership_since.is_none() || rebalance.membership != identities {
            rebalance.membership = identities.into_iter().map(str::to_string).collect();
            rebalance.membership_since = Some(tokio::time::Instant::now());
            rebalance.handed_over.clear();
        }
    }

    /// Records whether this pod holds the lease of `flow`.
    pub fn note_leading(&self, flow: &FlowIdentity, leading: bool) {
        let mut rebalance = self.rebalance();
        if leading {
            rebalance.leading.insert(flow.as_key());
        } else {
            rebalance.leading.remove(&flow.as_key());
        }
    }

    /// Whether this pod should hand the lease of `flow` over to its preferred
    /// pod: that pod has started its flows, the peer list has not changed for
    /// one peer TTL, this pod has not handed `flow` over since the last change,
    /// and this round has not reached its limit of [`REBALANCE_LIMIT_PERCENT`]
    /// of this pod's leases.
    pub async fn should_hand_over(&self, flow: &FlowIdentity) -> Result<bool, crate::cache::Error> {
        let peers = self.read_live().await?;
        let identities: Vec<String> = peers.iter().map(|peer| peer.identity.clone()).collect();
        if identities.len() < 2 {
            return Ok(false);
        }
        let Some(preferred) = preferred_peer(&flow.as_key(), &identities) else {
            return Ok(false);
        };
        let preferred_ready = peers
            .iter()
            .any(|peer| peer.identity == preferred && peer.ready);
        if preferred == self.identity || !preferred_ready {
            return Ok(false);
        }
        let now = tokio::time::Instant::now();
        let mut rebalance = self.rebalance();
        let Some(membership_since) = rebalance.membership_since else {
            return Ok(false);
        };
        let stable = now.saturating_duration_since(membership_since)
            >= std::time::Duration::from_secs(self.ttl_secs);
        let key = flow.as_key();
        if !stable || rebalance.handed_over.contains(&key) {
            return Ok(false);
        }
        let round_over = match rebalance.round_started_at {
            Some(started) => {
                now.saturating_duration_since(started)
                    >= std::time::Duration::from_secs(self.renewal_secs)
            }
            None => true,
        };
        if round_over {
            rebalance.round_started_at = Some(now);
            rebalance.handed_over_in_round = 0;
        }
        let limit = (rebalance.leading.len() * REBALANCE_LIMIT_PERCENT / 100).max(1);
        if rebalance.handed_over_in_round >= limit {
            return Ok(false);
        }
        rebalance.handed_over_in_round += 1;
        rebalance.handed_over.insert(key);
        Ok(true)
    }

    /// Returns every other registered peer that advertises an address.
    pub async fn list_peer_addresses(&self) -> Result<Vec<Peer>, crate::cache::Error> {
        Ok(self
            .list_other_peers()
            .await?
            .into_iter()
            .filter_map(
                |RegisteredPeer {
                     identity, address, ..
                 }| { address.map(|address| Peer { identity, address }) },
            )
            .collect())
    }

    /// Returns every other registered peer.
    pub async fn list_other_peers(&self) -> Result<Vec<RegisteredPeer>, crate::cache::Error> {
        Ok(self
            .read_live()
            .await?
            .into_iter()
            .filter(|peer| peer.identity != self.identity)
            .collect())
    }

    /// Returns every registered peer, this pod included, sorted by identity.
    pub async fn list_registered_peers(&self) -> Result<Vec<RegisteredPeer>, crate::cache::Error> {
        self.read_live().await
    }

    /// Returns `true` if this pod is the preferred owner for the given flow,
    /// per [`preferred_peer`]. When only one peer is registered, it always
    /// returns `true`.
    ///
    /// The hash input is the identity's key-safe form so it stays consistent
    /// with the lease key derived from the same identity.
    pub async fn is_preferred_owner(
        &self,
        flow_name: &FlowIdentity,
    ) -> Result<bool, crate::cache::Error> {
        let peers = self.list_peers().await?;
        if peers.len() <= 1 {
            return Ok(true);
        }
        Ok(preferred_peer(&flow_name.as_key(), &peers) == Some(self.identity.as_str()))
    }
}

/// Preferred peer for a flow by rendezvous hashing: the peer with the highest
/// `sha256(flow, peer)` wins, so adding a peer moves only the flows it now
/// wins and removing one moves only the flows it had. `None` without peers.
pub fn preferred_peer<'a>(flow_name: &str, peers: &'a [String]) -> Option<&'a str> {
    peers
        .iter()
        .max_by_key(|peer| (rendezvous_score(flow_name, peer), peer.as_str()))
        .map(String::as_str)
}

fn rendezvous_score(flow_name: &str, peer: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(flow_name.as_bytes());
    hasher.update([0]);
    hasher.update(peer.as_bytes());
    let hash = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash[..8]);
    u64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::memory::MemoryCache;

    fn make_registry(identity: &str) -> PeerRegistry {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        PeerRegistry::new(cache, identity.to_string())
    }

    fn make_shared_registry(cache: Arc<dyn crate::cache::Cache>, identity: &str) -> PeerRegistry {
        PeerRegistry::new(cache, identity.to_string())
    }

    #[tokio::test]
    async fn test_register_and_list() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let r1 = make_shared_registry(cache.clone(), "pod-a");
        let r2 = make_shared_registry(cache.clone(), "pod-b");
        let r3 = make_shared_registry(cache.clone(), "pod-c");

        r1.register().await.unwrap();
        r2.register().await.unwrap();
        r3.register().await.unwrap();

        let peers = r1.list_peers().await.unwrap();
        assert_eq!(peers, vec!["pod-a", "pod-b", "pod-c"]);
    }

    #[tokio::test]
    async fn test_deregister() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let r1 = make_shared_registry(cache.clone(), "pod-a");
        let r2 = make_shared_registry(cache.clone(), "pod-b");

        r1.register().await.unwrap();
        r2.register().await.unwrap();
        r1.deregister().await.unwrap();

        let peers = r2.list_peers().await.unwrap();
        assert_eq!(peers, vec!["pod-b"]);
    }

    #[tokio::test]
    async fn test_single_peer_always_preferred() {
        let r = make_registry("only-pod");
        r.register().await.unwrap();

        assert!(r
            .is_preferred_owner(&FlowIdentity::new("any-flow"))
            .await
            .unwrap());
        assert!(r
            .is_preferred_owner(&FlowIdentity::new("another-flow"))
            .await
            .unwrap());
    }

    #[test]
    fn test_preferred_peer_deterministic() {
        let peers = vec!["pod-a".into(), "pod-b".into(), "pod-c".into()];
        let p1 = preferred_peer("my-flow", &peers);
        let p2 = preferred_peer("my-flow", &peers);
        assert!(p1.is_some());
        assert_eq!(p1, p2, "same input must produce same output");
        assert_eq!(preferred_peer("my-flow", &[]), None);
    }

    #[test]
    fn test_preferred_peer_distributes() {
        let peers: Vec<String> = (0..3).map(|i| format!("pod-{i}")).collect();
        let mut counts = std::collections::HashMap::new();
        for i in 0..100 {
            let flow = format!("flow-{i}");
            let p = preferred_peer(&flow, &peers).unwrap();
            *counts.entry(p.to_string()).or_insert(0u32) += 1;
        }
        for (pod, count) in &counts {
            assert!(
                *count > 5,
                "pod {pod} only got {count} flows out of 100 — distribution too uneven"
            );
        }
    }

    #[test]
    fn adding_a_peer_moves_only_the_flows_it_now_wins() {
        let three: Vec<String> = (0..3).map(|i| format!("pod-{i}")).collect();
        let four: Vec<String> = (0..4).map(|i| format!("pod-{i}")).collect();

        let moved: Vec<(String, String)> = (0..1000)
            .map(|i| format!("flow-{i}"))
            .filter_map(|flow| {
                let before = preferred_peer(&flow, &three).unwrap();
                let after = preferred_peer(&flow, &four).unwrap();
                (before != after).then(|| (before.to_string(), after.to_string()))
            })
            .collect();

        assert!(moved.iter().all(|(_, after)| after == "pod-3"));
        assert!(
            (150..=350).contains(&moved.len()),
            "{} of 1000 flows moved, expected about a quarter",
            moved.len()
        );
    }

    #[tokio::test]
    async fn test_preferred_owner_among_multiple_peers() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let r1 = make_shared_registry(cache.clone(), "pod-a");
        let r2 = make_shared_registry(cache.clone(), "pod-b");
        let r3 = make_shared_registry(cache.clone(), "pod-c");

        r1.register().await.unwrap();
        r2.register().await.unwrap();
        r3.register().await.unwrap();

        let flow = FlowIdentity::new("test-flow");
        let owners: Vec<bool> = futures_util::future::join_all(vec![
            r1.is_preferred_owner(&flow),
            r2.is_preferred_owner(&flow),
            r3.is_preferred_owner(&flow),
        ])
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

        let preferred_count = owners.iter().filter(|&&o| o).count();
        assert_eq!(
            preferred_count, 1,
            "exactly one peer should be preferred for a flow"
        );
    }

    #[tokio::test]
    async fn test_list_peer_addresses_skips_self_addressless_and_legacy_peers() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let me = make_shared_registry(cache.clone(), "pod-a")
            .with_address(Some("10.0.0.1:8082".to_string()));
        let addressed = make_shared_registry(cache.clone(), "pod-b")
            .with_address(Some("10.0.0.2:8082".to_string()));
        let addressless = make_shared_registry(cache.clone(), "pod-c");
        me.register().await.unwrap();
        addressed.register().await.unwrap();
        addressless.register().await.unwrap();
        let legacy_identity_value = Bytes::from("pod-d");
        cache
            .put("peers.pod-d", legacy_identity_value, None)
            .await
            .unwrap();

        let peers = me.list_peer_addresses().await.unwrap();

        assert_eq!(
            peers,
            vec![Peer {
                identity: "pod-b".to_string(),
                address: "10.0.0.2:8082".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn test_list_peers_drops_registrations_older_than_ttl() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let live = make_shared_registry(cache.clone(), "pod-live");
        live.register().await.unwrap();
        let expired = PeerRecord {
            address: Some("10.0.0.9:8082".to_string()),
            renewed_at_ms: Some(
                chrono::Utc::now().timestamp_millis() - (DEFAULT_PEER_TTL_SECS as i64 + 1) * 1000,
            ),
            ..Default::default()
        };
        cache
            .put(
                "peers.pod-expired",
                Bytes::from(serde_json::to_vec(&expired).unwrap()),
                None,
            )
            .await
            .unwrap();
        cache
            .put("peers.pod-legacy", Bytes::from("pod-legacy"), None)
            .await
            .unwrap();

        let peers = live.list_peers().await.unwrap();

        assert_eq!(peers, vec!["pod-live"]);
    }

    async fn peers_become(registry: &PeerRegistry, expected: &[&str]) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while registry.list_peers().await.unwrap() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("peer list did not converge");
    }

    #[tokio::test]
    async fn informer_follows_other_pods_from_memory() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let a = make_shared_registry(cache.clone(), "pod-a");
        let b = make_shared_registry(cache.clone(), "pod-b");
        a.register().await.unwrap();
        let _informer = a.spawn_informer(CancellationToken::new());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !a.informer_synced() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("informer did not sync");

        b.register().await.unwrap();
        peers_become(&a, &["pod-a", "pod-b"]).await;
        b.deregister().await.unwrap();
        peers_become(&a, &["pod-a"]).await;
        a.deregister().await.unwrap();

        assert!(a.list_peers().await.unwrap().is_empty());
    }

    #[test]
    fn informer_keeps_the_newer_renewal() {
        let registry = make_registry("pod-a");
        registry.set_informed(Some(Records::new()));
        let record = |renewed_at_ms| {
            Bytes::from(
                serde_json::to_vec(&PeerRecord {
                    renewed_at_ms: Some(renewed_at_ms),
                    ..Default::default()
                })
                .unwrap(),
            )
        };

        registry.inform("peers.pod-b", Some(&record(2_000)));
        registry.inform("peers.pod-b", Some(&record(1_000)));

        let informed = registry.informer.read().unwrap().clone().unwrap();
        assert_eq!(informed["pod-b"].renewed_at_ms, Some(2_000));
    }

    fn flows_preferring(peer: &str, peers: &[String], count: usize) -> Vec<FlowIdentity> {
        (0..)
            .map(|n| FlowIdentity::new(format!("flow-{n}")))
            .filter(|flow| preferred_peer(&flow.as_key(), peers) == Some(peer))
            .take(count)
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn hand_over_waits_for_stable_peers_and_limits_each_round() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let a = make_shared_registry(cache.clone(), "pod-a");
        a.register().await.unwrap();
        make_shared_registry(cache.clone(), "pod-b")
            .mark_ready()
            .await
            .unwrap();
        let peers = vec!["pod-a".to_string(), "pod-b".to_string()];
        let to_b = flows_preferring("pod-b", &peers, 20);
        let kept = &flows_preferring("pod-a", &peers, 1)[0];
        for flow in to_b.iter().chain([kept]) {
            a.note_leading(flow, true);
        }
        let hand_over = |flows: &[FlowIdentity]| {
            let a = a.clone();
            let flows = flows.to_vec();
            async move {
                let mut handed = Vec::new();
                for flow in &flows {
                    handed.push(a.should_hand_over(flow).await.unwrap());
                }
                handed
            }
        };

        let before_stable = hand_over(&to_b).await;
        tokio::time::advance(std::time::Duration::from_secs(DEFAULT_PEER_TTL_SECS)).await;
        let first_round = hand_over(&to_b).await;
        let same_round = hand_over(&to_b[2..]).await;
        tokio::time::advance(std::time::Duration::from_secs(DEFAULT_PEER_RENEWAL_SECS)).await;
        let second_round = hand_over(&to_b).await;

        assert!(before_stable.iter().all(|handed| !handed));
        assert_eq!(first_round.iter().filter(|handed| **handed).count(), 2);
        assert!(first_round[..2].iter().all(|handed| *handed));
        assert!(same_round.iter().all(|handed| !handed));
        assert_eq!(second_round.iter().filter(|handed| **handed).count(), 2);
        assert!(second_round[..2].iter().all(|handed| !handed));
        assert!(!a.should_hand_over(kept).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn hand_over_waits_for_the_preferred_pod_to_start_its_flows() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let a = make_shared_registry(cache.clone(), "pod-a");
        a.register().await.unwrap();
        let b = make_shared_registry(cache.clone(), "pod-b");
        b.register().await.unwrap();
        let flow = &flows_preferring("pod-b", &["pod-a".to_string(), "pod-b".to_string()], 1)[0];
        a.note_leading(flow, true);
        a.list_peers().await.unwrap();
        tokio::time::advance(std::time::Duration::from_secs(DEFAULT_PEER_TTL_SECS)).await;

        let while_starting = a.should_hand_over(flow).await.unwrap();
        b.mark_ready().await.unwrap();
        let once_ready = a.should_hand_over(flow).await.unwrap();

        assert!(!while_starting);
        assert!(once_ready);
    }

    #[tokio::test(start_paused = true)]
    async fn hand_over_restarts_the_wait_when_peers_change() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let a = make_shared_registry(cache.clone(), "pod-a");
        a.register().await.unwrap();
        make_shared_registry(cache.clone(), "pod-b")
            .mark_ready()
            .await
            .unwrap();
        let flow = &flows_preferring(
            "pod-b",
            &[
                "pod-a".to_string(),
                "pod-b".to_string(),
                "pod-c".to_string(),
            ],
            1,
        )[0];
        a.note_leading(flow, true);
        a.list_peers().await.unwrap();
        tokio::time::advance(std::time::Duration::from_secs(DEFAULT_PEER_TTL_SECS - 1)).await;

        make_shared_registry(cache.clone(), "pod-c")
            .register()
            .await
            .unwrap();
        let after_change = a.should_hand_over(flow).await.unwrap();
        tokio::time::advance(std::time::Duration::from_secs(DEFAULT_PEER_TTL_SECS)).await;
        let once_stable = a.should_hand_over(flow).await.unwrap();

        assert!(!after_change);
        assert!(once_stable);
    }

    #[tokio::test]
    async fn test_empty_peers_returns_preferred() {
        let r = make_registry("pod-a");
        assert!(r
            .is_preferred_owner(&FlowIdentity::new("flow"))
            .await
            .unwrap());
    }
}
