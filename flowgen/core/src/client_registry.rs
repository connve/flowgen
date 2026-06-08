//! Shared connection pooling for external service clients.
//!
//! `ClientRegistry` lives at the worker level and is shared across all tasks
//! via `TaskContext`. Flows with identical credentials automatically reuse the
//! same authenticated client, avoiding redundant login/auth calls that hammer
//! external services (e.g. Salesforce OAuth rate limits).
//!
//! Each task hashes its credential fields into a `ClientKey`. The first
//! task to request a client for a given key performs the actual authentication;
//! all others wait on a per-key mutex and reuse the result.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Opaque key identifying a unique client configuration.
///
/// Built by tasks from their credential fields (credentials path, endpoint
/// URL, etc.). Two tasks with identical fields share the same client.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ClientKey(u64);

impl ClientKey {
    /// Creates a new client key from any hashable identity.
    ///
    /// The identity should include every field that affects client construction:
    /// credentials path, endpoint URL, pool size, etc. Two identical identities
    /// produce the same key and share the same client.
    pub fn new<H: Hash>(identity: &H) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut hasher);
        Self(hasher.finish())
    }
}

impl fmt::Display for ClientKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Type-erased client entry stored in the registry.
type AnyClient = Arc<dyn Any + Send + Sync>;

/// Per-key initialisation guard preventing thundering herd on first connect.
type InitGuard = Arc<Mutex<()>>;

/// Shared client registry for deduplicating connections to external services.
///
/// Thread-safe and cheaply cloneable (all clones share the same backing maps).
/// Passed through `TaskContext` so every task in the worker can participate.
#[derive(Clone, Debug)]
pub struct ClientRegistry {
    /// Fully initialised clients keyed by their configuration hash.
    clients: Arc<RwLock<HashMap<ClientKey, AnyClient>>>,
    /// Per-key mutexes that serialise the init closure so only the first caller
    /// performs the actual authentication.
    init_guards: Arc<RwLock<HashMap<ClientKey, InitGuard>>>,
}

impl ClientRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            init_guards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns a shared client for the given key, initialising it on first access.
    ///
    /// The `init` closure is called at most once per key. Concurrent callers for
    /// the same key block on the per-key mutex until the first caller finishes,
    /// then all receive the same `Arc<T>`.
    ///
    /// # Type Safety
    ///
    /// The caller must ensure that every call site for a given `ClientKey` uses
    /// the same concrete type `T`. A type mismatch will return the init error
    /// variant or the downcast error.
    pub async fn get_or_init<T, F, Fut, E>(
        &self,
        key: ClientKey,
        init: F,
    ) -> Result<Arc<T>, Error<E>>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        // Fast path: client already exists.
        {
            let clients = self.clients.read().await;
            if let Some(entry) = clients.get(&key) {
                return entry
                    .clone()
                    .downcast::<T>()
                    .map_err(|_| Error::TypeMismatch);
            }
        }

        // Slow path: acquire the per-key init guard.
        let guard = {
            let mut guards = self.init_guards.write().await;
            guards.entry(key.clone()).or_default().clone()
        };

        let _lock = guard.lock().await;

        // Double-check: another caller may have initialised while we waited.
        {
            let clients = self.clients.read().await;
            if let Some(entry) = clients.get(&key) {
                return entry
                    .clone()
                    .downcast::<T>()
                    .map_err(|_| Error::TypeMismatch);
            }
        }

        // We are the first caller — run init.
        let client = Arc::new(init().await.map_err(|e| Error::Init { source: e })?);
        let any: AnyClient = client.clone();

        {
            let mut clients = self.clients.write().await;
            clients.insert(key.clone(), any);
        }

        // Clean up the init guard only after the client is stored. Removing it
        // earlier would let a concurrent caller create a new guard and run init
        // in parallel, violating the at-most-once guarantee.
        {
            let mut guards = self.init_guards.write().await;
            guards.remove(&key);
        }

        Ok(client)
    }

    /// Removes a client from the registry, forcing re-initialisation on the
    /// next `get_or_init` call for this key.
    ///
    /// Also cleans up the per-key init guard to prevent unbounded accumulation
    /// from previously failed init attempts.
    ///
    /// Call this when a task detects an unrecoverable connection failure (e.g.
    /// NATS disconnect, permanent auth rejection) so that subsequent init
    /// attempts create a fresh client instead of reusing the dead one.
    pub async fn remove(&self, key: &ClientKey) -> bool {
        let removed = {
            let mut clients = self.clients.write().await;
            clients.remove(key).is_some()
        };
        {
            let mut guards = self.init_guards.write().await;
            guards.remove(key);
        }
        removed
    }

    /// Returns the number of clients currently in the registry.
    pub async fn len(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Returns true if the registry contains no clients.
    pub async fn is_empty(&self) -> bool {
        self.clients.read().await.is_empty()
    }
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors from `ClientRegistry::get_or_init`.
#[derive(thiserror::Error, Debug)]
pub enum Error<E> {
    /// The init closure failed.
    #[error("Client initialization failed: {source}")]
    Init {
        #[source]
        source: E,
    },
    /// The stored client type does not match the requested downcast type.
    #[error("Client type mismatch — same ClientKey used with different types")]
    TypeMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FakeClient {
        value: String,
    }

    #[derive(Debug)]
    struct OtherClient {
        id: u32,
    }

    #[derive(thiserror::Error, Debug)]
    #[error("test error")]
    struct TestError;

    #[tokio::test]
    async fn get_or_init_creates_client_on_first_call() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"creds-a");

        let client: Arc<FakeClient> = registry
            .get_or_init(key, || async {
                Ok::<_, TestError>(FakeClient {
                    value: "hello".to_string(),
                })
            })
            .await
            .unwrap();

        assert_eq!(client.value, "hello");
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn get_or_init_reuses_existing_client() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"creds-a");

        let first: Arc<FakeClient> = registry
            .get_or_init(key.clone(), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "first".to_string(),
                })
            })
            .await
            .unwrap();

        let second: Arc<FakeClient> = registry
            .get_or_init(key, || async {
                Ok::<_, TestError>(FakeClient {
                    value: "second".to_string(),
                })
            })
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.value, "first");
    }

    #[tokio::test]
    async fn different_keys_create_different_clients() {
        let registry = ClientRegistry::new();

        let a: Arc<FakeClient> = registry
            .get_or_init(ClientKey::new(&"creds-a"), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "a".to_string(),
                })
            })
            .await
            .unwrap();

        let b: Arc<OtherClient> = registry
            .get_or_init(ClientKey::new(&"creds-b"), || async {
                Ok::<_, TestError>(OtherClient { id: 99 })
            })
            .await
            .unwrap();

        assert_eq!(a.value, "a");
        assert_eq!(b.id, 99);
        assert_eq!(registry.len().await, 2);
    }

    #[tokio::test]
    async fn same_identity_shares_client() {
        let registry = ClientRegistry::new();

        let first: Arc<FakeClient> = registry
            .get_or_init(ClientKey::new(&"/creds/shared.json"), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "shared".to_string(),
                })
            })
            .await
            .unwrap();

        let second: Arc<FakeClient> = registry
            .get_or_init(ClientKey::new(&"/creds/shared.json"), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "should not be called".to_string(),
                })
            })
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn init_error_propagates() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"bad-creds");

        let result: Result<Arc<FakeClient>, _> =
            registry.get_or_init(key, || async { Err(TestError) }).await;

        assert!(result.is_err());
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn retry_succeeds_after_init_failure() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"flaky-creds");

        let first: Result<Arc<FakeClient>, _> = registry
            .get_or_init(key.clone(), || async { Err(TestError) })
            .await;
        assert!(first.is_err());

        let second: Arc<FakeClient> = registry
            .get_or_init(key, || async {
                Ok::<_, TestError>(FakeClient {
                    value: "recovered".to_string(),
                })
            })
            .await
            .unwrap();

        assert_eq!(second.value, "recovered");
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn type_mismatch_returns_error() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"same-creds");

        let _: Arc<FakeClient> = registry
            .get_or_init(key.clone(), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "ok".to_string(),
                })
            })
            .await
            .unwrap();

        let result: Result<Arc<OtherClient>, _> = registry
            .get_or_init(key, || async { Ok::<_, TestError>(OtherClient { id: 1 }) })
            .await;

        assert!(matches!(result, Err(Error::TypeMismatch)));
    }

    #[tokio::test]
    async fn concurrent_init_only_runs_once() {
        let registry = Arc::new(ClientRegistry::new());
        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let reg = Arc::clone(&registry);
            let count = Arc::clone(&call_count);
            handles.push(tokio::spawn(async move {
                let _: Arc<FakeClient> = reg
                    .get_or_init(ClientKey::new(&"shared"), || {
                        let count = Arc::clone(&count);
                        async move {
                            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            Ok::<_, TestError>(FakeClient {
                                value: "shared".to_string(),
                            })
                        }
                    })
                    .await
                    .unwrap();
            }));
        }

        futures_util::future::join_all(handles).await;

        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn composite_key_hashes_multiple_fields() {
        let key_a = ClientKey::new(&("/creds/a.json", "https://api.sf.com"));
        let key_b = ClientKey::new(&("/creds/a.json", "https://other.sf.com"));
        let key_a2 = ClientKey::new(&("/creds/a.json", "https://api.sf.com"));

        assert_eq!(key_a, key_a2);
        assert_ne!(key_a, key_b);
    }

    #[tokio::test]
    async fn remove_forces_reinit() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"creds-a");

        let first: Arc<FakeClient> = registry
            .get_or_init(key.clone(), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "first".to_string(),
                })
            })
            .await
            .unwrap();

        assert!(registry.remove(&key).await);
        assert!(registry.is_empty().await);

        let second: Arc<FakeClient> = registry
            .get_or_init(key, || async {
                Ok::<_, TestError>(FakeClient {
                    value: "second".to_string(),
                })
            })
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(second.value, "second");
    }

    #[tokio::test]
    async fn remove_nonexistent_returns_false() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"nope");
        assert!(!registry.remove(&key).await);
    }

    #[tokio::test]
    async fn display_shows_hex_hash() {
        let key = ClientKey::new(&"my-creds");
        let display = format!("{key}");
        assert_eq!(display.len(), 16);
        assert!(display.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn init_guard_cleaned_up_after_success() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"creds");

        let _: Arc<FakeClient> = registry
            .get_or_init(key, || async {
                Ok::<_, TestError>(FakeClient {
                    value: "ok".to_string(),
                })
            })
            .await
            .unwrap();

        let guards = registry.init_guards.read().await;
        assert!(guards.is_empty());
    }

    #[tokio::test]
    async fn init_guard_retained_after_failure() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"bad");

        let _: Result<Arc<FakeClient>, _> =
            registry.get_or_init(key, || async { Err(TestError) }).await;

        let guards = registry.init_guards.read().await;
        assert_eq!(guards.len(), 1);
    }

    #[tokio::test]
    async fn remove_cleans_init_guards() {
        let registry = ClientRegistry::new();
        let key = ClientKey::new(&"fail-then-remove");

        let _: Result<Arc<FakeClient>, _> = registry
            .get_or_init(key.clone(), || async { Err(TestError) })
            .await;

        assert_eq!(registry.init_guards.read().await.len(), 1);

        registry.remove(&key).await;

        assert!(registry.init_guards.read().await.is_empty());
    }

    #[tokio::test]
    async fn remove_then_reinit_works() {
        let registry = Arc::new(ClientRegistry::new());
        let key = ClientKey::new(&"volatile");

        let first: Arc<FakeClient> = registry
            .get_or_init(key.clone(), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "v1".to_string(),
                })
            })
            .await
            .unwrap();
        assert_eq!(first.value, "v1");

        registry.remove(&key).await;

        let second: Arc<FakeClient> = registry
            .get_or_init(key.clone(), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "v2".to_string(),
                })
            })
            .await
            .unwrap();
        assert_eq!(second.value, "v2");
        assert!(!Arc::ptr_eq(&first, &second));

        assert_eq!(first.value, "v1");
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn concurrent_remove_and_get_or_init() {
        let registry = Arc::new(ClientRegistry::new());
        let key = ClientKey::new(&"race");

        let _: Arc<FakeClient> = registry
            .get_or_init(key.clone(), || async {
                Ok::<_, TestError>(FakeClient {
                    value: "original".to_string(),
                })
            })
            .await
            .unwrap();

        let mut handles = Vec::new();
        for i in 0..5 {
            let reg = Arc::clone(&registry);
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                if i == 0 {
                    reg.remove(&k).await;
                }
                let client: Arc<FakeClient> = reg
                    .get_or_init(k, || async {
                        Ok::<_, TestError>(FakeClient {
                            value: "refreshed".to_string(),
                        })
                    })
                    .await
                    .unwrap();
                client
            }));
        }

        let results: Vec<Arc<FakeClient>> = futures_util::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        for client in &results {
            assert!(!client.value.is_empty());
        }
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn concurrent_init_after_failure_runs_once() {
        let registry = Arc::new(ClientRegistry::new());
        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let _: Result<Arc<FakeClient>, _> = registry
            .get_or_init(ClientKey::new(&"flaky"), || async { Err(TestError) })
            .await;

        let mut handles = Vec::new();
        for _ in 0..10 {
            let reg = Arc::clone(&registry);
            let count = Arc::clone(&call_count);
            handles.push(tokio::spawn(async move {
                let _: Arc<FakeClient> = reg
                    .get_or_init(ClientKey::new(&"flaky"), || {
                        let count = Arc::clone(&count);
                        async move {
                            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            Ok::<_, TestError>(FakeClient {
                                value: "recovered".to_string(),
                            })
                        }
                    })
                    .await
                    .unwrap();
            }));
        }

        futures_util::future::join_all(handles).await;

        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(registry.len().await, 1);
    }
}
