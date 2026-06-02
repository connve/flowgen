//! In-memory cache implementation using DashMap.

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::stream::BoxStream;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Entry with value and revision number for optimistic concurrency control.
#[derive(Debug, Clone)]
struct CacheEntry {
    value: Bytes,
    revision: u64,
}

/// A registered watcher: stores the sender half and the prefix it subscribed to.
#[derive(Debug)]
struct Watcher {
    prefix: String,
    tx: tokio::sync::mpsc::UnboundedSender<Result<super::WatchEvent, super::Error>>,
}

/// Thread-safe in-memory cache implementation.
///
/// Uses DashMap for concurrent access without requiring external coordination.
/// Suitable for single-process deployments or as a fallback when distributed
/// caching is unavailable.
#[derive(Debug, Clone)]
pub struct MemoryCache {
    data: Arc<DashMap<String, CacheEntry>>,
    watchers: Arc<Mutex<Vec<Watcher>>>,
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCache {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            watchers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Broadcasts an event to all watchers whose prefix matches the key.
    /// Removes watchers with closed channels.
    fn broadcast(&self, event: super::WatchEvent) {
        let key = match &event {
            super::WatchEvent::Put { key, .. } => key.as_str(),
            super::WatchEvent::Delete { key } => key.as_str(),
        };

        let mut watchers = self.watchers.lock().expect("watchers lock poisoned");
        watchers.retain(|w| {
            if w.tx.is_closed() {
                return false;
            }
            if key.starts_with(&w.prefix) {
                // If the receiver is gone, retain returns false on next check.
                let _ = w.tx.send(Ok(event.clone()));
            }
            true
        });
    }
}

#[async_trait::async_trait]
impl super::Cache for MemoryCache {
    /// Note: In-memory cache does not support TTL expiration.
    /// For TTL support, use a distributed cache like NATS.
    async fn put(
        &self,
        key: &str,
        value: Bytes,
        _ttl_secs: Option<u64>,
    ) -> Result<(), super::Error> {
        self.data
            .entry(key.to_string())
            .and_modify(|e| {
                e.value = value.clone();
                e.revision += 1;
            })
            .or_insert(CacheEntry {
                value: value.clone(),
                revision: 1,
            });

        self.broadcast(super::WatchEvent::Put {
            key: key.to_string(),
            value,
        });

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, super::Error> {
        Ok(self.data.get(key).map(|entry| entry.value.clone()))
    }

    async fn delete(&self, key: &str) -> Result<(), super::Error> {
        self.data.remove(key);

        self.broadcast(super::WatchEvent::Delete {
            key: key.to_string(),
        });

        Ok(())
    }

    async fn create(
        &self,
        key: &str,
        value: Bytes,
        _ttl_secs: Option<u64>,
    ) -> Result<u64, super::Error> {
        match self.data.entry(key.to_string()) {
            dashmap::Entry::Occupied(_) => Err(super::CacheError::AlreadyExists),
            dashmap::Entry::Vacant(entry) => {
                entry.insert(CacheEntry { value, revision: 1 });
                Ok(1)
            }
        }
    }

    async fn update(
        &self,
        key: &str,
        value: Bytes,
        expected_revision: u64,
        _ttl_secs: Option<u64>,
    ) -> Result<u64, super::Error> {
        match self.data.entry(key.to_string()) {
            dashmap::Entry::Occupied(mut entry) => {
                let current_revision = entry.get().revision;
                if current_revision != expected_revision {
                    return Err(super::CacheError::RevisionMismatch {
                        expected: expected_revision,
                        actual: current_revision,
                    });
                }
                let new_revision = current_revision + 1;
                entry.get_mut().value = value;
                entry.get_mut().revision = new_revision;
                Ok(new_revision)
            }
            dashmap::Entry::Vacant(_) => Err(super::CacheError::NotFound),
        }
    }

    async fn get_with_revision(&self, key: &str) -> Result<Option<(Bytes, u64)>, super::Error> {
        Ok(self
            .data
            .get(key)
            .map(|entry| (entry.value.clone(), entry.revision)))
    }

    async fn delete_with_revision(
        &self,
        key: &str,
        expected_revision: u64,
    ) -> Result<(), super::Error> {
        match self.data.entry(key.to_string()) {
            dashmap::Entry::Occupied(entry) => {
                let current_revision = entry.get().revision;
                if current_revision != expected_revision {
                    return Err(super::CacheError::RevisionMismatch {
                        expected: expected_revision,
                        actual: current_revision,
                    });
                }
                entry.remove();
                Ok(())
            }
            dashmap::Entry::Vacant(_) => Err(super::CacheError::NotFound),
        }
    }

    async fn get_revision(&self, key: &str) -> Result<Option<u64>, super::Error> {
        Ok(self.data.get(key).map(|entry| entry.revision))
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, super::Error> {
        Ok(self
            .data
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| entry.key().clone())
            .collect())
    }

    async fn watch(
        &self,
        prefix: &str,
    ) -> Result<BoxStream<'static, Result<super::WatchEvent, super::Error>>, super::Error> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let watcher = Watcher {
            prefix: prefix.to_string(),
            tx,
        };

        self.watchers
            .lock()
            .expect("watchers lock poisoned")
            .push(watcher);

        let stream = UnboundedReceiverStream::new(rx);
        // Filter is already done at broadcast time, but we box the stream here.
        Ok(Box::pin(stream)
            as BoxStream<
                'static,
                Result<super::WatchEvent, super::Error>,
            >)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::Cache;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_memory_cache_put_and_get() {
        let cache = MemoryCache::new();
        let key = "test_key";
        let value = Bytes::from("test_value");

        cache.put(key, value.clone(), None).await.unwrap();
        let result = cache.get(key).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_memory_cache_get_nonexistent() {
        let cache = MemoryCache::new();
        let result = cache.get("nonexistent").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_memory_cache_delete() {
        let cache = MemoryCache::new();
        let key = "test_key";
        let value = Bytes::from("test_value");

        cache.put(key, value.clone(), None).await.unwrap();
        cache.delete(key).await.unwrap();
        let result = cache.get(key).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_memory_cache_update() {
        let cache = MemoryCache::new();
        let key = "test_key";
        let value1 = Bytes::from("value1");
        let value2 = Bytes::from("value2");

        cache.put(key, value1, None).await.unwrap();
        cache.put(key, value2.clone(), None).await.unwrap();
        let result = cache.get(key).await.unwrap();

        assert_eq!(result, Some(value2));
    }

    #[tokio::test]
    async fn test_memory_cache_list_keys() {
        let cache = MemoryCache::new();

        cache
            .put("flowgen.flows.a", Bytes::from("a"), None)
            .await
            .unwrap();
        cache
            .put("flowgen.flows.b", Bytes::from("b"), None)
            .await
            .unwrap();
        cache
            .put("flowgen.resources.tpl", Bytes::from("tpl"), None)
            .await
            .unwrap();

        let mut flow_keys = cache.list_keys("flowgen.flows.").await.unwrap();
        flow_keys.sort();
        assert_eq!(flow_keys, vec!["flowgen.flows.a", "flowgen.flows.b"]);

        let resource_keys = cache.list_keys("flowgen.resources.").await.unwrap();
        assert_eq!(resource_keys, vec!["flowgen.resources.tpl"]);

        let empty_keys = cache.list_keys("nonexistent.").await.unwrap();
        assert!(empty_keys.is_empty());
    }

    #[tokio::test]
    async fn test_memory_cache_clone() {
        let cache1 = MemoryCache::new();
        let cache2 = cache1.clone();
        let key = "test_key";
        let value = Bytes::from("test_value");

        cache1.put(key, value.clone(), None).await.unwrap();
        let result = cache2.get(key).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_watch_receives_put_events() {
        let cache = MemoryCache::new();
        let mut stream = cache.watch("test.").await.unwrap();

        cache
            .put("test.key1", Bytes::from("value1"), None)
            .await
            .unwrap();

        let event = stream.next().await.unwrap().unwrap();
        match event {
            crate::cache::WatchEvent::Put { key, value } => {
                assert_eq!(key, "test.key1");
                assert_eq!(value, Bytes::from("value1"));
            }
            _ => panic!("Expected Put event"),
        }
    }

    #[tokio::test]
    async fn test_watch_receives_delete_events() {
        let cache = MemoryCache::new();
        cache
            .put("test.key1", Bytes::from("value1"), None)
            .await
            .unwrap();

        let mut stream = cache.watch("test.").await.unwrap();
        cache.delete("test.key1").await.unwrap();

        let event = stream.next().await.unwrap().unwrap();
        match event {
            crate::cache::WatchEvent::Delete { key } => {
                assert_eq!(key, "test.key1");
            }
            _ => panic!("Expected Delete event"),
        }
    }

    #[tokio::test]
    async fn test_watch_filters_by_prefix() {
        let cache = MemoryCache::new();
        let mut stream = cache.watch("flows.").await.unwrap();

        // This should NOT reach the watcher.
        cache
            .put("resources.tpl", Bytes::from("tpl"), None)
            .await
            .unwrap();
        // This should reach the watcher.
        cache
            .put("flows.my-flow", Bytes::from("cfg"), None)
            .await
            .unwrap();

        let event = stream.next().await.unwrap().unwrap();
        match event {
            crate::cache::WatchEvent::Put { key, .. } => {
                assert_eq!(key, "flows.my-flow");
            }
            _ => panic!("Expected Put event"),
        }
    }

    #[tokio::test]
    async fn test_watch_cleans_up_closed_senders() {
        let cache = MemoryCache::new();

        // Create a watcher and immediately drop its stream.
        {
            let _stream = cache.watch("test.").await.unwrap();
        }

        // The broadcast triggered by put should clean up the dead watcher.
        cache
            .put("test.key1", Bytes::from("val"), None)
            .await
            .unwrap();

        let watchers = cache.watchers.lock().unwrap();
        assert!(watchers.is_empty());
    }
}
