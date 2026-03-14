//! In-memory cache implementation using DashMap.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

/// Entry with value and revision number for optimistic concurrency control.
#[derive(Debug, Clone)]
struct CacheEntry {
    value: Bytes,
    revision: u64,
}

/// Thread-safe in-memory cache implementation.
///
/// Uses DashMap for concurrent access without requiring external coordination.
/// Suitable for single-process deployments or as a fallback when distributed
/// caching is unavailable.
#[derive(Debug, Clone)]
pub struct MemoryCache {
    data: Arc<DashMap<String, CacheEntry>>,
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
        }
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
            .or_insert(CacheEntry { value, revision: 1 });
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, super::Error> {
        Ok(self.data.get(key).map(|entry| entry.value.clone()))
    }

    async fn delete(&self, key: &str) -> Result<(), super::Error> {
        self.data.remove(key);
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

    async fn get_with_revision(
        &self,
        key: &str,
    ) -> Result<Option<(Bytes, u64)>, super::Error> {
        Ok(self
            .data
            .get(key)
            .map(|entry| (entry.value.clone(), entry.revision)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::Cache;

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
    async fn test_memory_cache_clone() {
        let cache1 = MemoryCache::new();
        let cache2 = cache1.clone();
        let key = "test_key";
        let value = Bytes::from("test_value");

        cache1.put(key, value.clone(), None).await.unwrap();
        let result = cache2.get(key).await.unwrap();

        assert_eq!(result, Some(value));
    }
}
