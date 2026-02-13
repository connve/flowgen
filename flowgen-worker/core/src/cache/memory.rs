//! In-memory cache implementation using DashMap.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

/// Thread-safe in-memory cache implementation.
///
/// Uses DashMap for concurrent access without requiring external coordination.
/// Suitable for single-process deployments or as a fallback when distributed
/// caching is unavailable.
#[derive(Debug, Clone)]
pub struct MemoryCache {
    data: Arc<DashMap<String, Bytes>>,
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
    async fn put(&self, key: &str, value: Bytes) -> Result<(), super::Error> {
        self.data.insert(key.to_string(), value);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, super::Error> {
        Ok(self.data.get(key).map(|entry| entry.value().clone()))
    }

    async fn delete(&self, key: &str) -> Result<(), super::Error> {
        self.data.remove(key);
        Ok(())
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

        cache.put(key, value.clone()).await.unwrap();
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

        cache.put(key, value.clone()).await.unwrap();
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

        cache.put(key, value1).await.unwrap();
        cache.put(key, value2.clone()).await.unwrap();
        let result = cache.get(key).await.unwrap();

        assert_eq!(result, Some(value2));
    }

    #[tokio::test]
    async fn test_memory_cache_clone() {
        let cache1 = MemoryCache::new();
        let cache2 = cache1.clone();
        let key = "test_key";
        let value = Bytes::from("test_value");

        cache1.put(key, value.clone()).await.unwrap();
        let result = cache2.get(key).await.unwrap();

        assert_eq!(result, Some(value));
    }
}
