//! Hot-reload watcher for cache-sourced flows.
//!
//! Subscribes to key changes under the flow prefix in the system cache bucket and
//! forwards `WatchEvent`s to the reconciler. Reconnects automatically when the
//! stream fails or ends, and reads the keys again on each subscribe, so a lost
//! connection or a watch the cache ended for falling behind misses nothing.

use flowgen_core::cache::{Cache, WatchEvent};
use futures_util::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Errors that can occur in the watcher.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to subscribe to the watch stream.
    #[error("Failed to subscribe to watch stream for prefix '{prefix}': {source}")]
    WatchSubscribe {
        prefix: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    /// A watch stream event could not be read.
    #[error("Watch stream error for prefix '{prefix}': {source}")]
    WatchStream {
        prefix: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    /// The keys under the prefix could not be read again after subscribing.
    #[error("Failed to read the keys under prefix '{prefix}' after subscribing: {source}")]
    Resync {
        prefix: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    /// The watch stream ended unexpectedly; the caller will reconnect.
    #[error("Watch stream for prefix '{prefix}' ended unexpectedly")]
    WatchStreamEnded { prefix: String },
}

/// Spawns a background task that watches the cache for flow key changes.
///
/// Calls `cache.watch(prefix)` to subscribe to all key changes under the flow prefix,
/// maps each entry to a `WatchEvent`, and forwards it to the reconciler via `tx`.
/// Reconnects with a 5-second delay on stream errors. Exits cleanly when `shutdown`
/// is cancelled.
pub fn spawn(
    cache: Arc<dyn Cache>,
    prefix: String,
    tx: tokio::sync::mpsc::Sender<WatchEvent>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut known = HashSet::new();
        loop {
            match run_watch_loop(&cache, &prefix, &tx, &shutdown, &mut known).await {
                Ok(()) => break,
                Err(e) => {
                    error!(error = %e);
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        _ = shutdown.cancelled() => break,
                    }
                }
            }
        }
        info!("Hot-reload watcher stopped");
    })
}

/// Runs a single watch loop iteration until the stream ends or shutdown is signalled.
///
/// Returns `Ok(())` on clean shutdown, or an `Error` variant that causes the caller
/// to reconnect after a backoff delay.
async fn run_watch_loop(
    cache: &Arc<dyn Cache>,
    prefix: &str,
    tx: &tokio::sync::mpsc::Sender<WatchEvent>,
    shutdown: &CancellationToken,
    known: &mut HashSet<String>,
) -> Result<(), Error> {
    let mut stream = cache
        .watch(prefix, false)
        .await
        .map_err(|source| Error::WatchSubscribe {
            prefix: prefix.to_string(),
            source,
        })?;

    info!(prefix = %prefix, "Hot-reload watcher subscribed");
    if !resync(cache, prefix, tx, known).await? {
        return Ok(());
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            event = stream.next() => {
                match event {
                    None => {
                        warn!(prefix = %prefix, "Watch stream ended unexpectedly, reconnecting");
                        return Err(Error::WatchStreamEnded { prefix: prefix.to_string() });
                    }
                    Some(Err(source)) => {
                        return Err(Error::WatchStream {
                            prefix: prefix.to_string(),
                            source,
                        });
                    }
                    Some(Ok(ev)) => {
                        match &ev {
                            WatchEvent::Put { key, .. } => known.insert(key.clone()),
                            WatchEvent::Delete { key } => known.remove(key),
                        };
                        if tx.send(ev).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Sends a `Put` for every key under `prefix` and a `Delete` for each of
/// `known` that is gone, so changes made while no watch was open are not
/// missed; `known` becomes the keys read. Returns false once the reconciler
/// has shut down.
async fn resync(
    cache: &Arc<dyn Cache>,
    prefix: &str,
    tx: &tokio::sync::mpsc::Sender<WatchEvent>,
    known: &mut HashSet<String>,
) -> Result<bool, Error> {
    let failed = |source| Error::Resync {
        prefix: prefix.to_string(),
        source,
    };
    let keys: HashSet<String> = cache
        .list_keys(prefix)
        .await
        .map_err(failed)?
        .into_iter()
        .collect();
    let mut events = Vec::with_capacity(keys.len());
    for key in known.difference(&keys) {
        events.push(WatchEvent::Delete { key: key.clone() });
    }
    for key in &keys {
        if let Some(value) = cache.get(key).await.map_err(failed)? {
            events.push(WatchEvent::Put {
                key: key.clone(),
                value,
            });
        }
    }
    *known = keys;
    for event in events {
        if tx.send(event).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowgen_core::cache::memory::MemoryCache;

    #[tokio::test]
    async fn forwards_put_events() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn Cache>;
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let _handle = spawn(
            Arc::clone(&cache),
            "flows.".to_string(),
            tx,
            shutdown.clone(),
        );

        // Give the watcher time to subscribe.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        cache
            .put("flows.my-flow", bytes::Bytes::from("cfg"), None)
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("channel closed");

        match event {
            WatchEvent::Put { key, value } => {
                assert_eq!(key, "flows.my-flow");
                assert_eq!(value, bytes::Bytes::from("cfg"));
            }
            _ => panic!("Expected Put event"),
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn forwards_delete_events() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn Cache>;
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let _handle = spawn(
            Arc::clone(&cache),
            "flows.".to_string(),
            tx,
            shutdown.clone(),
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Seed a key then delete it.
        cache
            .put("flows.my-flow", bytes::Bytes::from("cfg"), None)
            .await
            .unwrap();
        // Drain the Put event.
        let _ = rx.recv().await;

        cache.delete("flows.my-flow").await.unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("channel closed");

        match event {
            WatchEvent::Delete { key } => {
                assert_eq!(key, "flows.my-flow");
            }
            _ => panic!("Expected Delete event"),
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn a_resubscribe_sends_what_changed_while_no_watch_was_open() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn Cache>;
        cache
            .put("flows.b", bytes::Bytes::from("b2"), None)
            .await
            .unwrap();
        cache
            .put("flows.c", bytes::Bytes::from("c"), None)
            .await
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let mut known = HashSet::from(["flows.a".to_string(), "flows.b".to_string()]);

        assert!(resync(&cache, "flows.", &tx, &mut known).await.unwrap());

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(match event {
                WatchEvent::Put { key, value } => {
                    format!("put {key} {}", String::from_utf8_lossy(&value))
                }
                WatchEvent::Delete { key } => format!("delete {key}"),
            });
        }
        events.sort();
        assert_eq!(
            events,
            vec!["delete flows.a", "put flows.b b2", "put flows.c c"]
        );
        assert_eq!(
            known,
            HashSet::from(["flows.b".to_string(), "flows.c".to_string()])
        );
    }

    #[tokio::test]
    async fn stops_on_shutdown() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn Cache>;
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let handle = spawn(
            Arc::clone(&cache),
            "flows.".to_string(),
            tx,
            shutdown.clone(),
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown.cancel();

        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("watcher did not stop within timeout")
            .expect("watcher task panicked");
    }

    #[tokio::test]
    async fn only_forwards_matching_prefix() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn Cache>;
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let _handle = spawn(
            Arc::clone(&cache),
            "flows.".to_string(),
            tx,
            shutdown.clone(),
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Non-matching prefix should not produce an event.
        cache
            .put("resources.tpl", bytes::Bytes::from("tpl"), None)
            .await
            .unwrap();

        // Matching prefix should produce an event.
        cache
            .put("flows.my-flow", bytes::Bytes::from("cfg"), None)
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("channel closed");

        match event {
            WatchEvent::Put { key, .. } => {
                assert_eq!(key, "flows.my-flow");
            }
            _ => panic!("Expected Put event"),
        }

        // Verify no extra event is pending for the non-matching key.
        let extra = rx.try_recv();
        assert!(
            extra.is_err(),
            "Should not have received event for non-matching prefix"
        );

        shutdown.cancel();
    }
}
