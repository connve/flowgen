//! Hot-reload watcher for cache-sourced flows.
//!
//! Subscribes to key changes under the flow prefix in the system cache bucket and
//! forwards `WatchEvent`s to the reconciler. Reconnects automatically on stream
//! errors so transient NATS disconnects do not permanently disable hot-reload.

use flowgen_core::cache::{Cache, WatchEvent};
use futures_util::StreamExt;
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
        loop {
            match run_watch_loop(&cache, &prefix, &tx, &shutdown).await {
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
        info!("Hot-reload watcher stopped.");
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
) -> Result<(), Error> {
    let mut stream = cache
        .watch(prefix)
        .await
        .map_err(|source| Error::WatchSubscribe {
            prefix: prefix.to_string(),
            source,
        })?;

    info!(prefix = %prefix, "Hot-reload watcher subscribed.");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            event = stream.next() => {
                match event {
                    None => {
                        warn!(prefix = %prefix, "Watch stream ended unexpectedly; reconnecting.");
                        return Err(Error::WatchStreamEnded { prefix: prefix.to_string() });
                    }
                    Some(Err(source)) => {
                        return Err(Error::WatchStream {
                            prefix: prefix.to_string(),
                            source,
                        });
                    }
                    Some(Ok(ev)) => {
                        // If the receiver is gone the reconciler has shut down; exit cleanly.
                        if tx.send(ev).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
