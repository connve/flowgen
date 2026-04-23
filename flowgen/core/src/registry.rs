//! Response registry for correlating requests with pipeline results.
//!
//! When a source task (MCP server, streaming webhook, etc.) receives a request,
//! it creates a registry entry with channels for progress streaming and final
//! result delivery. The entry's correlation_id is threaded through `Event.meta`
//! so downstream tasks can look up the channels and deliver results back to the
//! waiting response stream.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Event meta key for threading the correlation identifier through the pipeline.
pub const CORRELATION_ID: &str = "correlation_id";
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Tool result containing content items and an error flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    /// Content items returned by the tool.
    pub content: Vec<Content>,
    /// Whether the tool call resulted in an error.
    #[serde(default)]
    pub is_error: bool,
}

/// Content item (text, image, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Content {
    #[serde(rename = "text")]
    Text { text: String },
}

/// Progress event streamed to the client during pipeline execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    /// Task name producing the progress update.
    pub task: String,
    /// Current status of the task.
    pub status: String,
}

/// Channels for delivering results back to a waiting response stream.
pub struct ResponseSender {
    /// For streaming intermediate progress events.
    pub progress_tx: mpsc::Sender<ProgressEvent>,
    /// For sending the final result. Wrapped in Option so it can be taken once.
    pub result_tx: Option<oneshot::Sender<ToolResult>>,
}

/// Registry mapping correlation IDs to response channels.
///
/// Thread-safe registry that source tasks (MCP server, webhook) write to
/// (creating entries) and pipeline tasks read from (sending results). Entries
/// are cleaned up after the result is sent or on timeout.
#[derive(Clone)]
pub struct ResponseRegistry {
    entries: Arc<Mutex<HashMap<String, ResponseSender>>>,
}

impl std::fmt::Debug for ResponseRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseRegistry")
            .field("entries", &"<HashMap>")
            .finish()
    }
}

impl Default for ResponseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseRegistry {
    /// Creates a new empty response registry.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a new correlation ID with its response channels.
    pub async fn insert(&self, correlation_id: String, sender: ResponseSender) {
        let mut entries = self.entries.lock().await;
        entries.insert(correlation_id, sender);
    }

    /// Removes and returns the response sender for a correlation ID.
    pub async fn remove(&self, correlation_id: &str) -> Option<ResponseSender> {
        let mut entries = self.entries.lock().await;
        entries.remove(correlation_id)
    }

    /// Sends a progress event for a correlation ID without removing the entry.
    ///
    /// Returns `true` if the progress was sent successfully, `false` if the
    /// correlation ID was not found or the receiver was dropped.
    pub async fn send_progress(&self, correlation_id: &str, progress: ProgressEvent) -> bool {
        let entries = self.entries.lock().await;
        if let Some(sender) = entries.get(correlation_id) {
            sender.progress_tx.send(progress).await.is_ok()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registry_insert_and_remove() {
        let registry = ResponseRegistry::new();
        let (progress_tx, _progress_rx) = mpsc::channel(16);
        let (result_tx, _result_rx) = oneshot::channel();

        registry
            .insert(
                "corr-123".to_string(),
                ResponseSender {
                    progress_tx,
                    result_tx: Some(result_tx),
                },
            )
            .await;

        let sender = registry.remove("corr-123").await;
        assert!(sender.is_some());

        let sender = registry.remove("corr-123").await;
        assert!(sender.is_none());
    }

    #[tokio::test]
    async fn test_registry_send_progress() {
        let registry = ResponseRegistry::new();
        let (progress_tx, mut progress_rx) = mpsc::channel(16);
        let (result_tx, _result_rx) = oneshot::channel();

        registry
            .insert(
                "corr-456".to_string(),
                ResponseSender {
                    progress_tx,
                    result_tx: Some(result_tx),
                },
            )
            .await;

        let sent = registry
            .send_progress(
                "corr-456",
                ProgressEvent {
                    task: "test_task".to_string(),
                    status: "running".to_string(),
                },
            )
            .await;
        assert!(sent);

        let progress = progress_rx.recv().await.unwrap();
        assert_eq!(progress.task, "test_task");
        assert_eq!(progress.status, "running");
    }

    #[tokio::test]
    async fn test_registry_send_progress_not_found() {
        let registry = ResponseRegistry::new();
        let sent = registry
            .send_progress(
                "nonexistent",
                ProgressEvent {
                    task: "test".to_string(),
                    status: "running".to_string(),
                },
            )
            .await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn test_registry_result_delivery() {
        let registry = ResponseRegistry::new();
        let (progress_tx, _progress_rx) = mpsc::channel(16);
        let (result_tx, result_rx) = oneshot::channel();

        registry
            .insert(
                "corr-789".to_string(),
                ResponseSender {
                    progress_tx,
                    result_tx: Some(result_tx),
                },
            )
            .await;

        let mut sender = registry.remove("corr-789").await.unwrap();
        let tx = sender.result_tx.take().unwrap();
        tx.send(ToolResult {
            content: vec![Content::Text {
                text: "result".to_string(),
            }],
            is_error: false,
        })
        .unwrap();

        let result = result_rx.await.unwrap();
        assert!(!result.is_error);
        assert_eq!(result.content.len(), 1);
    }
}
