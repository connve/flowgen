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
#[serde(rename_all = "camelCase")]
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
///
/// Carries either a text delta (`status`, the default) or a tool step
/// (`tool`, when the agent invoked a tool mid-turn). They ride one channel so
/// tool steps stay ordered relative to the surrounding text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    /// Task name producing the progress update.
    pub task: String,
    /// Current status of the task. The streamed text delta; empty for a
    /// tool-step event.
    pub status: String,
    /// A tool the agent invoked, when this event marks a tool call rather than
    /// a text delta. `None` for ordinary text progress (the common case).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolStep>,
}

/// A tool invocation surfaced as a progress step: the tool's name and the
/// arguments it was called with. The result/status is not included — the agent
/// loop runs the tool internally and does not stream its outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolStep {
    /// Tool name (e.g. `bigquery_query`).
    pub name: String,
    /// Arguments the tool was called with.
    pub arguments: serde_json::Value,
    /// `correlation_id` of the flow run the call started.
    pub run: Option<String>,
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

    /// Registers response channels under `requested` when no entry holds it,
    /// otherwise under a new UUID v7, and returns the correlation ID used.
    pub async fn register(&self, requested: Option<String>, sender: ResponseSender) -> String {
        let mut entries = self.entries.lock().await;
        let mut correlation_id = match requested {
            Some(id) => id,
            None => uuid::Uuid::now_v7().to_string(),
        };
        while entries.contains_key(&correlation_id) {
            correlation_id = uuid::Uuid::now_v7().to_string();
        }
        entries.insert(correlation_id.clone(), sender);
        correlation_id
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
        let progress_tx = match self.entries.lock().await.get(correlation_id) {
            Some(sender) => sender.progress_tx.clone(),
            None => return false,
        };
        progress_tx.send(progress).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(status: &str) -> ProgressEvent {
        ProgressEvent {
            task: "test_task".to_string(),
            status: status.to_string(),
            tool: None,
        }
    }

    #[tokio::test]
    async fn test_registry_insert_and_remove() {
        let registry = ResponseRegistry::new();
        let (progress_tx, _progress_rx) = mpsc::channel(16);
        let (result_tx, _result_rx) = oneshot::channel();

        let id = registry
            .register(
                Some("corr-123".to_string()),
                ResponseSender {
                    progress_tx,
                    result_tx: Some(result_tx),
                },
            )
            .await;
        assert_eq!(id, "corr-123");

        let sender = registry.remove("corr-123").await;
        assert!(sender.is_some());

        let sender = registry.remove("corr-123").await;
        assert!(sender.is_none());
    }

    #[tokio::test]
    async fn a_registration_without_a_requested_id_gets_a_new_uuid() {
        let registry = ResponseRegistry::new();
        let (progress_tx, _progress_rx) = mpsc::channel(1);

        let id = registry
            .register(
                None,
                ResponseSender {
                    progress_tx,
                    result_tx: None,
                },
            )
            .await;

        assert!(uuid::Uuid::parse_str(&id).is_ok());
        assert!(registry.remove(&id).await.is_some());
    }

    #[tokio::test]
    async fn a_requested_id_already_registered_gets_a_new_one_and_the_first_entry_keeps_its_channel(
    ) {
        let registry = ResponseRegistry::new();
        let (first_tx, mut first_rx) = mpsc::channel(2);
        let (second_tx, mut second_rx) = mpsc::channel(2);
        let first = registry
            .register(
                Some("run".to_string()),
                ResponseSender {
                    progress_tx: first_tx,
                    result_tx: None,
                },
            )
            .await;

        let second = registry
            .register(
                Some("run".to_string()),
                ResponseSender {
                    progress_tx: second_tx,
                    result_tx: None,
                },
            )
            .await;
        registry.send_progress(&first, progress("first")).await;
        registry.send_progress(&second, progress("second")).await;

        assert_eq!(first, "run");
        assert_ne!(second, "run");
        assert_eq!(first_rx.recv().await.unwrap().status, "first");
        assert_eq!(second_rx.recv().await.unwrap().status, "second");
    }

    #[tokio::test]
    async fn a_progress_send_waiting_on_a_full_channel_does_not_block_other_runs() {
        let registry = ResponseRegistry::new();
        let (slow_tx, _slow_rx) = mpsc::channel(1);
        let (fast_tx, mut fast_rx) = mpsc::channel(1);
        let slow = registry
            .register(
                None,
                ResponseSender {
                    progress_tx: slow_tx,
                    result_tx: None,
                },
            )
            .await;
        let fast = registry
            .register(
                None,
                ResponseSender {
                    progress_tx: fast_tx,
                    result_tx: None,
                },
            )
            .await;
        registry.send_progress(&slow, progress("fills")).await;
        let blocked = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.send_progress(&slow, progress("waits")).await })
        };
        tokio::task::yield_now().await;

        let sent = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            registry.send_progress(&fast, progress("passes")),
        )
        .await;

        assert_eq!(sent.ok(), Some(true));
        assert_eq!(fast_rx.recv().await.unwrap().status, "passes");
        assert!(!blocked.is_finished());
        blocked.abort();
    }

    #[tokio::test]
    async fn test_registry_send_progress() {
        let registry = ResponseRegistry::new();
        let (progress_tx, mut progress_rx) = mpsc::channel(16);
        let (result_tx, _result_rx) = oneshot::channel();

        registry
            .register(
                Some("corr-456".to_string()),
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
                    tool: None,
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
                    tool: None,
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
            .register(
                Some("corr-789".to_string()),
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
