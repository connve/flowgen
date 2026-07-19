//! Log-query facade consumed by the web UI.
//!
//! Not an OTel standard — the OTel spec covers push (`LogExporter`) but
//! leaves query APIs vendor-specific. This trait lets the web layer
//! stay backend-agnostic across memory / Loki / VictoriaLogs.
//!
//! The memory implementation parses the same JSON lines that
//! `tracing_subscriber::fmt::json()` writes to stdout, so no extra
//! serialization path is introduced.

use crate::telemetry::StoredLog;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing_subscriber::fmt::MakeWriter;

/// Structured filter accepted by `LogsQuery::query` and
/// `LogsQuery::tail`. Fields are AND-combined; `None` = "any".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogFilter {
    /// Restrict to records whose `flow` attribute matches.
    pub flow: Option<String>,
    /// Restrict to records whose `task` attribute matches.
    pub task: Option<String>,
    /// Restrict to records at the given tracing level (`info` / `warn` / `error`).
    pub level: Option<String>,
    /// Restrict to records emitted at or after this UNIX epoch (ms).
    pub since_ms: Option<u64>,
}

impl LogFilter {
    /// Returns `true` when `record` satisfies every populated field.
    pub fn matches(&self, record: &StoredLog) -> bool {
        let mut flow_ok = self.flow.is_none();
        let mut task_ok = self.task.is_none();
        let mut level_ok = self.level.is_none();
        let mut since_ok = self.since_ms.is_none();
        for (k, v) in &record.attributes {
            match k.as_str() {
                "flow" => {
                    if let Some(expected) = &self.flow {
                        flow_ok = expected == v;
                    }
                }
                "task" => {
                    if let Some(expected) = &self.task {
                        task_ok = expected == v;
                    }
                }
                "level" => {
                    if let Some(expected) = &self.level {
                        level_ok = expected.eq_ignore_ascii_case(v);
                    }
                }
                "ts_ms" => {
                    if let Some(cutoff) = self.since_ms {
                        match v.parse::<u64>() {
                            Ok(ts) => since_ok = ts >= cutoff,
                            Err(_) => since_ok = false,
                        }
                    }
                }
                _ => {}
            }
        }
        flow_ok && task_ok && level_ok && since_ok
    }
}

/// Backend-agnostic log query facade.
#[async_trait]
pub trait LogsQuery: Send + Sync {
    async fn query(
        &self,
        filter: LogFilter,
        limit: usize,
    ) -> Result<Vec<StoredLog>, LogsQueryError>;

    async fn tail(
        &self,
        filter: LogFilter,
    ) -> Result<BoxStream<'static, StoredLog>, LogsQueryError>;
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum LogsQueryError {
    #[error("Log query backend error: {source}")]
    Backend {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Creates a paired writer / query backed by an in-memory per-flow
/// ring buffer of `capacity_per_flow` records.
///
/// The writer is meant to be handed to
/// `tracing_subscriber::fmt::layer().json().with_writer(...)`; the
/// query goes into the admin web state. Live-tail subscribers receive
/// records through a broadcast channel of the same capacity; slow
/// subscribers see dropped frames rather than backing up the writer.
pub fn pair(capacity_per_flow: usize) -> (MemoryLogsWriter, MemoryLogsQuery) {
    let inner = Arc::new(Inner {
        buffers: Mutex::new(HashMap::new()),
        capacity_per_flow,
    });
    let (tx, _rx) = broadcast::channel(capacity_per_flow.max(16));
    let writer = MemoryLogsWriter {
        inner: Arc::clone(&inner),
        tx: tx.clone(),
    };
    let query = MemoryLogsQuery { inner, tx };
    (writer, query)
}

#[derive(Debug)]
struct Inner {
    buffers: Mutex<HashMap<String, VecDeque<StoredLog>>>,
    capacity_per_flow: usize,
}

/// `MakeWriter` half of the pair returned by [`pair`].
#[derive(Debug, Clone)]
pub struct MemoryLogsWriter {
    inner: Arc<Inner>,
    tx: broadcast::Sender<StoredLog>,
}

impl MemoryLogsWriter {
    fn ingest(&self, line: &[u8]) {
        let Ok(parsed) = serde_json::from_slice::<JsonLogLine>(line) else {
            return;
        };
        let record = parsed.into_stored_log();
        let flow = flow_of(&record).unwrap_or_default();
        match self.inner.buffers.lock() {
            Ok(mut guard) => push_bounded(
                guard.entry(flow).or_default(),
                &record,
                self.inner.capacity_per_flow,
            ),
            Err(poisoned) => push_bounded(
                poisoned.into_inner().entry(flow).or_default(),
                &record,
                self.inner.capacity_per_flow,
            ),
        }
        let _ = self.tx.send(record);
    }
}

impl<'a> MakeWriter<'a> for MemoryLogsWriter {
    type Writer = LineBufferedWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        LineBufferedWriter {
            writer: self,
            buffer: Vec::new(),
        }
    }
}

/// `io::Write` returned by [`MemoryLogsWriter::make_writer`]. Buffers
/// bytes until a newline arrives, then hands the full JSON line to the
/// parent writer for parsing.
pub struct LineBufferedWriter<'a> {
    writer: &'a MemoryLogsWriter,
    buffer: Vec<u8>,
}

impl io::Write for LineBufferedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        while let Some(nl) = self.buffer.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=nl).collect();
            let trimmed = &line[..line.len() - 1];
            if !trimmed.is_empty() {
                self.writer.ingest(trimmed);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.writer.ingest(&line);
        }
        Ok(())
    }
}

impl Drop for LineBufferedWriter<'_> {
    fn drop(&mut self) {
        let _ = io::Write::flush(self);
    }
}

/// [`LogsQuery`] half of the pair returned by [`pair`].
#[derive(Debug, Clone)]
pub struct MemoryLogsQuery {
    inner: Arc<Inner>,
    tx: broadcast::Sender<StoredLog>,
}

impl MemoryLogsQuery {
    /// Removes every retained record from the per-flow ring buffers.
    /// Does not affect live tail subscribers.
    pub fn clear(&self) {
        match self.inner.buffers.lock() {
            Ok(mut guard) => guard.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }
}

#[async_trait]
impl LogsQuery for MemoryLogsQuery {
    async fn query(
        &self,
        filter: LogFilter,
        limit: usize,
    ) -> Result<Vec<StoredLog>, LogsQueryError> {
        let snapshot: Vec<StoredLog> = match self.inner.buffers.lock() {
            Ok(guard) => collect_from(&guard, &filter),
            Err(poisoned) => collect_from(&poisoned.into_inner(), &filter),
        };
        let start = snapshot.len().saturating_sub(limit);
        Ok(snapshot[start..].to_vec())
    }

    async fn tail(
        &self,
        filter: LogFilter,
    ) -> Result<BoxStream<'static, StoredLog>, LogsQueryError> {
        let rx = self.tx.subscribe();
        let stream = BroadcastStream::new(rx)
            .filter_map(move |res| {
                let filter = filter.clone();
                async move {
                    match res {
                        Ok(record) if filter.matches(&record) => Some(record),
                        _ => None,
                    }
                }
            })
            .boxed();
        Ok(stream)
    }
}

fn collect_from(
    buffers: &HashMap<String, VecDeque<StoredLog>>,
    filter: &LogFilter,
) -> Vec<StoredLog> {
    if let Some(flow) = &filter.flow {
        return match buffers.get(flow) {
            Some(q) => q.iter().filter(|r| filter.matches(r)).cloned().collect(),
            None => Vec::new(),
        };
    }
    buffers
        .values()
        .flat_map(|q| q.iter())
        .filter(|r| filter.matches(r))
        .cloned()
        .collect()
}

fn push_bounded(buffer: &mut VecDeque<StoredLog>, record: &StoredLog, capacity: usize) {
    if buffer.len() == capacity {
        buffer.pop_front();
    }
    buffer.push_back(record.clone());
}

fn flow_of(record: &StoredLog) -> Option<String> {
    record
        .attributes
        .iter()
        .find(|(k, _)| k == "flow")
        .map(|(_, v)| v.clone())
}

/// Parsed subset of one `tracing_subscriber::fmt::json()` line.
#[derive(Deserialize)]
struct JsonLogLine {
    #[serde(default)]
    level: String,
    #[serde(default)]
    target: String,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    fields: HashMap<String, serde_json::Value>,
    #[serde(default)]
    spans: Vec<HashMap<String, serde_json::Value>>,
}

impl JsonLogLine {
    fn into_stored_log(self) -> StoredLog {
        let mut attributes: Vec<(String, String)> = Vec::new();
        let mut body = String::new();

        if !self.level.is_empty() {
            attributes.push(("level".to_string(), self.level.to_ascii_lowercase()));
        }
        if !self.target.is_empty() {
            attributes.push(("target".to_string(), self.target));
        }
        if let Some(ts) = self.timestamp {
            attributes.push(("timestamp".to_string(), ts));
        }

        for span in self.spans {
            for (k, v) in span {
                if k == "name" {
                    continue;
                }
                attributes.push((k, json_value_to_string(&v)));
            }
        }

        for (k, v) in self.fields {
            if k == "message" {
                body = json_value_to_string(&v);
            } else {
                attributes.push((k, json_value_to_string(&v)));
            }
        }

        StoredLog { body, attributes }
    }
}

fn json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(flow: &str, task: &str, body: &str, ts_ms: u64) -> StoredLog {
        StoredLog {
            body: body.to_string(),
            attributes: vec![
                ("flow".to_string(), flow.to_string()),
                ("task".to_string(), task.to_string()),
                ("ts_ms".to_string(), ts_ms.to_string()),
            ],
        }
    }

    #[test]
    fn filter_matches_by_flow() {
        let f = LogFilter {
            flow: Some("orders".to_string()),
            ..Default::default()
        };
        assert!(f.matches(&record("orders", "handle", "ok", 1)));
        assert!(!f.matches(&record("payments", "handle", "ok", 1)));
    }

    #[test]
    fn filter_combines_flow_and_task() {
        let f = LogFilter {
            flow: Some("orders".to_string()),
            task: Some("handle".to_string()),
            ..Default::default()
        };
        assert!(f.matches(&record("orders", "handle", "ok", 1)));
        assert!(!f.matches(&record("orders", "emit", "ok", 1)));
    }

    #[test]
    fn filter_respects_since_ms() {
        let f = LogFilter {
            since_ms: Some(1000),
            ..Default::default()
        };
        assert!(f.matches(&record("orders", "handle", "ok", 1500)));
        assert!(!f.matches(&record("orders", "handle", "ok", 500)));
    }

    #[test]
    fn empty_filter_matches_everything() {
        let f = LogFilter::default();
        assert!(f.matches(&record("a", "b", "c", 1)));
    }

    #[test]
    fn json_line_flattens_spans_and_fields() {
        let line = r#"{"timestamp":"2026-07-17T07:12:08.699289Z","level":"WARN","fields":{"message":"boom","index":148},"target":"flowgen_salesforce::restapi::composite","spans":[{"flow":"mssql_to_salesforce","name":"flow.run"},{"task":"upsert","task_id":4,"task_type":"salesforce_restapi_composite","name":"task.run"},{"name":"task.handle"}]}"#;
        let parsed: JsonLogLine = serde_json::from_str(line).unwrap();
        let stored = parsed.into_stored_log();
        assert_eq!(stored.body, "boom");
        let attrs: HashMap<_, _> = stored.attributes.iter().cloned().collect();
        assert_eq!(
            attrs.get("flow").map(String::as_str),
            Some("mssql_to_salesforce")
        );
        assert_eq!(attrs.get("task").map(String::as_str), Some("upsert"));
        assert_eq!(attrs.get("task_id").map(String::as_str), Some("4"));
        assert_eq!(
            attrs.get("task_type").map(String::as_str),
            Some("salesforce_restapi_composite")
        );
        assert_eq!(attrs.get("level").map(String::as_str), Some("warn"));
        assert_eq!(attrs.get("index").map(String::as_str), Some("148"));
    }
}
