//! Log-query facade consumed by the web UI.
//!
//! Not an OTel standard — the OTel spec covers push (`LogExporter`) but
//! leaves query APIs vendor-specific. This trait lets the web layer
//! stay backend-agnostic across memory / Loki / VictoriaLogs.
//!
//! The memory implementation parses the same JSON lines that
//! `tracing_subscriber::fmt::json()` writes to stdout, so no extra
//! serialization path is introduced.

use crate::telemetry::{StoredLog, StoredSpan};
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

/// Structured filter accepted by `LogsStore::query` and
/// `LogsStore::tail`. Fields are AND-combined; `None` = "any".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogFilter {
    /// Restrict to records whose `flow` attribute matches.
    pub flow: Option<String>,
    /// Restrict to records whose `task` attribute matches.
    pub task: Option<String>,
    /// Restrict to records at one of these tracing levels; empty matches every level.
    pub levels: Vec<String>,
    /// Restrict to records emitted at or after this UNIX epoch (ms).
    pub since_ms: Option<u64>,
    /// Restrict to records emitted at or before this UNIX epoch (ms).
    pub until_ms: Option<u64>,
}

impl LogFilter {
    /// Parses comma-separated levels such as `warn,error`, skipping empty entries.
    pub fn parse_levels(levels: &str) -> Vec<String> {
        levels
            .split(',')
            .map(str::trim)
            .filter(|level| !level.is_empty())
            .map(str::to_ascii_lowercase)
            .collect()
    }

    /// Returns `true` when `record` satisfies every populated field.
    pub fn matches(&self, record: &StoredLog) -> bool {
        let level_ok = self.levels.is_empty()
            || self
                .levels
                .iter()
                .any(|level| level.eq_ignore_ascii_case(&record.level));
        let ts_ms = timestamp_ms(record);
        let since_ok = match self.since_ms {
            None => true,
            Some(cutoff) => matches!(ts_ms, Some(t) if t >= cutoff),
        };
        let until_ok = match self.until_ms {
            None => true,
            Some(cutoff) => matches!(ts_ms, Some(t) if t <= cutoff),
        };
        let flow_ok = match (&self.flow, span_field(record, "flow")) {
            (None, _) => true,
            (Some(expected), Some(v)) => expected == v,
            (Some(_), None) => false,
        };
        let task_ok = match (&self.task, span_field(record, "task")) {
            (None, _) => true,
            (Some(expected), Some(v)) => expected == v,
            (Some(_), None) => false,
        };
        flow_ok && task_ok && level_ok && since_ok && until_ok
    }
}

fn timestamp_ms(record: &StoredLog) -> Option<u64> {
    record
        .timestamp
        .as_deref()
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.timestamp_millis() as u64)
}

/// Returns the first field value with `key` found across the span chain,
/// leaf-to-root (so an inner span shadowing an outer span wins).
fn span_field<'a>(record: &'a StoredLog, key: &str) -> Option<&'a str> {
    record
        .spans
        .iter()
        .rev()
        .flat_map(|s| s.fields.iter())
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// Largest `limit` a log query serves, bounding one response's size.
pub const MAX_QUERY_LIMIT: usize = 10_000;

/// Backend-agnostic log query facade.
#[async_trait]
pub trait LogsStore: Send + Sync {
    /// Returns retained records matching `filter`, oldest first, capped
    /// at `limit`.
    async fn query(
        &self,
        filter: LogFilter,
        limit: usize,
    ) -> Result<Vec<StoredLog>, LogsStoreError>;

    /// Subscribes to records matching `filter` as they arrive.
    async fn tail(
        &self,
        filter: LogFilter,
    ) -> Result<BoxStream<'static, StoredLog>, LogsStoreError>;
}

/// Errors returned by [`LogsStore`] implementations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum LogsStoreError {
    #[error("Log query backend error: {source}")]
    Backend {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Creates a paired writer / query backed by in-memory ring buffers of
/// `capacity_per_flow` records, one per flow and level.
///
/// The writer is meant to be handed to
/// `tracing_subscriber::fmt::layer().json().with_writer(...)`; the
/// query goes into the web state. Live-tail subscribers receive
/// records through a broadcast channel of the same capacity; slow
/// subscribers see dropped frames rather than backing up the writer.
pub fn pair(capacity_per_flow: usize) -> (MemoryLogsStoreWriter, MemoryLogsStore) {
    let inner = Arc::new(Inner {
        buffers: Mutex::new(HashMap::new()),
        capacity_per_flow,
    });
    let (tx, _rx) = broadcast::channel(capacity_per_flow.max(16));
    let writer = MemoryLogsStoreWriter {
        inner: Arc::clone(&inner),
        tx: tx.clone(),
    };
    let query = MemoryLogsStore { inner, tx };
    (writer, query)
}

type Buffers = HashMap<Option<String>, HashMap<String, VecDeque<StoredLog>>>;

#[derive(Debug)]
struct Inner {
    buffers: Mutex<Buffers>,
    capacity_per_flow: usize,
}

/// `MakeWriter` half of the pair returned by [`pair`].
#[derive(Debug, Clone)]
pub struct MemoryLogsStoreWriter {
    inner: Arc<Inner>,
    tx: broadcast::Sender<StoredLog>,
}

impl MemoryLogsStoreWriter {
    fn ingest(&self, line: &[u8]) {
        let Ok(parsed) = serde_json::from_slice::<JsonLogLine>(line) else {
            return;
        };
        let record = parsed.into_stored_log();
        let flow = flow_of(&record);
        match self.inner.buffers.lock() {
            Ok(mut guard) => push_bounded(&mut guard, flow, &record, self.inner.capacity_per_flow),
            Err(poisoned) => push_bounded(
                &mut poisoned.into_inner(),
                flow,
                &record,
                self.inner.capacity_per_flow,
            ),
        }
        let _ = self.tx.send(record);
    }
}

impl<'a> MakeWriter<'a> for MemoryLogsStoreWriter {
    type Writer = LineBufferedWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        LineBufferedWriter {
            writer: self,
            buffer: Vec::new(),
        }
    }
}

/// `io::Write` returned by [`MemoryLogsStoreWriter::make_writer`]. Buffers
/// bytes until a newline arrives, then hands the full JSON line to the
/// parent writer for parsing.
pub struct LineBufferedWriter<'a> {
    writer: &'a MemoryLogsStoreWriter,
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

/// [`LogsStore`] half of the pair returned by [`pair`].
#[derive(Debug, Clone)]
pub struct MemoryLogsStore {
    inner: Arc<Inner>,
    tx: broadcast::Sender<StoredLog>,
}

impl MemoryLogsStore {
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
impl LogsStore for MemoryLogsStore {
    async fn query(
        &self,
        filter: LogFilter,
        limit: usize,
    ) -> Result<Vec<StoredLog>, LogsStoreError> {
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
    ) -> Result<BoxStream<'static, StoredLog>, LogsStoreError> {
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

fn collect_from(buffers: &Buffers, filter: &LogFilter) -> Vec<StoredLog> {
    let mut records: Vec<StoredLog> = buffers
        .iter()
        .filter(|(flow, _)| match (&filter.flow, flow) {
            (None, _) => true,
            (Some(expected), Some(flow)) => expected == flow,
            (Some(_), None) => false,
        })
        .flat_map(|(_, levels)| levels.values())
        .flat_map(|buffer| buffer.iter())
        .filter(|record| filter.matches(record))
        .cloned()
        .collect();
    records.sort_by_cached_key(timestamp_ms);
    records
}

fn push_bounded(buffers: &mut Buffers, flow: Option<String>, record: &StoredLog, capacity: usize) {
    let buffer = buffers
        .entry(flow)
        .or_default()
        .entry(record.level.clone())
        .or_default();
    if buffer.len() == capacity {
        buffer.pop_front();
    }
    buffer.push_back(record.clone());
}

fn flow_of(record: &StoredLog) -> Option<String> {
    span_field(record, "flow").map(str::to_string)
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
        let mut body = String::new();
        let mut fields: Vec<(String, String)> = Vec::new();
        for (k, v) in self.fields {
            match k.as_str() {
                "message" => body = json_value_to_string(&v),
                _ => fields.push((k, json_value_to_string(&v))),
            }
        }

        let spans = self
            .spans
            .into_iter()
            .map(|mut span| {
                let name = match span.remove("name") {
                    Some(serde_json::Value::String(s)) => s,
                    Some(other) => json_value_to_string(&other),
                    None => String::new(),
                };
                let fields = span
                    .into_iter()
                    .map(|(k, v)| (k, json_value_to_string(&v)))
                    .collect();
                StoredSpan { name, fields }
            })
            .collect();

        StoredLog {
            body,
            level: self.level.to_ascii_lowercase(),
            timestamp: self.timestamp,
            target: self.target,
            spans,
            fields,
        }
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
        let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms as i64)
            .map(|dt| dt.to_rfc3339());
        StoredLog {
            body: body.to_string(),
            level: "info".to_string(),
            timestamp,
            target: String::new(),
            spans: vec![
                StoredSpan {
                    name: "flow.run".to_string(),
                    fields: vec![("flow".to_string(), flow.to_string())],
                },
                StoredSpan {
                    name: "task.run".to_string(),
                    fields: vec![("task".to_string(), task.to_string())],
                },
            ],
            fields: Vec::new(),
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
    fn filter_matches_any_of_the_levels() {
        let at = |level: &str| StoredLog {
            level: level.to_string(),
            ..record("orders", "handle", "ok", 1)
        };
        let f = LogFilter {
            levels: LogFilter::parse_levels("warn, ERROR,"),
            ..Default::default()
        };
        assert_eq!(f.levels, vec!["warn", "error"]);
        assert!(f.matches(&at("warn")));
        assert!(f.matches(&at("error")));
        assert!(!f.matches(&at("info")));
    }

    #[tokio::test]
    async fn query_limit_counts_only_matching_levels() {
        let (writer, store) = pair(100);
        let mut line = writer.make_writer();
        for level in ["ERROR", "INFO", "INFO", "INFO"] {
            let json = format!(
                r#"{{"level":"{level}","fields":{{"message":"m"}},"target":"t","spans":[{{"flow":"orders","name":"flow.run"}}]}}"#
            );
            io::Write::write_all(&mut line, json.as_bytes()).unwrap();
            io::Write::write_all(&mut line, b"\n").unwrap();
        }
        let errors = LogFilter {
            levels: vec!["error".to_string()],
            ..Default::default()
        };

        let records = store.query(errors, 2).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].level, "error");
    }

    fn write_line(writer: &MemoryLogsStoreWriter, flow: &str, level: &str, ts: &str, body: &str) {
        let json = format!(
            r#"{{"timestamp":"{ts}","level":"{level}","fields":{{"message":"{body}"}},"target":"t","spans":[{{"flow":"{flow}","name":"flow.run"}}]}}"#
        );
        let mut line = writer.make_writer();
        io::Write::write_all(&mut line, json.as_bytes()).unwrap();
        io::Write::write_all(&mut line, b"\n").unwrap();
    }

    #[tokio::test]
    async fn logs_without_a_flow_match_no_flow_filter() {
        let (writer, store) = pair(10);
        let mut line = writer.make_writer();
        io::Write::write_all(
            &mut line,
            br#"{"timestamp":"2026-09-28T10:00:00Z","level":"INFO","fields":{"message":"started"},"target":"t","spans":[]}"#,
        )
        .unwrap();
        io::Write::write_all(&mut line, b"\n").unwrap();
        drop(line);
        write_line(&writer, "orders", "INFO", "2026-09-28T10:00:01Z", "ok");
        let scoped = |flow: &str| LogFilter {
            flow: Some(flow.to_string()),
            ..Default::default()
        };

        let all = store.query(LogFilter::default(), 10).await.unwrap();
        let orders = store.query(scoped("orders"), 10).await.unwrap();
        let unnamed = store.query(scoped(""), 10).await.unwrap();

        assert_eq!(all.len(), 2);
        assert_eq!(orders.len(), 1);
        assert!(unnamed.is_empty());
    }

    #[tokio::test]
    async fn info_burst_does_not_evict_errors() {
        let (writer, store) = pair(2);
        write_line(&writer, "orders", "ERROR", "2026-09-28T10:00:00Z", "failed");
        for second in 1..=5 {
            let ts = format!("2026-09-28T10:00:0{second}Z");
            write_line(&writer, "orders", "INFO", &ts, "ok");
        }

        let records = store.query(LogFilter::default(), 100).await.unwrap();

        let levels: Vec<&str> = records.iter().map(|r| r.level.as_str()).collect();
        assert_eq!(levels, vec!["error", "info", "info"]);
    }

    #[tokio::test]
    async fn query_returns_the_newest_records_across_flows() {
        let (writer, store) = pair(10);
        write_line(&writer, "a", "INFO", "2026-09-28T10:00:01Z", "a1");
        write_line(&writer, "b", "INFO", "2026-09-28T10:00:02Z", "b1");
        write_line(&writer, "a", "INFO", "2026-09-28T10:00:03Z", "a2");
        write_line(&writer, "b", "WARN", "2026-09-28T10:00:04Z", "b2");

        let records = store.query(LogFilter::default(), 3).await.unwrap();

        let bodies: Vec<&str> = records.iter().map(|r| r.body.as_str()).collect();
        assert_eq!(bodies, vec!["b1", "a2", "b2"]);
    }

    #[test]
    fn empty_filter_matches_everything() {
        let f = LogFilter::default();
        assert!(f.matches(&record("a", "b", "c", 1)));
    }

    #[test]
    fn json_line_preserves_spans_and_event_fields() {
        let line = r#"{"timestamp":"2026-07-17T07:12:08.699289Z","level":"WARN","fields":{"message":"boom","index":148},"target":"flowgen_salesforce::restapi::composite","spans":[{"flow":"mssql_to_salesforce","name":"flow.run"},{"task":"upsert","task_id":4,"task_type":"salesforce_restapi_composite","name":"task.run"},{"name":"task.handle"}]}"#;
        let parsed: JsonLogLine = serde_json::from_str(line).unwrap();
        let stored = parsed.into_stored_log();

        assert_eq!(stored.body, "boom");
        assert_eq!(stored.level, "warn");
        assert_eq!(stored.target, "flowgen_salesforce::restapi::composite");
        assert_eq!(
            stored.timestamp.as_deref(),
            Some("2026-07-17T07:12:08.699289Z")
        );

        let span_names: Vec<&str> = stored.spans.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(span_names, vec!["flow.run", "task.run", "task.handle"]);

        assert_eq!(span_field(&stored, "flow"), Some("mssql_to_salesforce"));
        assert_eq!(span_field(&stored, "task"), Some("upsert"));
        assert_eq!(span_field(&stored, "task_id"), Some("4"));
        assert_eq!(
            span_field(&stored, "task_type"),
            Some("salesforce_restapi_composite")
        );

        let event_fields: HashMap<_, _> = stored.fields.iter().cloned().collect();
        assert_eq!(event_fields.get("index").map(String::as_str), Some("148"));
    }
}
