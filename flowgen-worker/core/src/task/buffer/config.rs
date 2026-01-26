//! Configuration for the buffer task processor.
//!
//! The buffer task collects individual events into batches before forwarding them downstream.
//! This enables efficient input/output operations, reduces API calls to cloud storage,
//! and is essential for columnar formats like Parquet.

use crate::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for buffer processor task.
///
/// Accumulates individual events into batches and emits them based on size or timeout triggers.
///
/// # Examples
///
/// Basic buffer configuration:
/// ```yaml
/// buffer:
///   name: "batch_rows"
///   size: 1000
/// ```
///
/// Buffer with custom timeout:
/// ```yaml
/// buffer:
///   name: "batch_rows"
///   size: 75
///   timeout: "10s"
/// ```
///
/// Buffer with partitioned grouping:
/// ```yaml
/// buffer:
///   name: "batch_by_program_country"
///   size: 75
///   timeout: "10s"
///   partition_key: "{{event.data.program_id}}.{{event.data.country}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// Unique name identifier for this buffer task.
    pub name: String,
    /// Number of events to collect before flushing the buffer.
    pub size: usize,
    /// Optional timeout duration to flush buffer even if not full (default: 30s).
    /// Accepts duration strings: "100ms", "30s", "5m", etc.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// Optional key template for partitioning events into separate buffers.
    /// When specified, events are partitioned by the rendered key value and each partition
    /// is buffered and flushed independently based on size and timeout.
    pub partition_key: Option<String>,
    /// Optional retry configuration (overrides app-level retry config).
    pub retry: Option<crate::retry::RetryConfig>,
}

/// Default timeout value of 30 seconds.
fn default_timeout() -> Option<Duration> {
    Some(Duration::from_secs(30))
}

impl ConfigExt for Processor {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_serialization() {
        let processor = Processor {
            name: "test_buffer".to_string(),
            size: 100,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            retry: None,
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(default_timeout(), Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_processor_with_custom_timeout() {
        let processor = Processor {
            name: "custom_buffer".to_string(),
            size: 75,
            timeout: Some(Duration::from_secs(10)),
            partition_key: None,
            retry: None,
        };

        assert_eq!(processor.timeout, Some(Duration::from_secs(10)));
        assert_eq!(processor.size, 75);
    }

    #[test]
    fn test_processor_with_partition_key() {
        let processor = Processor {
            name: "partitioned_buffer".to_string(),
            size: 75,
            timeout: Some(Duration::from_secs(30)),
            partition_key: Some("{{event.data.program_id}}.{{event.data.country}}".to_string()),
            retry: None,
        };

        assert_eq!(
            processor.partition_key,
            Some("{{event.data.program_id}}.{{event.data.country}}".to_string())
        );
        assert_eq!(processor.size, 75);
    }
}
