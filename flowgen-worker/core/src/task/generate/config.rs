//! Configuration structures for generate task types.
//!
//! Defines configuration options for event generation tasks that produce
//! synthetic or scheduled data streams in workflows.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Errors that can occur during configuration validation.
#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("Either 'interval' or 'cron' params must be specified")]
    MissingSchedule,
    #[error("Cannot specify both 'interval' and 'cron' params")]
    BothSchedulesSpecified,
}

/// Configuration for generate subscriber tasks that produce scheduled events.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Subscriber {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Optional structured payload for generated events.
    /// Accepts arbitrary JSON that will be included in the event alongside system_info.
    pub payload: Option<serde_json::Value>,
    /// Interval - runs IMMEDIATELY then repeats every duration.
    /// Accepts duration strings: "100ms", "30s", "5m", etc.
    /// Mutually exclusive with `cron`.
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// Cron expression for calendar-based scheduling.
    /// First event fires at the NEXT time matching the cron expression.
    /// Uses standard cron syntax: "MIN HOUR DAY MONTH WEEKDAY"
    /// Examples:
    ///   - "0 0 * * *" = Daily at midnight
    ///   - "*/5 * * * *" = Every 5 minutes
    ///   - "0 9-17 * * MON-FRI" = Hourly during business hours weekdays
    ///
    /// Mutually exclusive with `interval`.
    pub cron: Option<String>,
    /// Optional timezone for cron scheduling (defaults to UTC).
    /// Uses IANA timezone names (e.g., "US/Eastern", "Europe/London", "Asia/Tokyo").
    #[serde(default)]
    pub timezone: Option<String>,
    /// Optional maximum number of events to generate before stopping.
    /// When specified without `interval` or `cron`, enables run-once mode
    /// where the task executes immediately and stops after generating
    /// the specified number of events.
    pub count: Option<u64>,
    /// When true, resets the persisted counter on startup so the task always
    /// re-runs from zero. Useful for test/setup flows that should execute on every deploy.
    #[serde(default)]
    pub allow_rerun: bool,
    /// Timeout for waiting on flow completion before considering the event failed.
    /// If not specified, waits indefinitely for flow completion.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<Duration>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
}

impl Subscriber {
    /// Validates that exactly one scheduling method is specified.
    /// Allows neither interval nor cron when count is specified (run-once mode).
    pub fn validate(&self) -> Result<(), ConfigError> {
        match (&self.interval, &self.cron, &self.count) {
            // Both interval and cron specified - error
            (Some(_), Some(_), _) => Err(ConfigError::BothSchedulesSpecified),
            // Neither interval nor cron, but count is specified - OK (run-once mode)
            (None, None, Some(_)) => Ok(()),
            // Neither interval nor cron, and no count - error
            (None, None, None) => Err(ConfigError::MissingSchedule),
            // One of interval or cron specified - OK
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_subscriber_config_default() {
        let config = Subscriber::default();
        assert_eq!(config.name, String::new());
        assert!(config.payload.is_none());
        assert!(config.interval.is_none());
        assert!(config.cron.is_none());
        assert!(config.count.is_none());
        assert!(config.ack_timeout.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn test_subscriber_config_with_interval() {
        let config = Subscriber {
            name: "test_task_name".to_string(),
            payload: Some(json!({"test": "data"})),
            interval: Some(Duration::from_secs(5)),
            cron: None,
            count: Some(10),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        };

        assert_eq!(config.name, "test_task_name");
        assert_eq!(config.payload, Some(json!({"test": "data"})));
        assert_eq!(config.interval, Some(Duration::from_secs(5)));
        assert!(config.cron.is_none());
        assert_eq!(config.count, Some(10));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_subscriber_config_with_cron() {
        let config = Subscriber {
            name: "cron_task".to_string(),
            payload: None,
            interval: None,
            cron: Some("0 0 * * *".to_string()),
            count: None,
            ack_timeout: None,
            retry: None,
            ..Default::default()
        };

        assert_eq!(config.cron, Some("0 0 * * *".to_string()));
        assert!(config.interval.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_both_specified() {
        let config = Subscriber {
            name: "test".to_string(),
            payload: None,
            interval: Some(Duration::from_secs(60)),
            cron: Some("0 0 * * *".to_string()),
            count: None,
            ack_timeout: None,
            retry: None,
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(ConfigError::BothSchedulesSpecified)
        ));
    }

    #[test]
    fn test_validation_neither_specified_no_count() {
        let config = Subscriber {
            name: "test".to_string(),
            payload: None,
            interval: None,
            cron: None,
            count: None,
            ack_timeout: None,
            retry: None,
            ..Default::default()
        };

        assert!(matches!(
            config.validate(),
            Err(ConfigError::MissingSchedule)
        ));
    }

    #[test]
    fn test_validation_run_once_mode() {
        let config = Subscriber {
            name: "test".to_string(),
            payload: None,
            interval: None,
            cron: None,
            count: Some(1),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        };

        // Should be valid - run-once mode
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_subscriber_config_serialization() {
        let config = Subscriber {
            name: "serialize_test".to_string(),
            payload: None,
            interval: Some(Duration::from_secs(1)),
            cron: None,
            count: Some(5),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Subscriber = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_subscriber_config_clone() {
        let config = Subscriber {
            name: "clone_test".to_string(),
            payload: Some(json!({"clone": "data"})),
            interval: Some(Duration::from_secs(2)),
            cron: None,
            count: None,
            ack_timeout: None,
            retry: None,
            ..Default::default()
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}
