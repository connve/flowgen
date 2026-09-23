//! Configuration structures for generate task types.
//!
//! Defines configuration options for event generation tasks that produce
//! synthetic or scheduled data streams in workflows.

use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};

/// Errors that can occur during configuration validation.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("Either 'interval' or 'cron' params must be specified")]
    MissingSchedule,
    #[error("Cannot specify both 'interval' and 'cron' params")]
    BothSchedulesSpecified,
    #[error("Invalid cron expression '{expression}': {source}")]
    InvalidCron {
        expression: String,
        #[source]
        source: croner::errors::CronError,
    },
    #[error("Invalid timezone '{0}', expected an IANA name such as 'Europe/London'")]
    InvalidTimezone(String),
    #[error("Interval must not exceed 100 years")]
    IntervalTooLong,
}

/// Longest accepted `interval` (`100y` in humantime, which counts 365.25-day years),
/// keeping schedule arithmetic far from overflow.
pub const MAX_INTERVAL: Duration = Duration::from_secs(3_155_760_000);

/// Configuration for generate subscriber tasks that produce scheduled events.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, Hash, Default)]
pub struct Subscriber {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Optional structured payload for generated events.
    /// When the payload is a JSON object, a `system_info` field is added to it;
    /// any other JSON value is sent as-is without `system_info`.
    pub payload: Option<serde_json::Value>,
    /// Interval - waits one interval after startup, then repeats every duration.
    /// After a restart, the first run is scheduled from the last successful run.
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
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
}

/// Parsed scheduling mode of a generate task.
#[derive(Clone, Debug)]
pub enum Schedule {
    /// Fires every `interval`, measured from the previous attempt.
    Interval(Duration),
    /// Fires at each occurrence of `cron`, evaluated in `timezone`.
    Cron {
        cron: Box<croner::Cron>,
        timezone: chrono_tz::Tz,
    },
    /// Fires back-to-back until `count` events have completed.
    Once,
}

impl Subscriber {
    /// Validates the config, including the cron expression and timezone.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.schedule()?;
        Ok(())
    }

    /// Parses the scheduling fields into a [`Schedule`].
    ///
    /// Exactly one of `interval` or `cron` is required, except in run-once
    /// mode where neither is set and `count` is.
    pub fn schedule(&self) -> Result<Schedule, ConfigError> {
        match (&self.interval, &self.cron, &self.count) {
            (Some(_), Some(_), _) => Err(ConfigError::BothSchedulesSpecified),
            (Some(interval), None, _) if *interval > MAX_INTERVAL => {
                Err(ConfigError::IntervalTooLong)
            }
            (Some(interval), None, _) => Ok(Schedule::Interval(*interval)),
            (None, Some(expression), _) => {
                let cron = croner::Cron::from_str(expression).map_err(|source| {
                    ConfigError::InvalidCron {
                        expression: expression.clone(),
                        source,
                    }
                })?;
                let timezone = match &self.timezone {
                    Some(name) => name
                        .parse()
                        .map_err(|_| ConfigError::InvalidTimezone(name.clone()))?,
                    None => chrono_tz::UTC,
                };
                Ok(Schedule::Cron {
                    cron: Box::new(cron),
                    timezone,
                })
            }
            (None, None, Some(_)) => Ok(Schedule::Once),
            (None, None, None) => Err(ConfigError::MissingSchedule),
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

    #[test]
    fn test_validation_rejects_invalid_cron() {
        let config = Subscriber {
            cron: Some("0 25 * * *".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidCron { .. })
        ));
    }

    #[test]
    fn test_validation_rejects_invalid_timezone() {
        let config = Subscriber {
            cron: Some("0 0 * * *".to_string()),
            timezone: Some("Europe/Londn".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidTimezone(_))
        ));
    }

    #[test]
    fn test_validation_rejects_interval_above_max() {
        let config = Subscriber {
            interval: Some(MAX_INTERVAL + Duration::from_secs(1)),
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::IntervalTooLong)
        ));
    }

    #[test]
    fn test_schedule_defaults_cron_timezone_to_utc() {
        let config = Subscriber {
            cron: Some("0 0 * * *".to_string()),
            ..Default::default()
        };
        match config.schedule() {
            Ok(Schedule::Cron { timezone, .. }) => assert_eq!(timezone, chrono_tz::UTC),
            other => panic!("expected a cron schedule, got {other:?}"),
        }
    }
}
