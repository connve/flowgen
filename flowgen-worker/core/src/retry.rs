//! Retry configuration and utilities for task execution.
//!
//! Provides exponential backoff with jitter retry logic for all task processors.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

/// Default initial backoff delay (1 second).
pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Retry configuration with exponential backoff and jitter.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (default: None = infinite retries).
    /// Set to Some(n) to limit retries to n attempts.
    #[serde(default)]
    pub max_attempts: Option<usize>,

    /// Initial backoff delay (default: "1s").
    /// Accepts human-readable durations like "500ms", "2s", "1m".
    /// Each subsequent retry doubles this delay with jitter applied.
    #[serde(default = "default_initial_backoff", with = "humantime_serde")]
    pub initial_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: None,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
        }
    }
}

impl RetryConfig {
    /// Creates a tokio-retry strategy with exponential backoff and jitter.
    ///
    /// Backoff grows unbounded with jitter to spread thundering herd across retrying tasks.
    /// Use max_attempts to control when to stop retrying.
    ///
    /// Example sequence with default 1s initial backoff (jitter adds approximately plus or minus 50%):
    /// - Attempt 1: ~1s
    /// - Attempt 2: ~2s
    /// - Attempt 3: ~4s
    /// - Attempt 4: ~8s
    /// - Attempt 5: ~16s
    /// - Attempt 6: ~32s
    /// - Attempt 7: ~64s (~1 minute)
    /// - Attempt 8: ~128s (~2 minutes)
    /// - ...grows indefinitely unless max_attempts is set
    pub fn strategy(&self) -> Box<dyn Iterator<Item = Duration> + Send> {
        let initial_ms = self.initial_backoff.as_millis() as u64;

        // ExponentialBackoff uses base^n * factor formula.
        // With base=2 and factor=initial_ms/2: first delay = 2 * (initial_ms/2) = initial_ms.
        // Clamping to 1 prevents integer truncation to zero for sub-2ms backoff values,
        // which would otherwise disable all delay between retries.
        let factor = (initial_ms / 2).max(1);
        let base_strategy = ExponentialBackoff::from_millis(2)
            .factor(factor)
            .map(jitter);

        match self.max_attempts {
            Some(max) => Box::new(base_strategy.take(max.saturating_sub(1))),
            None => Box::new(base_strategy),
        }
    }

    /// Merges task-level retry config with app-level config.
    ///
    /// Task-level config takes precedence over app-level.
    pub fn merge(app_level: &Option<RetryConfig>, task_level: &Option<RetryConfig>) -> RetryConfig {
        match (app_level, task_level) {
            (_, Some(task_config)) => task_config.clone(),
            (Some(app_config), None) => app_config.clone(),
            (None, None) => RetryConfig::default(),
        }
    }
}

fn default_initial_backoff() -> Duration {
    DEFAULT_INITIAL_BACKOFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, None);
        assert_eq!(config.initial_backoff, DEFAULT_INITIAL_BACKOFF);
    }

    #[test]
    fn test_retry_strategy_finite() {
        let config = RetryConfig {
            max_attempts: Some(3),
            initial_backoff: Duration::from_millis(100),
        };

        let delays: Vec<Duration> = config.strategy().collect();
        assert_eq!(delays.len(), 2); // max_attempts - 1
    }

    #[test]
    fn test_retry_strategy_infinite() {
        let config = RetryConfig {
            max_attempts: None,
            initial_backoff: Duration::from_millis(100),
        };

        let delays: Vec<Duration> = config.strategy().take(10).collect();
        assert_eq!(delays.len(), 10);
    }

    #[test]
    fn test_retry_strategy_grows_unbounded() {
        let config = RetryConfig {
            max_attempts: None,
            initial_backoff: Duration::from_secs(1),
        };

        let delays: Vec<Duration> = config.strategy().take(8).collect();
        assert_eq!(delays.len(), 8);
        for d in &delays {
            assert!(*d > Duration::ZERO);
        }
    }

    #[test]
    fn test_merge_task_level_override() {
        let app_config = Some(RetryConfig {
            max_attempts: Some(3),
            initial_backoff: Duration::from_millis(500),
        });

        let task_config = Some(RetryConfig {
            max_attempts: Some(10),
            initial_backoff: Duration::from_secs(2),
        });

        let merged = RetryConfig::merge(&app_config, &task_config);
        assert_eq!(merged.max_attempts, Some(10));
        assert_eq!(merged.initial_backoff, Duration::from_secs(2));
    }

    #[test]
    fn test_merge_app_level_fallback() {
        let app_config = Some(RetryConfig {
            max_attempts: Some(3),
            initial_backoff: Duration::from_millis(500),
        });

        let merged = RetryConfig::merge(&app_config, &None);
        assert_eq!(merged.max_attempts, Some(3));
        assert_eq!(merged.initial_backoff, Duration::from_millis(500));
    }

    #[test]
    fn test_merge_use_defaults() {
        let merged = RetryConfig::merge(&None, &None);
        assert_eq!(merged, RetryConfig::default());
    }

    #[test]
    fn test_deserialize_humantime() {
        let yaml = r#"
            max_attempts: 5
            initial_backoff: "2s"
        "#;
        let config: RetryConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.max_attempts, Some(5));
        assert_eq!(config.initial_backoff, Duration::from_secs(2));
    }

    #[test]
    fn test_deserialize_humantime_millis() {
        let yaml = r#"
            initial_backoff: "500ms"
        "#;
        let config: RetryConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.initial_backoff, Duration::from_millis(500));
    }

    #[test]
    fn test_strategy_sub_2ms_does_not_produce_zero_delays() {
        let config = RetryConfig {
            max_attempts: Some(5),
            initial_backoff: Duration::from_millis(1),
        };
        let delays: Vec<Duration> = config.strategy().collect();
        for d in &delays {
            assert!(*d > Duration::ZERO, "delay must not be zero");
        }
    }
}
