//! Retry configuration and utilities for task execution.
//!
//! Provides exponential backoff retry logic for all task processors.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;

/// Default initial backoff delay in milliseconds
pub const DEFAULT_INITIAL_BACKOFF_MS: u64 = 1000;

/// Default maximum backoff delay in milliseconds
pub const DEFAULT_MAX_BACKOFF_MS: u64 = 30000;

/// Retry configuration with exponential backoff and jitter.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (default: None = infinite retries)
    /// Set to Some(n) to limit retries to n attempts
    #[serde(default)]
    pub max_attempts: Option<usize>,

    /// Initial backoff delay in milliseconds (default: 1000ms = 1s)
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,

    /// Maximum backoff delay in milliseconds (default: 30000ms = 30s)
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: None, // Infinite retries by default
            initial_backoff_ms: DEFAULT_INITIAL_BACKOFF_MS,
            max_backoff_ms: DEFAULT_MAX_BACKOFF_MS,
        }
    }
}

impl RetryConfig {
    /// Creates a tokio-retry strategy with exponential backoff.
    ///
    /// Backoff sequence with defaults (1s initial, 30s max):
    /// - Attempt 1: 1s
    /// - Attempt 2: 2s
    /// - Attempt 3: 4s
    /// - Attempt 4: 8s
    /// - Attempt 5: 16s
    /// - Attempt 6+: 30s (capped at max)
    ///
    /// If max_attempts is None, retries indefinitely.
    /// If max_attempts is Some(n), retries up to n-1 times (n total attempts).
    pub fn strategy(&self) -> Box<dyn Iterator<Item = Duration> + Send> {
        let base_strategy = ExponentialBackoff::from_millis(2)
            .factor(self.initial_backoff_ms / 2)
            .max_delay(Duration::from_millis(self.max_backoff_ms));

        match self.max_attempts {
            Some(max) => Box::new(base_strategy.take(max.saturating_sub(1))),
            None => Box::new(base_strategy), // Infinite iterator
        }
    }

    /// Merge task-level retry config with app-level config.
    ///
    /// Task-level config takes precedence over app-level.
    pub fn merge(app_level: &Option<RetryConfig>, task_level: &Option<RetryConfig>) -> RetryConfig {
        match (app_level, task_level) {
            (_, Some(task_config)) => task_config.clone(), // Task-level overrides
            (Some(app_config), None) => app_config.clone(), // Use app-level
            (None, None) => RetryConfig::default(),        // Use defaults
        }
    }
}

fn default_initial_backoff_ms() -> u64 {
    DEFAULT_INITIAL_BACKOFF_MS
}

fn default_max_backoff_ms() -> u64 {
    DEFAULT_MAX_BACKOFF_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, None); // Infinite retries by default
        assert_eq!(config.initial_backoff_ms, DEFAULT_INITIAL_BACKOFF_MS);
        assert_eq!(config.max_backoff_ms, DEFAULT_MAX_BACKOFF_MS);
    }

    #[test]
    fn test_retry_strategy_finite() {
        let config = RetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
        };

        let delays: Vec<Duration> = config.strategy().collect();
        assert_eq!(delays.len(), 2); // max_attempts - 1
    }

    #[test]
    fn test_retry_strategy_infinite() {
        let config = RetryConfig {
            max_attempts: None,
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
        };

        // Take first 10 attempts from infinite iterator
        let delays: Vec<Duration> = config.strategy().take(10).collect();
        assert_eq!(delays.len(), 10); // Should be able to take as many as needed
    }

    #[test]
    fn test_merge_task_level_override() {
        let app_config = Some(RetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 500,
            max_backoff_ms: 5000,
        });

        let task_config = Some(RetryConfig {
            max_attempts: Some(10),
            initial_backoff_ms: 2000,
            max_backoff_ms: 60000,
        });

        let merged = RetryConfig::merge(&app_config, &task_config);
        assert_eq!(merged.max_attempts, Some(10)); // Task-level wins
        assert_eq!(merged.initial_backoff_ms, 2000);
    }

    #[test]
    fn test_merge_app_level_fallback() {
        let app_config = Some(RetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 500,
            max_backoff_ms: 5000,
        });

        let merged = RetryConfig::merge(&app_config, &None);
        assert_eq!(merged.max_attempts, Some(3)); // App-level used
    }

    #[test]
    fn test_merge_use_defaults() {
        let merged = RetryConfig::merge(&None, &None);
        assert_eq!(merged, RetryConfig::default());
    }
}
