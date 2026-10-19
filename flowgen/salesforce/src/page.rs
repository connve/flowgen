//! Fetching one page of query results, retried in place.

use tokio::sync::Mutex;
use tracing::warn;

/// How a failed page request is retried.
pub(crate) enum Failure {
    /// The session expired or was revoked: log in again, then retry.
    Auth,
    /// Retrying might help.
    Transient,
    /// Retrying won't help.
    Permanent,
}

/// Runs `fetch` until it succeeds, waiting between attempts as `retry` says
/// and logging in again after an authentication failure. Retrying the page
/// itself, not the event, keeps pages already sent downstream from being sent
/// again. Returns the last error once retries run out, the error is
/// permanent, or the task is cancelled.
pub(crate) async fn fetch<T, E, F, Fut>(
    retry: &flowgen_core::retry::RetryConfig,
    cancellation: &tokio_util::sync::CancellationToken,
    sfdc_client: &Mutex<salesforce_core::client::Client>,
    classify: impl Fn(&E) -> Failure,
    mut fetch: F,
) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut delays = retry.strategy();
    loop {
        let error = match fetch().await {
            Ok(page) => return Ok(page),
            Err(error) => error,
        };
        let delay = match classify(&error) {
            Failure::Permanent => None,
            Failure::Transient => delays.next(),
            Failure::Auth => {
                if let Err(source) = sfdc_client.lock().await.reconnect().await {
                    warn!(error = %source, "Reconnecting after an authentication error failed");
                }
                delays.next()
            }
        };
        let delay = match delay {
            Some(delay) => delay,
            None => return Err(error),
        };
        warn!(error = %error, "Failed to fetch a page of results, retrying it");
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(error),
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn retry() -> flowgen_core::retry::RetryConfig {
        flowgen_core::retry::RetryConfig {
            max_attempts: Some(3),
            initial_backoff: Duration::from_millis(1),
        }
    }

    fn client() -> Mutex<salesforce_core::client::Client> {
        Mutex::new(
            salesforce_core::client::Builder::new()
                .client_id("a")
                .client_secret("b")
                .instance_url("https://a.my.salesforce.com")
                .build()
                .unwrap(),
        )
    }

    async fn attempts(failure: fn() -> Failure, failures: usize) -> (Result<usize, String>, usize) {
        let calls = AtomicUsize::new(0);
        let result = fetch(
            &retry(),
            &tokio_util::sync::CancellationToken::new(),
            &client(),
            |_: &String| failure(),
            || async {
                match calls.fetch_add(1, Ordering::SeqCst) < failures {
                    true => Err("down".to_string()),
                    false => Ok(7),
                }
            },
        )
        .await;
        (result, calls.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried_until_the_page_arrives() {
        assert_eq!(attempts(|| Failure::Transient, 2).await, (Ok(7), 3));
    }

    #[tokio::test]
    async fn retries_stop_after_the_configured_attempts() {
        assert_eq!(
            attempts(|| Failure::Transient, 5).await,
            (Err("down".to_string()), 3)
        );
    }

    #[tokio::test]
    async fn a_permanent_failure_is_not_retried() {
        assert_eq!(
            attempts(|| Failure::Permanent, 1).await,
            (Err("down".to_string()), 1)
        );
    }
}
