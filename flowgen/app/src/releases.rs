//! The newest flowgen release, checked in the background so the web UI can
//! say when an update is available.

use crate::config::UpdateCheckOptions;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::watch;
use tracing::debug;

/// How long one check may take.
const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// This build's version.
pub const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Deserialize)]
struct Release {
    tag_name: String,
}

/// The newest released version when it is newer than this build.
pub type Latest = watch::Sender<Option<String>>;

/// Checks the newest release now and then every `options.interval`; a failed
/// check keeps the last answer.
pub async fn watch_latest(options: UpdateCheckOptions, latest: Latest) {
    let client = match client() {
        Ok(client) => client,
        Err(e) => {
            debug!(error = %e, "Failed to build the release check client");
            return;
        }
    };
    let mut interval = tokio::time::interval(options.interval);
    loop {
        interval.tick().await;
        match newest_release(&client, &options.url).await {
            Ok(version) => {
                latest.send_replace(is_newer(&version, CURRENT_VERSION).then_some(version));
            }
            Err(e) => debug!(error = %e, "Failed to check the newest flowgen release"),
        }
    }
}

/// The client release checks and downloads go through.
pub(crate) fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(CHECK_TIMEOUT)
        .user_agent(format!("flowgen/{CURRENT_VERSION}"))
        .build()
}

/// The version of the newest release `url` names.
pub(crate) async fn newest_release(
    client: &reqwest::Client,
    url: &str,
) -> Result<String, reqwest::Error> {
    let release: Release = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(release.tag_name.trim_start_matches('v').to_string())
}

/// Whether `candidate` is a later `major.minor.patch` than `current`.
pub(crate) fn is_newer(candidate: &str, current: &str) -> bool {
    match (parse(candidate), parse(current)) {
        (Some(candidate), Some(current)) => candidate > current,
        _ => false,
    }
}

fn parse(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.').map(|part| part.parse::<u64>().ok());
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(Some(major)), Some(Some(minor)), Some(Some(patch)), None) => {
            Some((major, minor, patch))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_later_release_is_newer() {
        assert!(is_newer("0.155.0", "0.154.0"));
        assert!(is_newer("1.0.0", "0.155.3"));
        assert!(is_newer("0.154.10", "0.154.9"));
    }

    #[test]
    fn the_same_or_an_earlier_release_is_not_newer() {
        assert!(!is_newer("0.154.0", "0.154.0"));
        assert!(!is_newer("0.153.9", "0.154.0"));
    }

    #[test]
    fn an_unreadable_version_is_not_newer() {
        assert!(!is_newer("0.155.0-rc.1", "0.154.0"));
        assert!(!is_newer("latest", "0.154.0"));
        assert!(!is_newer("0.155", "0.154.0"));
    }
}
