//! Cache synchronization module.
//!
//! Orchestrates the full sync cycle: Git pull, discover, validate,
//! and sync flows/resources to the cache backend.

use crate::config::ServerAppConfig;
use flowgen_core::cache::Cache;
use std::collections::HashSet;
use std::path::Path;
use tracing::debug;

/// Errors that can occur during cache synchronization.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Git sync error: {source}")]
    Git {
        #[source]
        source: crate::git::Error,
    },

    #[error("Discovery error: {source}")]
    Discovery {
        #[source]
        source: walkdir::Error,
    },

    #[error("Validation error: {source}")]
    Validation {
        #[source]
        source: crate::validate::Error,
    },

    #[error("Cache error: {source}")]
    Cache {
        #[source]
        source: flowgen_core::cache::CacheError,
    },

    #[error("Failed to read file '{path}': {source}")]
    FileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("JSON serialization error: {source}")]
    Json {
        #[source]
        source: serde_json::Error,
    },
}

/// Statistics from a single sync cycle.
#[derive(Debug, Default)]
pub struct SyncStats {
    /// Number of new flows added to cache.
    pub flows_added: usize,
    /// Number of existing flows updated in cache.
    pub flows_updated: usize,
    /// Number of flows deleted from cache.
    pub flows_deleted: usize,
    /// Number of flows unchanged.
    pub flows_unchanged: usize,
    /// Number of resources synced to cache.
    pub resources_synced: usize,
    /// HEAD commit SHA after sync.
    pub commit: String,
}

/// Runs a complete sync cycle: Git pull, discover, validate, and sync to cache.
pub async fn run_sync(config: &ServerAppConfig, cache: &dyn Cache) -> Result<SyncStats, Error> {
    // Pull latest from Git.
    let commit = crate::git::clone_or_pull(&config.flows.git)
        .await
        .map_err(|source| Error::Git { source })?;

    let repo_path = &config.flows.git.clone_path;
    let flows_base = repo_path.join(&config.flows.git.flows_path);

    // Discover and validate flows.
    let flow_files = crate::discover::discover_flows(&flows_base)
        .map_err(|source| Error::Discovery { source })?;

    let flows = crate::validate::validate_flows(&flow_files)
        .map_err(|source| Error::Validation { source })?;

    // Sync flows to cache.
    let mut stats = sync_flows(cache, &config.flows.cache_prefix, &flows).await?;
    stats.commit = commit;

    // Sync resources if configured.
    if let Some(ref resource_config) = config.flow_resources {
        let resources_base = repo_path.join(&resource_config.resources_path);

        if resources_base.exists() {
            stats.resources_synced =
                sync_resources(cache, &resource_config.cache_prefix, &resources_base).await?;
        }
    }

    // Write sync metadata to cache.
    write_metadata(cache, &stats).await?;

    Ok(stats)
}

/// Syncs validated flows to the cache, adding, updating, and deleting as needed.
async fn sync_flows(
    cache: &dyn Cache,
    prefix: &str,
    flows: &std::collections::HashMap<String, crate::validate::ValidatedFlow>,
) -> Result<SyncStats, Error> {
    let mut stats = SyncStats::default();

    // Get current flow keys from cache.
    let current_keys = cache
        .list_keys(prefix)
        .await
        .map_err(|source| Error::Cache { source })?;

    // Put new or updated flows.
    for (flow_name, validated) in flows {
        let key = format!("{prefix}.{flow_name}");

        match cache
            .get(&key)
            .await
            .map_err(|source| Error::Cache { source })?
        {
            Some(existing) => {
                let existing_content = String::from_utf8_lossy(&existing);
                if existing_content == validated.content {
                    stats.flows_unchanged += 1;
                    continue;
                }
                debug!(flow = %flow_name, "Updating flow in cache.");
                stats.flows_updated += 1;
            }
            None => {
                debug!(flow = %flow_name, "Adding new flow to cache.");
                stats.flows_added += 1;
            }
        }

        cache
            .put(&key, bytes::Bytes::from(validated.content.clone()), None)
            .await
            .map_err(|source| Error::Cache { source })?;
    }

    // Delete flows that no longer exist in the repository.
    let git_keys: HashSet<String> = flows
        .keys()
        .map(|name| format!("{prefix}.{name}"))
        .collect();

    for key in &current_keys {
        if !git_keys.contains(key) {
            let flow_name = key.strip_prefix(&format!("{prefix}.")).unwrap_or(key);
            debug!(flow = %flow_name, "Deleting flow from cache.");
            cache
                .delete(key)
                .await
                .map_err(|source| Error::Cache { source })?;
            stats.flows_deleted += 1;
        }
    }

    Ok(stats)
}

/// Syncs resource files to cache, keyed by their relative path.
async fn sync_resources(cache: &dyn Cache, prefix: &str, base_path: &Path) -> Result<usize, Error> {
    let resource_files = crate::discover::discover_resources(base_path)
        .map_err(|source| Error::Discovery { source })?;

    let mut count = 0;

    for file_path in &resource_files {
        let content = std::fs::read(file_path).map_err(|source| Error::FileRead {
            path: file_path.display().to_string(),
            source,
        })?;

        let rel_path = file_path
            .strip_prefix(base_path)
            .unwrap_or(file_path.as_path());
        let key = format!("{}.{}", prefix, rel_path.display());

        // Only update if content changed.
        let should_put = match cache
            .get(&key)
            .await
            .map_err(|source| Error::Cache { source })?
        {
            Some(existing) => existing.as_ref() != content.as_slice(),
            None => true,
        };

        if should_put {
            cache
                .put(&key, bytes::Bytes::from(content), None)
                .await
                .map_err(|source| Error::Cache { source })?;
        }

        count += 1;
    }

    Ok(count)
}

/// Writes sync metadata to the cache for observability.
async fn write_metadata(cache: &dyn Cache, stats: &SyncStats) -> Result<(), Error> {
    let metadata = serde_json::json!({
        "last_sync_at": chrono::Utc::now().timestamp(),
        "last_commit": stats.commit,
        "flows_added": stats.flows_added,
        "flows_updated": stats.flows_updated,
        "flows_deleted": stats.flows_deleted,
        "flows_unchanged": stats.flows_unchanged,
        "resources_synced": stats.resources_synced,
    });

    let value = serde_json::to_vec(&metadata).map_err(|source| Error::Json { source })?;

    cache
        .put(
            "flowgen.metadata.sync_status",
            bytes::Bytes::from(value),
            None,
        )
        .await
        .map_err(|source| Error::Cache { source })?;

    Ok(())
}
