//! Git operations for flowgen.
//!
//! Provides Git-based integrations including repository synchronization
//! for loading flows and resources from remote repositories into the cache.

/// Git sync — clone and pull a repository, sync content to the cache.
pub mod sync {
    /// Configuration for the git sync task.
    pub mod config;
    /// Git sync processor implementation.
    pub mod processor;
}
