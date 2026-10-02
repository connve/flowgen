//! Git operations for flowgen.
//!
//! Provides Git-based integrations including repository synchronization
//! for loading flows and resources from remote repositories into the cache.

/// Git push — commit file changes and push them over smart HTTP.
pub mod push {
    /// Commit building and the receive-pack exchange.
    pub mod client;
    /// Configuration for the git push task.
    pub mod config;
    /// Git push processor implementation.
    pub mod processor;
}
/// HTTPS credentials and shallow clones shared by the git tasks.
pub mod remote;

/// Git sync — clone and pull a repository, sync content to the cache.
pub mod sync {
    /// Configuration for the git sync task.
    pub mod config;
    /// Git sync processor implementation.
    pub mod processor;
}
