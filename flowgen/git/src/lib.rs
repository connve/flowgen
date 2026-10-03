//! Git tasks.
//!
//! `git_sync` clones a branch and emits its files as events; `git_push`
//! commits the files of an event onto a branch and pushes the commit over
//! HTTPS.

/// Git push: commit file changes and push them over smart HTTP.
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

/// Git sync: clone a branch and emit its files as events.
pub mod sync {
    /// Configuration for the git sync task.
    pub mod config;
    /// Git sync processor implementation.
    pub mod processor;
}
