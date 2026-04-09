//! Git repository synchronization module.
//!
//! Handles cloning and pulling a Git repository using the system `git` CLI.
//! Supports SSH key and token-based authentication.

use crate::config::{GitAuth, GitAuthType, GitOptions};
use std::path::Path;
use tokio::process::Command;

/// Errors that can occur during Git operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Git clone failed: {0}")]
    CloneFailed(String),

    #[error("Git pull failed: {0}")]
    PullFailed(String),

    #[error("Failed to read HEAD commit: {0}")]
    HeadCommitFailed(String),

    #[error("Git command IO error: {source}")]
    Io {
        #[source]
        source: std::io::Error,
    },
}

/// Clones or pulls the repository and returns the HEAD commit SHA.
pub async fn clone_or_pull(config: &GitOptions) -> Result<String, Error> {
    let clone_path = &config.clone_path;

    if clone_path.join(".git").exists() {
        pull(clone_path, &config.auth).await?;
    } else {
        clone(
            &config.repository_url,
            &config.branch,
            clone_path,
            &config.auth,
        )
        .await?;
    }

    head_commit(clone_path, &config.auth).await
}

/// Clones a Git repository to the specified path.
async fn clone(
    repository_url: &str,
    branch: &str,
    clone_path: &Path,
    auth: &GitAuth,
) -> Result<(), Error> {
    let mut cmd = Command::new("git");
    cmd.args([
        "clone",
        "--branch",
        branch,
        "--single-branch",
        "--depth",
        "1",
    ]);
    cmd.arg(repository_url);
    cmd.arg(clone_path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd.output().await.map_err(|source| Error::Io { source })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::CloneFailed(stderr.to_string()));
    }

    Ok(())
}

/// Pulls the latest changes from the remote repository.
async fn pull(clone_path: &Path, auth: &GitAuth) -> Result<(), Error> {
    let mut cmd = Command::new("git");
    cmd.args(["pull", "--ff-only"]);
    cmd.current_dir(clone_path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd.output().await.map_err(|source| Error::Io { source })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::PullFailed(stderr.to_string()));
    }

    Ok(())
}

/// Reads the HEAD commit SHA from the repository.
async fn head_commit(clone_path: &Path, auth: &GitAuth) -> Result<String, Error> {
    let mut cmd = Command::new("git");
    cmd.args(["rev-parse", "HEAD"]);
    cmd.current_dir(clone_path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd.output().await.map_err(|source| Error::Io { source })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::HeadCommitFailed(stderr.to_string()));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Applies authentication environment variables to a Git command.
fn apply_auth_env(cmd: &mut Command, auth: &GitAuth) {
    match auth.auth_type {
        GitAuthType::Ssh => {
            let mut ssh_cmd = "ssh".to_string();

            if let Some(ref key_path) = auth.ssh_key_path {
                ssh_cmd.push_str(&format!(" -i {}", key_path.display()));
            }

            if let Some(ref known_hosts_path) = auth.ssh_known_hosts_path {
                ssh_cmd.push_str(&format!(
                    " -o UserKnownHostsFile={}",
                    known_hosts_path.display()
                ));
                ssh_cmd.push_str(" -o StrictHostKeyChecking=yes");
            } else {
                ssh_cmd.push_str(" -o StrictHostKeyChecking=no");
            }

            cmd.env("GIT_SSH_COMMAND", ssh_cmd);
        }
        GitAuthType::Token => {
            // Token auth is handled via the repository URL itself (e.g., https://token@github.com/...).
            // The caller should embed the token in the URL or use a credential helper.
            if let Some(ref token) = auth.token {
                cmd.env("GIT_ASKPASS", "echo");
                cmd.env("GIT_PASSWORD", token);
            }
        }
        GitAuthType::None => {}
    }
}
