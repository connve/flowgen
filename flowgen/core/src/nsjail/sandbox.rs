//! Sandbox executor for running scripts in isolated nsjail environment.

use serde::{Deserialize, Serialize};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::debug;

/// Sandbox configuration for script execution.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxConfig {
    /// Memory limit in megabytes.
    ///
    /// Default: 512 MB
    #[serde(default = "default_memory_limit_mb")]
    pub memory_limit_mb: u64,

    /// CPU time limit in seconds.
    ///
    /// Default: 30 seconds
    #[serde(default = "default_time_limit_seconds")]
    pub time_limit_seconds: u64,

    /// Maximum number of processes/threads.
    ///
    /// Default: 10
    #[serde(default = "default_max_pids")]
    pub max_pids: u32,

    /// Enable network access in sandbox.
    ///
    /// When false, sandboxed process cannot make network connections.
    ///
    /// Default: false (no network)
    #[serde(default)]
    pub allow_network: bool,

    /// Path to nsjail binary.
    ///
    /// If not specified, searches for nsjail in PATH.
    ///
    /// Default: "nsjail"
    #[serde(default = "default_nsjail_path")]
    pub nsjail_path: String,

    /// User ID to run the sandboxed process as.
    ///
    /// Default: 99999 (nobody)
    #[serde(default = "default_user_id")]
    pub user_id: u32,

    /// Group ID to run the sandboxed process as.
    ///
    /// Default: 99999 (nogroup)
    #[serde(default = "default_group_id")]
    pub group_id: u32,
}

fn default_memory_limit_mb() -> u64 {
    512
}

fn default_time_limit_seconds() -> u64 {
    30
}

fn default_max_pids() -> u32 {
    10
}

fn default_nsjail_path() -> String {
    "nsjail".to_string()
}

fn default_user_id() -> u32 {
    99999
}

fn default_group_id() -> u32 {
    99999
}

/// Errors that can occur during sandbox execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to spawn nsjail process: {source}")]
    SpawnFailed {
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to wait for nsjail process: {source}")]
    WaitFailed {
        #[source]
        source: std::io::Error,
    },
    #[error("NsJail execution failed with exit code {code}: {stderr}")]
    ExecutionFailed { code: i32, stderr: String },
    #[error("NsJail process was terminated by signal")]
    TerminatedBySignal,
    #[error("Failed to capture stdout handle from child process")]
    CaptureStdout,
    #[error("Failed to capture stderr handle from child process")]
    CaptureStderr,
    #[error("Failed to join stdout reading task: {source}")]
    JoinStdoutTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("Failed to join stderr reading task: {source}")]
    JoinStderrTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("Failed to write stdin: {source}")]
    WriteStdin {
        #[source]
        source: std::io::Error,
    },
}

/// Result of sandbox execution.
#[derive(Debug, Clone)]
pub struct SandboxResult {
    /// Standard output from the sandboxed process.
    pub stdout: String,
    /// Standard error from the sandboxed process.
    pub stderr: String,
    /// Exit code from the sandboxed process.
    pub exit_code: i32,
    /// Whether execution was successful.
    pub success: bool,
}

/// Sandbox executor for running commands in isolated nsjail environment.
pub struct SandboxExecutor {
    config: SandboxConfig,
}

impl SandboxExecutor {
    /// Creates a new SandboxExecutor with the given configuration.
    pub fn new(config: SandboxConfig) -> Self {
        Self { config }
    }

    /// Executes a command in the sandbox.
    ///
    /// # Arguments
    ///
    /// * `command` - The command to execute (e.g., "/bin/bash")
    /// * `args` - Arguments to pass to the command
    /// * `stdin_data` - Optional data to pipe to the command's stdin
    pub async fn execute(
        &self,
        command: &str,
        args: &[String],
        stdin_data: Option<&str>,
    ) -> Result<SandboxResult, Error> {
        debug!(
            command = %command,
            args = ?args,
            memory_mb = self.config.memory_limit_mb,
            time_limit = self.config.time_limit_seconds,
            "Executing command in nsjail sandbox"
        );

        let mut cmd = Command::new(&self.config.nsjail_path);

        // User and group isolation.
        cmd.arg("--user")
            .arg(self.config.user_id.to_string())
            .arg("--group")
            .arg(self.config.group_id.to_string());

        // Resource limits.
        cmd.arg("--rlimit_as")
            .arg((self.config.memory_limit_mb * 1024).to_string());

        cmd.arg("--rlimit_cpu")
            .arg(self.config.time_limit_seconds.to_string());

        cmd.arg("--rlimit_nproc")
            .arg(self.config.max_pids.to_string());

        cmd.arg("--time_limit")
            .arg(self.config.time_limit_seconds.to_string());

        // Network isolation.
        // By default, nsjail creates a network namespace (isolated).
        // To allow network access, we disable the network namespace.
        if self.config.allow_network {
            cmd.arg("--disable_clone_newnet");
        }

        // Filesystem isolation.
        cmd.arg("--chroot").arg("/");
        cmd.arg("--proc_rw");

        // Command to execute.
        cmd.arg("--");
        cmd.arg(command);
        cmd.args(args);

        // Setup stdio pipes.
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| Error::SpawnFailed { source: e })?;

        // Write stdin data if provided.
        if let Some(data) = stdin_data {
            use tokio::io::AsyncWriteExt;
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(data.as_bytes())
                    .await
                    .map_err(|e| Error::WriteStdin { source: e })?;
            }
        }

        // Capture stdout.
        let stdout_reader =
            BufReader::new(child.stdout.take().ok_or_else(|| Error::CaptureStdout)?);

        // Capture stderr.
        let stderr_reader =
            BufReader::new(child.stderr.take().ok_or_else(|| Error::CaptureStderr)?);

        let mut stdout_lines = stdout_reader.lines();
        let mut stderr_lines = stderr_reader.lines();

        // Read stdout in background.
        let stdout_task = tokio::spawn(async move {
            let mut output = String::new();
            while let Ok(Some(line)) = stdout_lines.next_line().await {
                output.push_str(&line);
                output.push('\n');
            }
            output
        });

        // Read stderr in background.
        let stderr_task = tokio::spawn(async move {
            let mut output = String::new();
            while let Ok(Some(line)) = stderr_lines.next_line().await {
                output.push_str(&line);
                output.push('\n');
            }
            output
        });

        // Wait for process to complete.
        let status = child
            .wait()
            .await
            .map_err(|e| Error::WaitFailed { source: e })?;

        // Collect outputs.
        let stdout = stdout_task
            .await
            .map_err(|e| Error::JoinStdoutTask { source: e })?;

        let stderr = stderr_task
            .await
            .map_err(|e| Error::JoinStderrTask { source: e })?;

        let exit_code = match status.code() {
            Some(code) => code,
            None => return Err(Error::TerminatedBySignal),
        };

        let success = status.success();

        Ok(SandboxResult {
            stdout,
            stderr,
            exit_code,
            success,
        })
    }
}
