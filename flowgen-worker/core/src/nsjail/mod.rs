//! NsJail sandbox integration for secure script execution.
//!
//! This module provides sandboxed execution for scripts (Python, Bash, etc.) using nsjail,
//! a lightweight process isolation tool utilizing Linux namespaces, cgroups, and seccomp.
//!
//! # Security Model
//!
//! Script execution can be sandboxed for security. The sandbox parameters can be tuned
//! per-task but sandboxing is recommended for untrusted code.
//!
//! # Default Limits
//!
//! - Memory limit: 512 MB
//! - Time limit: 30 seconds
//! - Max processes: 10
//! - Network access: DISABLED
//! - User/Group: 99999 (nobody/nogroup)
//!
//! # Example Configuration
//!
//! ```yaml
//! script:
//!   name: "transform"
//!   engine: python
//!   code: "..."
//!   sandbox:
//!     memory_limit_mb: 1024
//!     time_limit_seconds: 60
//! ```

pub mod sandbox;

pub use sandbox::{Error, SandboxConfig, SandboxExecutor, SandboxResult};
