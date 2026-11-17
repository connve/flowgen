//! Git tasks.
//!
//! `git_sync` reads the tip of a branch, on a git server or in a repository
//! on this machine, and emits its files in one event; `git_push` commits the
//! files of an event onto a branch and pushes the commit to a git server or
//! moves the branch in a repository on this machine.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{
    parses, render_env, Connection, Connector, CredentialsField, CredentialsForm, Icon,
};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::path::Path;
use std::sync::Arc;

pub mod push;
/// HTTPS credentials and shallow fetches shared by the git tasks.
pub mod remote;
pub mod sync;
pub mod workspace;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "git",
    label: "Git",
    icon: Icon::Svg(include_str!("../icons/git.svg")),
    describe_credentials: None,
    credentials: &[CredentialsForm {
        label: "Access token",
        fields: &[
            CredentialsField::secret("token", "Token"),
            CredentialsField::text("username", "Username").optional(),
        ],
        check: parses::<remote::Credentials>,
    }],
};

impl TaskMeta for push::GitPush {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn validate(&self) -> Result<(), TaskError> {
        if !self.templated() {
            self.destination()?;
        }
        Ok(())
    }

    fn connections(&self) -> Vec<Connection> {
        connections(&self.repository, self.credentials_path.as_deref())
    }
}

impl TaskMeta for sync::GitSync {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn validate(&self) -> Result<(), TaskError> {
        if !self.templated() {
            self.source()?;
        }
        Ok(())
    }

    fn connections(&self) -> Vec<Connection> {
        connections(&self.repository, self.credentials_path.as_deref())
    }
}

/// A repository on a git server, after `{{env.*}}` templates are rendered, is
/// a connection; one on this machine is not.
fn connections(repository: &str, credentials_path: Option<&Path>) -> Vec<Connection> {
    let repository = match render_env(repository) {
        Some(rendered) => rendered,
        None => repository.to_string(),
    };
    match remote::is_url(&repository) {
        true => vec![Connection::new(CONNECTOR, credentials_path).target(&[&repository])],
        false => Vec::new(),
    }
}

impl Task for push::GitPush {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = push::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for sync::GitSync {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = sync::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repository_url_from_the_environment_is_a_connection() {
        std::env::set_var(
            "GIT_CONNECTIONS_TEST_REPOSITORY",
            "https://git.example.com/a.git",
        );
        let sync: sync::GitSync = serde_json::from_str(
            r#"{"name": "a", "repository": "{{env.GIT_CONNECTIONS_TEST_REPOSITORY}}"}"#,
        )
        .unwrap();
        let push: push::GitPush = serde_json::from_str(
            r#"{"name": "a", "repository": "{{env.GIT_CONNECTIONS_TEST_REPOSITORY}}", "message": "m"}"#,
        )
        .unwrap();
        let local: sync::GitSync =
            serde_json::from_str(r#"{"name": "a", "repository": "."}"#).unwrap();

        for connections in [sync.connections(), push.connections()] {
            assert_eq!(connections.len(), 1);
            assert_eq!(connections[0].target.as_deref(), Some("git.example.com"));
        }
        assert!(local.connections().is_empty());
    }
}
