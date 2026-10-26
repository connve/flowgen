//! Salesforce tasks: Pub/Sub API subscribe and publish, Bulk API query jobs,
//! REST API SObject, Composite, SOSL search and SOQL query, SOAP API merge,
//! and Tooling API operations.

use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{host, read_credentials, Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::path::Path;
use std::sync::Arc;

/// Bulk API 2.0 tasks.
pub mod bulkapi {
    pub mod query_job;
}
pub mod processor;
/// Pub/Sub API tasks.
pub mod pubsubapi {
    pub mod publish;
    pub mod subscribe;
}
/// REST API tasks.
pub mod restapi {
    pub mod composite;
    pub mod query;
    pub mod search;
    pub mod sobject;
}
/// SOAP API tasks.
pub mod soapapi {
    pub mod merge;
}
/// Binding `:name` parameters into SOQL queries.
pub mod soql;
pub mod toolingapi;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "salesforce",
    label: "Salesforce",
    icon: Icon::Svg(include_str!("../icons/salesforce.svg")),
    describe_credentials: Some(describe_credentials),
};

/// The host of the org the credentials log in to.
fn describe_credentials(path: &Path) -> Option<String> {
    let credentials: salesforce_core::client::Credentials = read_credentials(path)?;
    Some(host(&credentials.instance_url))
}

macro_rules! salesforce_task_meta {
    ($($config:ty $({ $($meta:tt)* })?),+ $(,)?) => {
        $(
            impl TaskMeta for $config {
                const CONNECTOR: Connector = CONNECTOR;
                $($($meta)*)?

                fn connections(&self) -> Vec<Connection> {
                    vec![Connection::new(CONNECTOR, Some(&self.credentials_path))]
                }
            }
        )+
    };
}

salesforce_task_meta!(
    pubsubapi::subscribe::Subscribe,
    pubsubapi::publish::Publish,
    bulkapi::query_job::QueryJob { const CPU_INTENSIVE: bool = true; },
    restapi::sobject::SObject,
    restapi::composite::Composite,
    restapi::search::Search,
    restapi::query::Query,
    soapapi::merge::Merge,
    toolingapi::Tooling,
);

macro_rules! salesforce_task {
    ($($config:ty),+ $(,)?) => {
        $(
            impl Task for $config {
                async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
                    let mut builder = processor::ProcessorBuilder::new()
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
                    builder.build()?.run().await?;
                    Ok(())
                }
            }
        )+
    };
}

salesforce_task!(
    bulkapi::query_job::QueryJob,
    restapi::sobject::SObject,
    restapi::composite::Composite,
    restapi::search::Search,
    restapi::query::Query,
    soapapi::merge::Merge,
    toolingapi::Tooling,
);

impl Task for pubsubapi::subscribe::Subscribe {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = pubsubapi::subscribe::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build()?.run().await?;
        Ok(())
    }
}

impl Task for pubsubapi::publish::Publish {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = pubsubapi::publish::ProcessorBuilder::new()
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
        builder.build()?.run().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_described_by_their_org_host_only() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"{"client_id": "c", "client_secret": "secret", "instance_url": "https://acme.my.salesforce.com", "tenant_id": "t"}"#,
        )
        .unwrap();

        assert_eq!(
            describe_credentials(file.path()).as_deref(),
            Some("acme.my.salesforce.com")
        );
    }
}
