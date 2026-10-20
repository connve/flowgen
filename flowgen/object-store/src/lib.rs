use flowgen_core::task::context::TaskContext;
use flowgen_core::task::meta::{is_per_event, Connection, Connector, Icon};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::{Task, TaskError, TaskIo, TaskMeta};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

pub mod client;
pub mod config;
pub mod list;
pub mod r#move;
pub mod read;
pub mod write;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "object-store",
    label: "Object Store",
    icon: Icon::Svg(include_str!("../icons/object-store.svg")),
    describe_credentials: None,
};

/// The bucket a path is in, such as `s3://data`; `None` for a local path.
fn bucket(path: &Path) -> Option<String> {
    match url::Url::parse(&path.to_string_lossy()) {
        Ok(url) => url
            .host_str()
            .map(|host| format!("{}://{host}", url.scheme())),
        Err(_) => None,
    }
}

impl TaskMeta for config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;

    fn connections(&self) -> Vec<Connection> {
        let mut connection = Connection::new(CONNECTOR, self.credentials_path.as_deref());
        let mut buckets = BTreeSet::new();
        for path in [&self.path, &self.source, &self.destination]
            .into_iter()
            .flatten()
        {
            match bucket(path) {
                Some(bucket) if !is_per_event(&bucket) => {
                    buckets.insert(bucket);
                }
                _ => connection.per_event |= is_per_event(&path.to_string_lossy()),
            }
        }
        connection.target = match buckets.is_empty() {
            true => None,
            false => Some(buckets.into_iter().collect::<Vec<_>>().join(" · ")),
        };
        vec![connection]
    }
}

impl Task for config::Processor {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let operation = self.operation.clone();
        let config = Arc::new(self);
        match operation {
            config::Operation::Read => {
                let mut builder = read::ReadProcessorBuilder::new()
                    .config(config)
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
            }
            config::Operation::Write => {
                let mut builder = write::WriteProcessorBuilder::new()
                    .config(config)
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
            }
            config::Operation::List => {
                let mut builder = list::ListProcessorBuilder::new()
                    .config(config)
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
            }
            config::Operation::Move => {
                let mut builder = r#move::MoveProcessorBuilder::new()
                    .config(config)
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
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_show_with_their_scheme_once() {
        let config: config::Processor = serde_json::from_value(serde_json::json!({
            "name": "a",
            "operation": "move",
            "source": "s3://data/in/*.csv",
            "destination": "s3://data/out/",
        }))
        .unwrap();

        let connections = config.connections();

        assert_eq!(connections[0].target.as_deref(), Some("s3://data"));
    }
}
