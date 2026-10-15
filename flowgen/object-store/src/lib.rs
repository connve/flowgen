use flowgen_core::task::meta::{is_per_event, Connection, Connector, Icon, TaskMeta};
use std::collections::BTreeSet;
use std::path::Path;

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
