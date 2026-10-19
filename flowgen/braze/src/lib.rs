//! Braze tasks: exporting user profiles, and sending messages, campaigns,
//! and Canvases through the Braze REST API.

#![deny(missing_docs)]

use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};

pub mod export_user_data;
pub mod messaging;
pub mod processor;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "braze",
    label: "Braze",
    icon: Icon::Png(include_bytes!("../icons/braze.png")),
    describe_credentials: None,
};

macro_rules! braze_task_meta {
    ($($config:ty),+) => {
        $(
            impl TaskMeta for $config {
                const CONNECTOR: Connector = CONNECTOR;

                fn connections(&self) -> Vec<Connection> {
                    vec![Connection::new(CONNECTOR, Some(&self.credentials_path))
                        .target(&[&self.base_url])]
                }
            }
        )+
    };
}

braze_task_meta!(export_user_data::ExportUserData, messaging::Messaging);
