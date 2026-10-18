//! Klaviyo tasks: templates, campaigns, lists, events and profiles.
//!
//! A private API key belongs to one Klaviyo account, so each task talks to
//! the account of its `credentials_path`.

use flowgen_core::task::meta::{Connection, Connector, Icon, TaskMeta};

pub mod campaigns;
pub mod events;
pub mod lists;
pub mod processor;
pub mod profiles;
pub mod templates;

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "klaviyo",
    label: "Klaviyo",
    icon: Icon::Png(include_bytes!("../icons/klaviyo.png")),
    describe_credentials: None,
};

macro_rules! klaviyo_task_meta {
    ($($config:ty),+) => {
        $(
            impl TaskMeta for $config {
                const CONNECTOR: Connector = CONNECTOR;

                fn connections(&self) -> Vec<Connection> {
                    let connection = Connection::new(CONNECTOR, Some(&self.credentials_path));
                    match &self.base_url {
                        Some(base_url) => vec![connection.target(&[base_url])],
                        None => vec![connection],
                    }
                }
            }
        )+
    };
}

klaviyo_task_meta!(
    templates::Templates,
    campaigns::Campaigns,
    lists::Lists,
    events::Events,
    profiles::Profiles
);
