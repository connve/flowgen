use super::config;
use flowgen_core::client::Client;
use std::path::PathBuf;
use tracing::error;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse config file")]
    ParseConfig(#[source] toml::de::Error),
    #[error("Cannot setup Flowgen Client")]
    FlowgenService(#[source] flowgen_core::service::Error),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenSalesforcePubSubSubscriberError(#[source] flowgen_salesforce::pubsub::subscriber::Error),
    #[error("Failed to setup Nats JetStream as flow target.")]
    FlowgenNatsJetStreamContext(#[source] flowgen_nats::jetstream::context::Error),
}

#[allow(non_camel_case_types)]
pub enum Source {
    salesforce_pubsub(flowgen_salesforce::pubsub::subscriber::Subscriber),
}

#[allow(non_camel_case_types)]
pub enum Target {
    nats_jetstream(flowgen_nats::jetstream::context::Context),
}

pub struct Flow {
    config: config::Config,
    pub source: Option<Source>,
    pub target: Option<Target>,
}

impl Flow {
    pub async fn init(mut self) -> Result<Self, Error> {
        // Setup Flowgen service.
        let service = flowgen_core::service::Builder::new()
            .with_endpoint(format!(
                "{0}:443",
                flowgen_salesforce::pubsub::eventbus::ENDPOINT
            ))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        // Get cloned version of the config.
        let config = self.config.clone();

        // Setup source subscribers.
        match config.flow.source {
            config::Source::salesforce_pubsub(config) => {
                let subscriber =
                    flowgen_salesforce::pubsub::subscriber::Builder::new(service.clone(), config)
                        .build()
                        .await
                        .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?;
                self.source = Some(Source::salesforce_pubsub(subscriber));
            }
        }

        // Setup target publishers.
        match config.flow.target {
            config::Target::nats_jetstream(config) => {
                let publisher = flowgen_nats::jetstream::context::Builder::new(config)
                    .build()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamContext)?;
                self.target = Some(Target::nats_jetstream(publisher));
            }
        }
        Ok(self)
    }
}

#[derive(Default)]
pub struct Builder {
    config_path: PathBuf,
}

impl Builder {
    pub fn new(config_path: PathBuf) -> Builder {
        Builder { config_path }
    }
    pub fn build(&mut self) -> Result<Flow, Error> {
        let c = std::fs::read_to_string(&self.config_path)
            .map_err(|e| Error::OpenFile(e, self.config_path.clone()))?;
        let config: config::Config = toml::from_str(&c).map_err(Error::ParseConfig)?;
        let f = Flow {
            config,
            source: None,
            target: None,
        };
        Ok(f)
    }
}
