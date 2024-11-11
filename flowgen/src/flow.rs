use super::config;
use async_nats::jetstream::{context::Publish, kv, stream};
use flowgen_core::client::Client;
use serde::Deserialize;
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
    // #[error("Cannot auth to Salesforce using provided credentials")]
    // FlowgenSalesforceAuth(#[source] flowgen_salesforce::client::Error),
    // #[error("Cannot setup Salesforce PubSub context")]
    // FlowgenSalesforcePubSub(#[source] flowgen_salesforce::pubsub::context::Error),
    // #[error("Cannot establish connection with NATS server")]
    // NatsConnect(#[source] async_nats::ConnectError),
    // #[error("Cannot create stream with provided config")]
    // NatsCreateStream(#[source] async_nats::jetstream::context::CreateStreamError),
    // #[error("Cannot create key value store with provided config")]
    // NatsCreateKeyValue(#[source] async_nats::jetstream::context::CreateKeyValueError),
    // #[error("There was an error with putting kv into the store")]
    // NatsPutKeyValue(#[source] async_nats::jetstream::kv::PutError),
    // #[error("There was an error with async_nats publish")]
    // NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    // #[error("Cannot execute async task")]
    // TokioJoin(#[source] tokio::task::JoinError),
    // #[error("There was an error with subscriber")]
    // Subscriber(#[source] subscriber::Error),
    // #[error("There was an error with bincode serialization / deserialization")]
    // Bincode(#[source] Box<bincode::ErrorKind>),
}

#[allow(non_camel_case_types)]
pub enum Source {
    salesforce_pubsub(flowgen_salesforce::pubsub::subscriber::Subscriber),
}

pub struct Flow {
    config: config::Config,
    pub source: Option<Source>,
}

impl Flow {
    pub async fn run(mut self) -> Result<Self, Error> {
        // Setup Flowgen service.
        let flowgen_service = flowgen_core::service::Builder::new()
            .with_endpoint(format!(
                "{0}:443",
                flowgen_salesforce::pubsub::eventbus::ENDPOINT
            ))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        let config = self.config.clone();
        let config::Source::salesforce_pubsub(source_config) = config.flow.source;
        let subscriber =
            flowgen_salesforce::pubsub::subscriber::Builder::new(flowgen_service, source_config)
                .build()
                .await
                .unwrap();
        self.source = Some(Source::salesforce_pubsub(subscriber));

        // Setup NATS client and stream.
        // let nats_seed = fs::read_to_string(nats_credentials).await?;
        // let nats_client = async_nats::ConnectOptions::with_nkey(nats_seed)
        //     .connect(nats_host)
        //     .await?;
        // let config::Target::nats(target_config) = self.config.target;
        // let nats_client = async_nats::connect(target_config.host)
        //     .await
        //     .map_err(Error::NatsConnect)?;
        // let nats_jetstream = async_nats::jetstream::new(nats_client);

        // let stream_config = stream::Config {
        //     name: target_config.stream_name.clone(),
        //     retention: stream::RetentionPolicy::Limits,
        //     max_age: Duration::new(60 * 60 * 24 * 7, 0),
        //     subjects: target_config.subjects.clone(),
        //     description: target_config.stream_description.clone(),
        //     ..Default::default()
        // };

        // if (nats_jetstream.create_stream(stream_config.clone()).await).is_err() {
        //     nats_jetstream
        //         .update_stream(stream_config)
        //         .await
        //         .map_err(Error::NatsCreateStream)?;
        // };

        // let kv = nats_jetstream
        //     .create_key_value(kv::Config {
        //         bucket: target_config.kv_bucket_name,
        //         description: target_config.kv_bucket_description,
        //         ..Default::default()
        //     })
        //     .await
        //     .map_err(Error::NatsCreateKeyValue)?;

        // let mut topic_info_list: Vec<TopicInfo> = Vec::new();

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
        };
        Ok(f)
    }
}
