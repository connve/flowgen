use super::subscriber;
use async_nats::jetstream::{context::Publish, kv, stream};
use flowgen_core::client::Client;
use flowgen_salesforce::eventbus::v1::FetchResponse;
use flowgen_salesforce::eventbus::v1::{FetchRequest, TopicInfo};
use futures::future::TryJoinAll;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, event, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse config file")]
    ParseConfig(#[source] toml::de::Error),
    #[error("Cannot setup Flowgen Client")]
    FlowgenService(#[source] flowgen_core::service::Error),
    #[error("Cannot auth to Salesforce using provided credentials")]
    FlowgenSalesforceAuth(#[source] flowgen_salesforce::auth::Error),
    #[error("Cannot setup Salesforce PubSub context")]
    FlowgenSalesforcePubSub(#[source] flowgen_salesforce::pubsub::Error),
    #[error("Cannot establish connection with NATS server")]
    NatsConnect(#[source] async_nats::ConnectError),
    #[error("Cannot create stream with provided config")]
    NatsCreateStream(#[source] async_nats::jetstream::context::CreateStreamError),
    #[error("Cannot create key value store with provided config")]
    NatsCreateKeyValue(#[source] async_nats::jetstream::context::CreateKeyValueError),
    #[error("There was an error with putting kv into the store")]
    NatsPutKeyValue(#[source] async_nats::jetstream::kv::PutError),
    #[error("There was an error with async_nats publish")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Cannot execute async task")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with subscriber")]
    Subscriber(#[source] subscriber::Error),
    #[error("There was an error with bincode serialization / deserialization")]
    Bincode(#[source] Box<bincode::ErrorKind>),
}

pub enum ChannelMessage {
    FetchResponse(FetchResponse),
    TopicInfo(TopicInfo),
}

#[derive(Deserialize)]
pub struct Salesforce {
    pub credentials: String,
    pub topic_list: Vec<String>,
}

#[derive(Deserialize)]
pub struct Nats {
    pub credentials: String,
    pub host: String,
    pub stream_name: String,
    pub stream_description: Option<String>,
    pub subjects: Vec<String>,
    pub kv_bucket_name: String,
    pub kv_bucket_description: String,
}

#[derive(Deserialize)]
pub struct Service {
    pub salesforce: Salesforce,
    pub nats: Nats,
}

impl Service {
    pub async fn run(self) -> Result<(), Error> {
        // Setup Flowgen client.
        let flowgen = flowgen_core::service::Builder::new()
            .with_endpoint(format!("{0}:443", flowgen_salesforce::eventbus::ENDPOINT))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        // Connect to Salesforce and get token response.
        let sfdc_client = flowgen_salesforce::auth::Builder::new()
            .with_credentials_path(self.salesforce.credentials.into())
            .build()
            .map_err(Error::FlowgenSalesforceAuth)?
            .connect()
            .await
            .map_err(Error::FlowgenSalesforceAuth)?;

        // Get PubSub context.
        let pubsub = flowgen_salesforce::pubsub::Builder::new(flowgen)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::FlowgenSalesforcePubSub)?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        // Setup NATS client and stream.
        // let nats_seed = fs::read_to_string(nats_credentials).await?;
        // let nats_client = async_nats::ConnectOptions::with_nkey(nats_seed)
        //     .connect(nats_host)
        //     .await?;
        let nats_client = async_nats::connect(self.nats.host)
            .await
            .map_err(Error::NatsConnect)?;
        let nats_jetstream = async_nats::jetstream::new(nats_client);

        let stream_config = stream::Config {
            name: self.nats.stream_name.clone(),
            retention: stream::RetentionPolicy::Limits,
            max_age: Duration::new(60 * 60 * 24 * 7, 0),
            subjects: self.nats.subjects.clone(),
            description: self.nats.stream_description.clone(),
            ..Default::default()
        };

        if (nats_jetstream.create_stream(stream_config.clone()).await).is_err() {
            nats_jetstream
                .update_stream(stream_config)
                .await
                .map_err(Error::NatsCreateStream)?;
        };

        let kv = nats_jetstream
            .create_key_value(kv::Config {
                bucket: self.nats.kv_bucket_name,
                description: self.nats.kv_bucket_description,
                ..Default::default()
            })
            .await
            .map_err(Error::NatsCreateKeyValue)?;

        let (tx, mut rx) = mpsc::channel(200);
        let mut async_task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        // Subscribe to all topics from the config.
        for topic in self.salesforce.topic_list {
            let pubsub = Arc::clone(&pubsub);
            let tx = Sender::clone(&tx);
            let topic = topic.clone();

            let subscribe_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                subscriber::Builder::new(pubsub, tx)
                    .build()
                    .map_err(Error::Subscriber)?
                    .subscribe(FetchRequest {
                        topic_name: topic,
                        num_requested: 200,
                        ..Default::default()
                    })
                    .await
                    .map_err(Error::Subscriber)?;

                Ok(())
            });

            async_task_list.push(subscribe_task);
        }

        let mut topic_info_list: Vec<TopicInfo> = Vec::new();

        // Process events.
        let receive_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            while let Some(cm) = rx.recv().await {
                match cm {
                    subscriber::ChannelMessage::FetchResponse(fr) => {
                        event!(name: "fetch_response", Level::INFO, rpc_id = fr.rpc_id);
                        for ce in fr.events {
                            if let Some(pe) = ce.event {
                                // Log event id.
                                event!(name: "event_consumed", Level::INFO, event_id = pe.id);

                                // Get the relevant topic from the list.
                                let topic: Vec<TopicInfo> = topic_info_list
                                    .iter()
                                    .filter(|t| t.schema_id == pe.schema_id)
                                    .cloned()
                                    .collect();

                                // Setup nats subject based on the topic_name.
                                let s = topic[0].topic_name.replace('/', ".").to_lowercase();
                                let subject = &s[1..];

                                // Publish an event.
                                let event: Vec<u8> =
                                    bincode::serialize(&pe).map_err(Error::Bincode)?;

                                nats_jetstream
                                    .send_publish(
                                        subject.to_string(),
                                        Publish::build().payload(event.into()),
                                    )
                                    .await
                                    .map_err(Error::NatsPublish)?;
                            }
                        }
                    }
                    subscriber::ChannelMessage::TopicInfo(t) => {
                        let t_bytes: Vec<u8> = bincode::serialize(&t).unwrap();
                        kv.put(t.topic_name.clone(), t_bytes.into())
                            .await
                            .map_err(Error::NatsPutKeyValue)?;
                        topic_info_list.push(t);
                    }
                }
            }
            Ok(())
        });
        async_task_list.push(receive_task);

        // Handle all async tasks.
        async_task_list
            .into_iter()
            .collect::<TryJoinAll<_>>()
            .await
            .map_err(Error::TokioJoin)?;

        Ok(())
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
    pub fn build(&mut self) -> Result<Service, Error> {
        let c = std::fs::read_to_string(&self.config_path)
            .map_err(|e| Error::OpenFile(e, self.config_path.clone()))?;
        let service: Service = toml::from_str(&c).map_err(Error::ParseConfig)?;
        Ok(service)
    }
}
