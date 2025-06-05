use flowgen_core::{
    connect::client::Client,
    convert::recordbatch::RecordBatchExt,
    stream::event::{Event, EventBuilder},
};
use futures_util::future::try_join_all;
use salesforce_pubsub::eventbus::v1::{FetchRequest, SchemaRequest, TopicRequest};
use serde_json::Value;
use std::sync::Arc;
use tokio::{
    sync::{broadcast::Sender, Mutex},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce.pubsub.in";
const DEFAULT_PUBSUB_URI: &str = "https://api.pubsub.salesforce.com";
const DEFAULT_PUBSUB_PORT: &str = "443";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with PubSub context")]
    SalesforcePubSub(#[source] super::context::Error),
    #[error("error with Salesforce authentication")]
    SalesforceAuth(#[source] crate::client::Error),
    #[error("error constructing event")]
    Event(#[source] flowgen_core::stream::event::Error),
    #[error("error executing async task")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("error parsing value to avro schema")]
    SerdeAvroSchema(#[source] serde_avro_fast::schema::SchemaError),
    #[error("error parsing value to an data entity")]
    SerdeAvroValue(#[source] serde_avro_fast::de::DeError),
    #[error("error with sending message over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error deserializing data into binary format")]
    Bincode(#[source] bincode::Error),
    #[error("error with processing record batch")]
    RecordBatch(#[source] flowgen_core::convert::recordbatch::Error),
    #[error("error setting up flowgen grpc service")]
    Service(#[source] flowgen_core::connect::service::Error),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
}

pub struct Subscriber {
    config: Arc<super::config::Subscriber>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Subscriber {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let service = flowgen_core::connect::service::ServiceBuilder::new()
            .endpoint(format!("{0}:{1}", DEFAULT_PUBSUB_URI, DEFAULT_PUBSUB_PORT))
            .build()
            .map_err(Error::Service)?
            .connect()
            .await
            .map_err(Error::Service)?;

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(self.config.credentials.clone().into())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        let pubsub = super::context::Builder::new(service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::SalesforcePubSub)?;

        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let pubsub = Arc::new(Mutex::new(pubsub));

        for topic in self.config.topic_list.clone().iter() {
            let pubsub: Arc<Mutex<super::context::Context>> = Arc::clone(&pubsub);
            let topic = topic.clone();
            let tx = self.tx.clone();

            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let topic_info = pubsub
                    .lock()
                    .await
                    .get_topic(TopicRequest {
                        topic_name: topic.clone(),
                    })
                    .await
                    .map_err(Error::SalesforcePubSub)?
                    .into_inner();

                let schema_info = pubsub
                    .lock()
                    .await
                    .get_schema(SchemaRequest {
                        schema_id: topic_info.schema_id,
                    })
                    .await
                    .map_err(Error::SalesforcePubSub)?
                    .into_inner();

                let mut stream = pubsub
                    .lock()
                    .await
                    .subscribe(FetchRequest {
                        topic_name: topic,
                        num_requested: 200,
                        ..Default::default()
                    })
                    .await
                    .map_err(Error::SalesforcePubSub)?
                    .into_inner();

                while let Some(received) = stream.next().await {
                    match received {
                        Ok(fr) => {
                            for ce in fr.events {
                                if let Some(event) = ce.event {
                                    let schema: serde_avro_fast::Schema = schema_info
                                        .schema_json
                                        .parse()
                                        .map_err(Error::SerdeAvroSchema)?;

                                    let value = serde_avro_fast::from_datum_slice::<Value>(
                                        &event.payload[..],
                                        &schema,
                                    )
                                    .map_err(Error::SerdeAvroValue)?;

                                    let recordbatch = value
                                        .to_string()
                                        .to_recordbatch()
                                        .map_err(Error::RecordBatch)?;

                                    let topic =
                                        topic_info.topic_name.replace('/', ".").to_lowercase();

                                    let subject = format!(
                                        "{}.{}.{}",
                                        DEFAULT_MESSAGE_SUBJECT,
                                        &topic[1..],
                                        event.id
                                    );

                                    let e = EventBuilder::new()
                                        .data(recordbatch)
                                        .subject(subject)
                                        .current_task_id(self.current_task_id)
                                        .build()
                                        .map_err(Error::Event)?;

                                    event!(Level::INFO, "event processed: {}", e.subject);
                                    tx.send(e).map_err(Error::SendMessage)?;
                                }
                            }
                        }
                        Err(e) => {
                            return Err(Error::SalesforcePubSub(super::context::Error::Tonic(e)));
                        }
                    }
                }
                Ok(())
            });
            handle_list.push(handle);
        }

        let _ = try_join_all(handle_list.iter_mut()).await;
        Ok(())
    }
}

#[derive(Default)]
pub struct SubscriberBuilder {
    config: Option<Arc<super::config::Subscriber>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Subscriber>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
