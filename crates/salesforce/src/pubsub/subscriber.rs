use flowgen_core::{
    client::Client,
    event::{Event, EventBuilder},
    recordbatch::RecordBatchExt,
};
use futures_util::future::TryJoinAll;
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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with PubSub context")]
    SalesforcePubSub(#[source] super::context::Error),
    #[error("error with Salesforce authentication")]
    SalesforceAuth(#[source] crate::client::Error),
    #[error("error constructing event")]
    Event(#[source] flowgen_core::event::Error),
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
    RecordBatch(#[source] flowgen_core::recordbatch::Error),
}

pub struct Subscriber {
    handle_list: Vec<JoinHandle<Result<(), Error>>>,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        tokio::spawn(async move {
            let _ = self
                .handle_list
                .into_iter()
                .collect::<TryJoinAll<_>>()
                .await
                .map_err(Error::TaskJoin);
        });
        event!(Level::INFO, "event: subscribed");
        Ok(())
    }
}

pub struct Builder {
    service: flowgen_core::service::Service,
    config: super::config::Source,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(
        service: flowgen_core::service::Service,
        config: super::config::Source,
        tx: &Sender<Event>,
        current_task_id: usize,
    ) -> Builder {
        Builder {
            service,
            config,
            tx: tx.clone(),
            current_task_id,
        }
    }

    /// Builds a new FlowgenSalesforcePubsub Subscriber.
    pub async fn build(self) -> Result<Subscriber, Error> {
        // Connect to Salesforce.
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        // Get PubSub context.
        let pubsub = super::context::Builder::new(self.service)
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

                                    tx.send(e).map_err(Error::SendMessage)?;
                                }
                            }
                        }
                        Err(e) => {
                            return Err(Error::SalesforcePubSub(super::context::Error::RPCFailed(
                                e,
                            )));
                        }
                    }
                }
                Ok(())
            });
            handle_list.push(handle);
        }

        Ok(Subscriber { handle_list })
    }
}
