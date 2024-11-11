use super::eventbus::v1::{FetchRequest, FetchResponse, TopicInfo, TopicRequest};
use flowgen_core::client::Client;
use futures_util::future::TryJoinAll;
use std::sync::Arc;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context")]
    FlowgenSalesforcePubSub(#[source] super::context::Error),
    #[error("Cannot auth to Salesforce using provided credentials")]
    FlowgenSalesforceAuth(#[source] crate::client::Error),
    #[error("There was an error with sending ChannelMessage")]
    TokioSendChannelMessage(#[source] tokio::sync::mpsc::error::SendError<ChannelMessage>),
    #[error("Cannot execute async task")]
    TokioJoin(#[source] tokio::task::JoinError),
}
pub enum ChannelMessage {
    FetchResponse(FetchResponse),
    TopicInfo(TopicInfo),
}

pub struct Subscriber {
    topic_list: Vec<String>,
    pub pubsub: Arc<Mutex<super::context::Context>>,
    pub rx: Receiver<ChannelMessage>,
    pub tx: Sender<ChannelMessage>,
}

impl Subscriber {
    pub fn subscribe(&self) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
        let mut async_task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        // Subscribe to all topics from the config.
        for topic in self.topic_list.iter() {
            let pubsub = Arc::clone(&self.pubsub);
            let tx = Sender::clone(&self.tx);
            let topic = topic.clone();

            let subscribe_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let topic_info: TopicInfo = pubsub
                    .lock()
                    .await
                    .get_topic(TopicRequest {
                        topic_name: topic.clone(),
                    })
                    .await
                    .map_err(Error::FlowgenSalesforcePubSub)?
                    .into_inner();

                tx.send(ChannelMessage::TopicInfo(topic_info))
                    .await
                    .map_err(Error::TokioSendChannelMessage)?;

                let mut stream = pubsub
                    .lock()
                    .await
                    .subscribe(FetchRequest {
                        topic_name: topic,
                        num_requested: 200,
                        ..Default::default()
                    })
                    .await
                    .map_err(Error::FlowgenSalesforcePubSub)?
                    .into_inner();

                while let Some(received) = stream.next().await {
                    match received {
                        Ok(fr) => {
                            tx.send(ChannelMessage::FetchResponse(fr))
                                .await
                                .map_err(Error::TokioSendChannelMessage)?;
                        }
                        Err(e) => {
                            return Err(Error::FlowgenSalesforcePubSub(
                                super::context::Error::RPCFailed(e),
                            ));
                        }
                    }
                }
                Ok(())
            });
            async_task_list.push(subscribe_task);
        }

        // // Run all async tasks.
        // async_task_list
        //     .into_iter()
        //     .collect::<TryJoinAll<_>>()
        //     .await
        //     .map_err(Error::TokioJoin)?;

        Ok(async_task_list)
    }
}

pub struct Builder {
    flowgen_service: flowgen_core::service::Service,
    source_config: super::config::Source,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(
        flowgen_service: flowgen_core::service::Service,
        source_config: super::config::Source,
    ) -> Builder {
        Builder {
            flowgen_service,
            source_config,
        }
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        // Connect to Salesforce.
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(self.source_config.credentials.into())
            .build()
            .map_err(Error::FlowgenSalesforceAuth)?
            .connect()
            .await
            .map_err(Error::FlowgenSalesforceAuth)?;

        // Get PubSub context.
        let pubsub = super::context::Builder::new(self.flowgen_service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::FlowgenSalesforcePubSub)?;

        let pubsub = Arc::new(Mutex::new(pubsub));
        let (tx, rx) = tokio::sync::mpsc::channel(200);
        let s = Subscriber {
            pubsub,
            rx,
            tx,
            topic_list: self.source_config.topic_list,
        };

        Ok(s)
    }
}
