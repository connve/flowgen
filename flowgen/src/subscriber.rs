use flowgen_salesforce::eventbus::v1::{FetchRequest, FetchResponse, TopicInfo, TopicRequest};
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, Mutex};
use tokio_stream::StreamExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context")]
    FlowgenSalesforcePubSub(#[source] flowgen_salesforce::pubsub::Error),
    #[error("There was an error with sending ChannelMessage")]
    TokioSendChannelMessage(#[source] tokio::sync::mpsc::error::SendError<ChannelMessage>),
    #[error("Cannot execute async task")]
    TokioJoin(#[source] tokio::task::JoinError),
}
pub enum ChannelMessage {
    FetchResponse(FetchResponse),
    TopicInfo(TopicInfo),
}

#[derive(Debug)]
pub struct Subscriber {
    pubsub: Arc<Mutex<flowgen_salesforce::pubsub::Context>>,
    tx: Sender<ChannelMessage>,
}

impl Subscriber {
    pub async fn subscribe(&self, request: FetchRequest) -> Result<(), Error> {
        let topic = request.topic_name.clone();
        let topic_info: TopicInfo = self
            .pubsub
            .lock()
            .await
            .get_topic(TopicRequest { topic_name: topic })
            .await
            .map_err(Error::FlowgenSalesforcePubSub)?
            .into_inner();

        self.tx
            .send(ChannelMessage::TopicInfo(topic_info))
            .await
            .map_err(Error::TokioSendChannelMessage)?;

        let mut stream = self
            .pubsub
            .lock()
            .await
            .subscribe(request)
            .await
            .map_err(Error::FlowgenSalesforcePubSub)?
            .into_inner();

        let tx = self.tx.clone();
        let subscribe_handle = tokio::spawn(async move {
            while let Some(received) = stream.next().await {
                match received {
                    Ok(fr) => {
                        tx.send(ChannelMessage::FetchResponse(fr))
                            .await
                            .map_err(Error::TokioSendChannelMessage)?;
                    }
                    Err(e) => {
                        return Err(Error::FlowgenSalesforcePubSub(
                            flowgen_salesforce::pubsub::Error::RPCFailed(e),
                        ));
                    }
                }
            }
            Ok(())
        });

        let _ = subscribe_handle.await.map_err(Error::TokioJoin)?;
        Ok(())
    }
}

pub struct Builder {
    pubsub: Arc<Mutex<flowgen_salesforce::pubsub::Context>>,
    tx: Sender<ChannelMessage>,
    nats_kv_store: Option<async_nats::jetstream::kv::Store>,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(
        pubsub: Arc<Mutex<flowgen_salesforce::pubsub::Context>>,
        tx: Sender<ChannelMessage>,
    ) -> Builder {
        Builder {
            pubsub,
            tx,
            nats_kv_store: None,
        }
    }
    // Pass Nats KV Store so that the recent replay_id will be stored.
    pub fn with_nats_kv_store(
        &mut self,
        nats_kv_store: async_nats::jetstream::kv::Store,
    ) -> &mut Builder {
        self.nats_kv_store = Some(nats_kv_store);
        self
    }

    pub fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            pubsub: self.pubsub,
            tx: self.tx,
        })
    }
}
