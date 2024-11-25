use async_nats::jetstream::context::Publish;
use flowgen::flow;
use flowgen_salesforce::pubsub::eventbus::v1::TopicInfo;
use flowgen_salesforce::pubsub::subscriber;
use futures::future::TryJoinAll;
use std::env;
use std::process;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::event;
use tracing::Level;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error deserializing data into binary format.")]
    Bincode(#[source] bincode::Error),
    #[error("There was an error deserializing data into binary format.")]
    Subscriber(#[source] subscriber::Error),
    #[error("Cannot execute async task")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("Cannot execute async task")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
}

#[tokio::main]
async fn main() {
    // Install global log collector.
    tracing_subscriber::fmt::init();

    // Setup environment variables
    let config_path = env::var("CONFIG_PATH").expect("env variable CONFIG_PATH should be set");

    // Run flowgen service with a provided config.
    let f = flowgen::flow::Builder::new(config_path.into())
        .build()
        .unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        })
        .init()
        .await
        .unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        });

    run(f).await.unwrap_or_else(|err| {
        error!("{:?}", err);
        process::exit(1);
    });
}

async fn run(f: flowgen::flow::Flow) -> Result<(), Error> {
    if let Some(source) = f.source {
        match source {
            flow::Source::salesforce_pubsub(source_subscriber) => {
                let subscriber_task_list = source_subscriber.init().unwrap();
                let mut rx = source_subscriber.rx;

                let mut topic_info_list: Vec<TopicInfo> = Vec::new();
                let receiver_task_list: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Some(cm) = rx.recv().await {
                        match cm {
                        flowgen_salesforce::pubsub::subscriber::ChannelMessage::FetchResponse(
                            fr,
                        ) => {
                            event!(name: "fetch_response", Level::INFO, rpc_id = fr.rpc_id);
                            for ce in fr.events {
                                if let Some(pe) = ce.event {
                                    event!(name: "event_consumed", Level::INFO, event_id = pe.id);
                                    
                                    // Get the relevant topic from the list.
                                    let topic: Vec<TopicInfo> = topic_info_list
                                        .iter()
                                        .filter(|t| t.schema_id == pe.schema_id)
                                        .cloned()
                                        .collect();

                                    // Setup nats subject and payload.
                                    let s = topic[0].topic_name.replace('/', ".").to_lowercase();
                                    let event_name = &s[1..];
                                    let subject = format!("{en}.{eid}", en = event_name, eid = pe.id);
                                    let event: Vec<u8> = bincode::serialize(&pe).map_err(Error::Bincode)?;

                                    // Publish an event.
                                    if let Some(target) = f.target.as_ref()
                                    {
                                        match target {
                                                flow::Target::nats_jetstream(context) => {
                                                    context.jetstream.send_publish(
                                                subject.to_string(),
                                                Publish::build().payload(event.into())).await.map_err(Error::NatsPublish)?;
                                                    }
                                                }
                                    }
                                    
                                }
                            }
                        }
                        flowgen_salesforce::pubsub::subscriber::ChannelMessage::TopicInfo(t) => {
                            event!(name: "topic_info", Level::INFO, rpc_id = t.rpc_id);
                            topic_info_list.push(t);
                        }
                    }
                    }
                    Ok(())
                });

                // Run all subscriber tasks.
                subscriber_task_list
                    .into_iter()
                    .collect::<TryJoinAll<_>>()
                    .await
                    .map_err(Error::TokioJoin)?;

                // Run all receiver tasks.
                let _ = receiver_task_list.await.map_err(Error::TokioJoin)?;
            }
        }
    }

    Ok(())
}
