use async_nats::jetstream::context::Publish;
use chrono::Utc;
use flowgen::flow;
use flowgen_salesforce::pubsub::eventbus::v1::TopicInfo;
use futures::future::try_join_all;
use futures::future::TryJoinAll;
use glob::glob;
use std::env;
use std::process;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::event;
use tracing::Level;

pub const DEFAULT_TOPIC_NAME: &str = "/data/ChangeEvents";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error deserializing data into binary format.")]
    Bincode(#[source] bincode::Error),
    #[error("Failed to publish message in Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Cannot execute async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenSalesforcePubSubSubscriberError(#[source] flowgen_salesforce::pubsub::subscriber::Error),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenFileSubscriberError(#[source] flowgen_file::subscriber::Error),
}

#[tokio::main]
async fn main() {
    // Install global log collector.
    tracing_subscriber::fmt::init();

    // Setup environment variables
    let config_dir = env::var("CONFIG_DIR").expect("env variable CONFIG_DIR should be set");

    // Run Flowgen service for each of individual configs.
    for config in glob(&config_dir).unwrap_or_else(|err| {
        error!("{:?}", err);
        process::exit(1);
    }) {
        let config_path = config.unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        });
        let f = flowgen::flow::Builder::new(config_path)
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
        })
    }
}

// Run an event relay service.
async fn run(f: flowgen::flow::Flow) -> Result<(), Error> {
    if let Some(source) = f.source {
        match source {
            flow::Source::salesforce_pubsub(source) => {
                let subscriber_task_list = source
                    .init()
                    .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?;
                let mut rx = source.rx;

                let mut topic_info_list: Vec<TopicInfo> = Vec::new();
                let receiver_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Some(cm) = rx.recv().await {
                        match cm {
                        flowgen_salesforce::pubsub::subscriber::ChannelMessage::FetchResponse(
                            fr,
                        ) => {
                            event!(name: "fetch_response", Level::INFO, rpc_id = fr.rpc_id);
                            for ce in fr.events {
                                if let Some(pe) = ce.event {

                                    // Relay events only if event_id is present.
                                    if !pe.id.is_empty() {
                                    event!(name: "event_consumed", Level::INFO, event_id = pe.id);
                                    // Get the relevant topic from the list.
                                    let mut topic_list: Vec<TopicInfo> = topic_info_list
                                        .iter()
                                        .filter(|t| t.schema_id == pe.schema_id)
                                        .cloned()
                                        .collect();


                                    // Use a default all change events topic if no schema_id is to be found.
                                    if topic_list.is_empty() {
                                         topic_list = topic_info_list
                                        .iter()
                                        .filter(|t| t.topic_name == DEFAULT_TOPIC_NAME)
                                        .cloned()
                                        .collect();
                                    }

                                    // Break if default topic was not setup.
                                    if topic_list.is_empty() {
                                        break;
                                    }

                                    // Setup nats subject and payload.
                                    let s = topic_list[0].topic_name.replace('/', ".").to_lowercase();
                                    let event_name = &s[1..];
                                    let subject = format!("salesforce.pubsub.in.{en}.{eid}", en = event_name, eid = pe.id);
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

                //Run all receiver tasks.
                try_join_all(vec![receiver_task])
                    .await
                    .map_err(Error::TokioJoin)?;
            }
            flow::Source::file(source) => {
                let mut subscriber = source.init().map_err(Error::FlowgenFileSubscriberError)?;
                subscriber
                    .subscribe()
                    .await
                    .map_err(Error::FlowgenFileSubscriberError)?;

                let mut rx = subscriber.rx;
                let path = subscriber.path.clone();

                let receiver_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Some(m) = rx.recv().await {
                        // Setup nats subject and payload.
                        let filename = match path.split("/").last() {
                            Some(filename) => filename,
                            None => break,
                        };

                        let timestamp = Utc::now().timestamp();
                        let subject = format!(
                            "filedrop.in.{filename}.{timestamp}",
                            filename = filename,
                            timestamp = timestamp
                        );

                        // Publish an event.
                        if let Some(target) = f.target.as_ref() {
                            match target {
                                flow::Target::nats_jetstream(context) => {
                                    context
                                        .jetstream
                                        .send_publish(subject, Publish::build().payload(m.into()))
                                        .await
                                        .map_err(Error::NatsPublish)?
                                        .await
                                        .map_err(Error::NatsPublish)?;

                                    rx.close();
                                }
                            }
                        }
                    }
                    Ok(())
                });

                //Run all receiver tasks.
                try_join_all(vec![receiver_task])
                    .await
                    .map_err(Error::TokioJoin)?;
            }
            flow::Source::gcp_storage(source) => {
                let mut subscriber = source.init().unwrap();
                subscriber.subscribe().await.unwrap();

                let mut rx = subscriber.rx;

                let receiver_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Some(m) = rx.recv().await {
                        println!("{:?}", m)
                    }
                    Ok(())
                });

                //Run all receiver tasks.
                try_join_all(vec![receiver_task])
                    .await
                    .map_err(Error::TokioJoin)?;
            }
        }
    }

    Ok(())
}
