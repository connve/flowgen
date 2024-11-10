use flowgen_salesforce::pubsub::eventbus::v1::TopicInfo;
use std::env;
use std::process;
use tracing::error;
use tracing::event;
use tracing::Level;

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
        .run()
        .await
        .unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        });

    let topic_info_list: Vec<TopicInfo> = Vec::new();

    let mut rx = f.source.unwrap().rx;
    // Process events.
    // tokio::spawn(async move {
    while let Some(cm) = rx.recv().await {
        match cm {
            flowgen_salesforce::pubsub::subscriber::ChannelMessage::FetchResponse(fr) => {
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
                        // let event: Vec<u8> = bincode::serialize(&pe).map_err(Error::Bincode)?;

                        // nats_jetstream
                        //     .send_publish(
                        //         subject.to_string(),
                        //         Publish::build().payload(event.into()),
                        //     )
                        //     .await
                        //     .map_err(Error::NatsPublish)?;
                    }
                }
            }
            flowgen_salesforce::pubsub::subscriber::ChannelMessage::TopicInfo(t) => {
                println!("{:?}", t);
                // let t_bytes: Vec<u8> = bincode::serialize(&t).unwrap();
                // kv.put(t.topic_name.clone(), t_bytes.into())
                //     .await
                //     .map_err(Error::NatsPutKeyValue)?;
                // topic_info_list.push(t);
            }
        }
    }
    // });
}
