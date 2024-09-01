use flowgen_salesforce::eventbus::v1::{FetchRequest, SchemaRequest, TopicRequest};
use std::env;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup environment variables.
    let sfdc_credentials = env!("SALESFORCE_CREDENTIALS");
    let sfdc_topic_name = env!("SALESFORCE_TOPIC_NAME");

    // Setup Flowgen client.
    let flowgen = flowgen::core::ServiceBuilder::new()
        .with_endpoint(format!("{0}:443", flowgen_salesforce::eventbus::ENDPOINT))
        .build()?
        .connect()
        .await?;

    // Connect to Salesforce and get token response.
    let sfdc_client = flowgen_salesforce::auth::ClientBuilder::new()
        .with_credentials_path(sfdc_credentials.to_string().into())
        .build()?
        .connect()
        .await?;

    let mut pubsub = flowgen_salesforce::pubsub::ContextBuilder::new(flowgen)
        .with_client(sfdc_client)
        .build()?;

    // Get a concrete PubSub topic.
    let topic = pubsub
        .get_topic(TopicRequest {
            topic_name: sfdc_topic_name.to_string(),
        })
        .await?;

    println!("{:?}", topic);

    // Get PubSub schema info for a provided topic.
    let schema_info = pubsub
        .get_schema(SchemaRequest {
            schema_id: topic.schema_id,
        })
        .await?;

    println!("{:?}", schema_info);

    // Establish stream of messages for a provided topic.
    let mut stream = pubsub
        .subscribe(FetchRequest {
            topic_name: sfdc_topic_name.to_owned(),
            num_requested: 1,
            ..Default::default()
        })
        .await?;

    // Setup channel.
    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(async move {
        while let Some(received) = stream.next().await {
            match received {
                Ok(fr) => {
                    let ce_ops = fr.events.into_iter().next();
                    if let Some(ce) = ce_ops {
                        let pe_ops = ce.event;
                        if let Some(pe) = pe_ops {
                            if let Err(err) = tx.send(pe).await {
                                // error!("{:?}", err);
                            }
                        }
                    }
                }
                Err(err) => {
                    // error!("{:?}", err)
                }
            }
        }
    });

    // Process stream of events.
    while let Some(event) = rx.recv().await {
        println!("{:?}", event);
    }
    Ok(())
}
