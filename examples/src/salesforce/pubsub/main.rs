use flowgen::client::Client;
use flowgen_salesforce::eventbus::v1::{FetchRequest, SchemaRequest, TopicRequest};
use std::env;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup environment variables.
    let sfdc_credentials = env::var("SALESFORCE_CREDENTIALS").unwrap();
    let sfdc_topic_name = env::var("SALESFORCE_TOPIC_NAME").unwrap();

    // Setup Flowgen client.
    let flowgen = flowgen::service::Builder::new()
        .with_endpoint(format!("{0}:443", flowgen_salesforce::eventbus::ENDPOINT))
        .build()?
        .connect()
        .await?;

    // Connect to Salesforce and authenticate.
    let sfdc_client = flowgen_salesforce::auth::Builder::new()
        .with_credentials_path(sfdc_credentials.to_string().into())
        .build()?
        .connect()
        .await?;

    // Get PubSub context.
    let mut pubsub = flowgen_salesforce::pubsub::Builder::new(flowgen)
        .with_client(sfdc_client)
        .build()?;

    // Get a concrete PubSub topic.
    let topic = pubsub
        .get_topic(TopicRequest {
            topic_name: sfdc_topic_name.to_string(),
        })
        .await?
        .into_inner();

    println!("{:?}", topic);

    // Get PubSub schema info for a provided topic.
    let schema_info = pubsub
        .get_schema(SchemaRequest {
            schema_id: topic.schema_id,
        })
        .await?
        .into_inner();

    println!("{:?}", schema_info);

    // Establish stream of messages for a provided topic.
    let mut stream = pubsub
        .subscribe(FetchRequest {
            topic_name: sfdc_topic_name.to_owned(),
            num_requested: 200,
            ..Default::default()
        })
        .await?
        .into_inner();

    // Setup channel.
    let (tx, mut rx) = mpsc::channel(200);
    tokio::spawn(async move {
        while let Some(received) = stream.next().await {
            match received {
                Ok(fr) => {
                    for event in fr.events {
                        if let Err(e) = tx.send(event).await {
                            eprintln!("error sending event: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("error receiving events: {}", e);
                    break;
                }
            }
        }
    });

    // Process stream of events.
    while let Some(ce) = rx.recv().await {
        if let Some(pe) = ce.event {
            println!("{:?}", pe.id);
        }
    }
    Ok(())
}
