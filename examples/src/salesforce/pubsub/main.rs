use flowgen_salesforce::eventbus::v1::{SchemaRequest, TopicRequest};
use std::env;

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

    Ok(())
}
