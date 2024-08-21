use flowgen_salesforce::eventbus::v1::{pub_sub_client::PubSubClient, SchemaRequest, TopicRequest};
use oauth2::TokenResponse;
use std::env;
use tonic::metadata::AsciiMetadataValue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup environment variables.
    let sfdc_credentials = env!("SALESFORCE_CREDENTIALS");
    let sfdc_topic_name = env!("SALESFORCE_TOPIC_NAME");

    // Setup Flowgen client.
    let flowgen_client = flowgen::core::Client::new()
        .with_endpoint(format!(
            "{0}:443",
            flowgen_salesforce::eventbus::GLOBAL_ENDPOINT
        ))
        .build()?
        .connect()
        .await?;

    // Connect to Salesforce and get token response.
    let sfdc_client = flowgen_salesforce::auth::Client::new()
        .with_credentials_path(sfdc_credentials.to_string())
        .build()?;

    // Setup required Salesforce PubSub request metadata.
    let auth_header: AsciiMetadataValue = sfdc_client
        .connect()
        .await?
        .access_token()
        .secret()
        .parse()?;
    let iu: AsciiMetadataValue = sfdc_client.instance_url.parse()?;
    let tid: AsciiMetadataValue = sfdc_client.tenant_id.parse()?;

    // Setup Salesforce grpc client for PubSub.
    let mut sfdc_grpc_client =
        PubSubClient::with_interceptor(flowgen_client, move |mut req: tonic::Request<()>| {
            req.metadata_mut()
                .insert("accesstoken", auth_header.clone());
            req.metadata_mut().insert("instanceurl", iu.clone());
            req.metadata_mut().insert("tenantid", tid.clone());
            Ok(req)
        });

    // Get a concrete PubSub topic.
    let topic_resp = sfdc_grpc_client
        .get_topic(tonic::Request::new(TopicRequest {
            topic_name: String::from(sfdc_topic_name),
        }))
        .await?;

    println!("{:?}", topic_resp);

    // Get PubSub schema info for a provided topic.
    let schema_info = sfdc_grpc_client
        .get_schema(tonic::Request::new(SchemaRequest {
            schema_id: topic_resp.into_inner().schema_id,
        }))
        .await?
        .into_inner();

    println!("{:?}", schema_info);

    Ok(())
}
