use flowgen_google::storage::v2::{storage_client::StorageClient, ListBucketsRequest};
use gcp_auth::{CustomServiceAccount, TokenProvider};
use std::env;
use std::path::PathBuf;
use tonic::{metadata::AsciiMetadataValue, Request};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read required env vars.
    let project_id = env::var("PROJECT_ID").map_err(|_| "PROJECT_ID is required".to_string())?;
    let gcp_credentials =
        env::var("GCP_CREDENTIALS").map_err(|_| "GCP_CREDENTIALS are required.".to_string())?;

    // Setup Flowgen client.
    let flowgen = flowgen::service::Builder::new()
        .with_endpoint(format!("{0}:443", flowgen_google::storage::ENDPOINT))
        .build()?
        .connect()
        .await?;

    // Authenticate do GCP Cloud.
    let credentials_path = PathBuf::from(gcp_credentials);
    let service_account = CustomServiceAccount::from_file(credentials_path)?;
    let scopes = &["https://www.googleapis.com/auth/cloud-platform"];
    let token = service_account.token(scopes).await?;

    // Setup required request headers.
    let bearer_token = format!("Bearer {:?}", token.as_str());
    let auth_header: AsciiMetadataValue = bearer_token.parse()?;
    let project_path = format!("projects/{0}", project_id);
    let x_goog: AsciiMetadataValue = format!("project={0}", project_path).parse()?;

    // Setup Storage Client.
    let mut client = StorageClient::with_interceptor(
        flowgen.channel.to_owned().unwrap(),
        move |mut req: Request<()>| {
            req.metadata_mut()
                .insert("authorization", auth_header.clone());
            req.metadata_mut()
                .insert("x-goog-request-params", x_goog.clone());
            Ok(req)
        },
    );

    // List all buckets in the project.
    let list_buckets_resp = client
        .list_buckets(Request::new(ListBucketsRequest {
            parent: project_path,
            ..Default::default()
        }))
        .await?;

    println!("RESPONSE={:?}", list_buckets_resp);

    Ok(())
}
