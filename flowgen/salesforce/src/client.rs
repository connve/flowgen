//! The logged-in Salesforce client every task shares per credentials file.

use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Errors logging in to Salesforce.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to load the Salesforce credentials: {source}")]
    Credentials {
        #[source]
        source: flowgen_core::credentials::Error,
    },
    #[error("Salesforce authentication error: {source}")]
    Auth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// The client registry key of the client for `credentials_path`.
pub fn key(
    task_type: &'static str,
    credentials_path: &Path,
) -> flowgen_core::client_registry::ClientKey {
    flowgen_core::client_registry::ClientKey::new(task_type, &credentials_path)
}

/// The client logged in with the credentials file at `credentials_path`,
/// shared through the client registry.
pub async fn connect(
    task_context: &flowgen_core::task::context::TaskContext,
    task_type: &'static str,
    credentials_path: &Path,
) -> Result<Arc<Mutex<salesforce_core::client::Client>>, Error> {
    task_context
        .client_registry
        .get_or_init(key(task_type, credentials_path), || async {
            let credentials = flowgen_core::credentials::load_credentials(credentials_path)
                .await
                .map_err(|source| Error::Credentials { source })?;
            let client = salesforce_core::client::Builder::new()
                .credentials(credentials)
                .build()
                .map_err(|source| Error::Auth { source })?
                .connect()
                .await
                .map_err(|source| Error::Auth { source })?;
            Ok(Mutex::new(client))
        })
        .await
        .map_err(|error| match error {
            flowgen_core::client_registry::Error::Init { source } => source,
            flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
        })
}
