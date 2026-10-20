//! The `salesforce_restapi_query` task: SOQL queries through the REST API,
//! following `nextRecordsUrl` through every page and emitting each page.

use crate::processor::{to_json, Call, Emitter, Error, Operation, Output, QueryOperation};
use flowgen_core::config::ConfigExt;
use flowgen_core::task::TaskConfig;
use salesforce_core::restapi::QueryResponse;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Configuration for Salesforce REST API SOQL queries.
///
/// Emits one event per page of results.
///
/// # Examples
///
/// ```yaml
/// salesforce_restapi_query:
///   name: recent_accounts
///   credentials_path: /path/to/salesforce_creds.json
///   query: "SELECT Id, Name FROM Account WHERE LastModifiedDate = TODAY"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// Unique task identifier.
    pub name: String,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SOQL query, inline or from a resource file (supports Handlebars templating).
    pub query: flowgen_core::resource::Source,
    /// Values bound to `:name` in the query as SOQL literals.
    #[serde(default)]
    pub parameters: Option<std::collections::HashMap<String, crate::soql::Parameter>>,
    /// `query` (default) or `queryAll`, which also returns deleted and archived records.
    #[serde(default)]
    pub query_operation: Option<QueryOperation>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Query {}

#[async_trait::async_trait]
impl Operation for Query {
    type Client = salesforce_core::restapi::Client;

    fn client(auth: &salesforce_core::client::Client) -> Result<Self::Client, Error> {
        crate::processor::rest_client(auth)
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    async fn call(
        &self,
        client: &Self::Client,
        call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let soql = call.soql(&self.query, self.parameters.as_ref()).await?;
        let query_all = matches!(self.query_operation, Some(QueryOperation::QueryAll));

        let mut next_records_url: Option<String> = None;
        loop {
            if call.is_cancelled() {
                return Ok(());
            }
            let mut page = call
                .page(|| fetch_page(client, &soql, query_all, next_records_url.as_deref()))
                .await?;
            let next = page.next_records_url.take();
            let num_records = page.records.len();
            emitter
                .emit(Output::json(to_json(&page)?).num_records(num_records))
                .await?;
            next_records_url = match next {
                Some(url) => Some(url),
                None => return Ok(()),
            };
        }
    }
}

/// The first page of the query, or the page at `next_records_url`.
async fn fetch_page(
    client: &salesforce_core::restapi::Client,
    soql: &str,
    query_all: bool,
    next_records_url: Option<&str>,
) -> Result<QueryResponse, Error> {
    let page = match (next_records_url, query_all) {
        (Some(url), _) => client.query_more(url).send().await,
        (None, false) => client.query(soql).send().await,
        (None, true) => client.query_all(soql).send().await,
    };
    page.map_err(|source| Error::Query {
        source: Box::new(source),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowgen_core::resource::Source;

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "recent_accounts",
            "credentials_path": "/creds.json",
            "query": "SELECT Id, Name FROM Account"
        }"#;
        let config: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.query,
            Source::Inline("SELECT Id, Name FROM Account".to_string())
        );
        assert!(config.query_operation.is_none());
    }

    #[test]
    fn config_deser_query_all_from_resource() {
        let json = r#"{
            "name": "deleted_accounts",
            "credentials_path": "/creds.json",
            "query": {"resource": "queries/deleted_accounts.soql"},
            "query_operation": "queryAll"
        }"#;
        let config: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.query,
            Source::Resource {
                resource: "queries/deleted_accounts.soql".to_string()
            }
        );
        assert_eq!(config.query_operation, Some(QueryOperation::QueryAll));
    }

    #[test]
    fn config_deser_rejects_unknown_fields() {
        let json = r#"{
            "name": "typo",
            "credentials_path": "/creds.json",
            "query": "SELECT Id FROM Account",
            "operation": "query"
        }"#;
        assert!(serde_json::from_str::<Query>(json).is_err());
    }

    #[test]
    fn config_deser_missing_query_fails() {
        let json = r#"{
            "name": "missing",
            "credentials_path": "/creds.json"
        }"#;
        assert!(serde_json::from_str::<Query>(json).is_err());
    }
}
