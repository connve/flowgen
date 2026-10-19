//! Runs the Salesforce query tasks through their public `Processor` against
//! an in-process HTTP server standing in for a Salesforce org, set as the
//! credentials' `instance_url`. Covers result typing, following result
//! pages, retrying a failed page, and binding query parameters. Needs nothing
//! else, so it isn't `#[ignore]`d.

use arrow::array::{Array, BooleanArray, Float64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_core::resource::Source;
use flowgen_core::task::runner::Runner;
use flowgen_salesforce::bulkapi::config::{QueryJob, QueryJobOperation};
use flowgen_salesforce::restapi::config::Query;
use flowgen_salesforce::soql::Parameter;
use salesforce_core::bulkapi::{
    ColumnDelimiter, ConcurrencyMode, ContentType, JobState, QueryJobInfo, QueryOperation,
};
use salesforce_core::restapi::{QueryResponse, SObjectDescribe, SObjectField};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// What the test server answers to one request.
struct Route {
    path: String,
    query: Option<&'static str>,
    headers: Vec<(&'static str, &'static str)>,
    body: String,
    failures: Mutex<usize>,
}

impl Route {
    fn new<T: Serialize>(path: String, body: &T) -> Self {
        Self::text(path, serde_json::to_string(body).unwrap())
    }

    fn text(path: String, body: String) -> Self {
        Self {
            path,
            query: None,
            headers: Vec::new(),
            body,
            failures: Mutex::new(0),
        }
    }

    /// Only answers requests whose query contains `query`.
    fn query(mut self, query: &'static str) -> Self {
        self.query = Some(query);
        self
    }

    fn header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.push((name, value));
        self
    }

    /// Answers 503 to the first `failures` requests.
    fn failing(self, failures: usize) -> Self {
        *self.failures.lock().unwrap() = failures;
        self
    }

    /// Whether this request is one of those answered with 503.
    fn fails_now(&self) -> bool {
        let mut left = self.failures.lock().unwrap();
        match *left {
            0 => false,
            _ => {
                *left -= 1;
                true
            }
        }
    }
}

#[derive(Serialize)]
struct Token {
    access_token: &'static str,
    token_type: &'static str,
}

/// Every request's path and decoded query parameters.
type Log = Arc<Mutex<Vec<(String, HashMap<String, String>)>>>;

/// Serves the token endpoint and `routes`, answering 404 to anything else.
async fn serve(mut routes: Vec<Route>) -> (String, Log) {
    routes.push(Route::new(
        "/services/oauth2/token".to_string(),
        &Token {
            access_token: "token",
            token_type: "Bearer",
        },
    ));
    let log: Log = Arc::default();
    let state = (Arc::new(routes), Arc::clone(&log));
    let app = axum::Router::new()
        .fallback(
            |axum::extract::State((routes, log)): axum::extract::State<(Arc<Vec<Route>>, Log)>,
             uri: axum::http::Uri| async move {
                let query = uri.query().unwrap_or_default();
                log.lock().unwrap().push((
                    uri.path().to_string(),
                    url::form_urlencoded::parse(query.as_bytes())
                        .into_owned()
                        .collect(),
                ));
                let route = routes.iter().find(|route| {
                    route.path == uri.path()
                        && route.query.is_none_or(|expected| query.contains(expected))
                });
                let response = axum::http::Response::builder();
                match route {
                    Some(route) if route.fails_now() => response
                        .status(axum::http::StatusCode::SERVICE_UNAVAILABLE)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                    Some(route) => {
                        let mut response = response.header("content-type", "application/json");
                        for (name, value) in &route.headers {
                            response = response.header(*name, *value);
                        }
                        response
                            .body(axum::body::Body::from(route.body.clone()))
                            .unwrap()
                    }
                    None => response
                        .status(axum::http::StatusCode::NOT_FOUND)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                }
            },
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}"), log)
}

fn api(path: &str) -> String {
    format!(
        "/services/data/v{}{path}",
        salesforce_core::DEFAULT_API_VERSION
    )
}

#[derive(Serialize)]
struct Credentials<'a> {
    client_id: &'a str,
    client_secret: &'a str,
    instance_url: &'a str,
}

/// A credentials file for `instance_url`, kept until the returned handle drops.
fn credentials(instance_url: &str) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        serde_json::to_string(&Credentials {
            client_id: "client",
            client_secret: "secret",
            instance_url,
        })
        .unwrap(),
    )
    .unwrap();
    file
}

fn task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("a".to_string())
            .task_manager(Arc::new(
                flowgen_core::task::manager::TaskManagerBuilder::new()
                    .build()
                    .unwrap(),
            ))
            .cache(Arc::new(flowgen_core::cache::memory::MemoryCache::new()))
            .retry(flowgen_core::retry::RetryConfig {
                max_attempts: Some(3),
                initial_backoff: Duration::from_millis(1),
            })
            .build()
            .unwrap(),
    )
}

fn trigger() -> Event {
    EventBuilder::new()
        .data(EventData::Json(Value::Null))
        .subject("a".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .unwrap()
}

/// Sends one event through `runner` and returns what it emits.
async fn run<R: Runner>(
    runner: R,
    input_tx: mpsc::Sender<Event>,
    mut output: mpsc::Receiver<Event>,
) -> Vec<Event>
where
    R::Error: std::fmt::Debug,
{
    input_tx.send(trigger()).await.unwrap();
    drop(input_tx);
    runner.run().await.unwrap();
    let mut emitted = Vec::new();
    while let Ok(event) = output.try_recv() {
        emitted.push(event);
    }
    emitted
}

fn job(object: &str) -> QueryJobInfo {
    let now = salesforce_core::chrono::Utc::now();
    QueryJobInfo {
        api_version: 68.0,
        column_delimiter: Some(ColumnDelimiter::Semicolon),
        concurrency_mode: ConcurrencyMode::Parallel,
        content_type: ContentType::Csv,
        created_by_id: "005".to_string(),
        created_date: now,
        id: "750".to_string(),
        is_pk_chunking_supported: None,
        job_type: None,
        line_ending: None,
        number_records_processed: None,
        object: object.to_string(),
        operation: QueryOperation::Query,
        retries: None,
        state: JobState::JobComplete,
        system_modstamp: now,
        total_processing_time: None,
    }
}

fn field(name: &str, field_type: &str, relationship: Option<(&str, &str)>) -> SObjectField {
    SObjectField {
        createable: false,
        default_value: None,
        external_id: None,
        label: name.to_string(),
        length: None,
        name: name.to_string(),
        nillable: true,
        picklist_values: Vec::new(),
        reference_to: relationship
            .map(|(_, object)| vec![object.to_string()])
            .unwrap_or_default(),
        relationship_name: relationship.map(|(name, _)| name.to_string()),
        type_: field_type.to_string(),
        unique: None,
        updateable: false,
    }
}

fn describe(name: &str, fields: Vec<SObjectField>) -> SObjectDescribe {
    SObjectDescribe {
        createable: false,
        deletable: false,
        fields,
        key_prefix: None,
        label: name.to_string(),
        label_plural: name.to_string(),
        name: name.to_string(),
        queryable: true,
        record_type_infos: Vec::new(),
        updateable: false,
    }
}

fn bulk_config(credentials: &tempfile::NamedTempFile) -> QueryJob {
    QueryJob {
        name: "results".to_string(),
        operation: QueryJobOperation::GetResults,
        credentials_path: credentials.path().to_path_buf(),
        query: None,
        parameters: None,
        query_operation: None,
        content_type: None,
        column_delimiter: None,
        line_ending: None,
        job_id: Some("750".to_string()),
        batch_size: 10,
        has_header: true,
        depends_on: None,
        retry: None,
        max_in_flight: None,
    }
}

#[tokio::test]
async fn bulk_results_are_typed_from_describe_and_a_failed_page_is_retried_alone() {
    let results = api("/jobs/query/750/results");
    let header = "Id;Amount;IsWon;CloseDate;LastModifiedDate;Owner.Score__c;Unknown__c\n";
    let (url, log) = serve(vec![
        Route::new(api("/jobs/query/750"), &job("Opportunity")),
        Route::new(
            api("/sobjects/Opportunity/describe"),
            &describe(
                "Opportunity",
                vec![
                    field("Id", "id", None),
                    field("Amount", "currency", None),
                    field("IsWon", "boolean", None),
                    field("CloseDate", "date", None),
                    field("LastModifiedDate", "datetime", None),
                    field("OwnerId", "reference", Some(("Owner", "User"))),
                ],
            ),
        ),
        Route::new(
            api("/sobjects/User/describe"),
            &describe("User", vec![field("Score__c", "double", None)]),
        ),
        Route::text(
            results.clone(),
            format!("{header}006B;20;false;2024-02-01;2024-02-01T08:00:00.000Z;2;\n"),
        )
        .query("locator=next")
        .header("Sforce-Locator", "null")
        .failing(1),
        Route::text(
            results,
            format!("{header}006A;1500.5;true;2024-01-15;2024-01-15T10:30:00.000Z;4.5;00123\n"),
        )
        .header("Sforce-Locator", "next"),
    ])
    .await;
    let credentials = credentials(&url);
    let (input_tx, input_rx) = mpsc::channel(1);
    let (output_tx, output_rx) = mpsc::channel(16);
    let processor = flowgen_salesforce::bulkapi::query_job::ProcessorBuilder::new()
        .config(Arc::new(bulk_config(&credentials)))
        .receiver(input_rx)
        .sender(output_tx)
        .task_id(1)
        .task_type("salesforce_bulkapi_query_job")
        .task_context(task_context())
        .build()
        .unwrap();

    let emitted = run(processor, input_tx, output_rx).await;

    let batches: Vec<_> = emitted
        .iter()
        .map(|event| match &event.data {
            EventData::ArrowRecordBatch(batch) => batch.clone(),
            other => panic!("expected a record batch, got {other:?}"),
        })
        .collect();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [1, 1]
    );
    let schema = batches[0].schema();
    let types: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.data_type().clone()))
        .collect();
    assert_eq!(
        types,
        [
            ("Id", DataType::Utf8),
            ("Amount", DataType::Float64),
            ("IsWon", DataType::Boolean),
            ("CloseDate", DataType::Date32),
            (
                "LastModifiedDate",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
            ),
            ("Owner.Score__c", DataType::Float64),
            ("Unknown__c", DataType::Utf8),
        ]
    );
    let first = &batches[0];
    let amount = first.column(1).as_any().downcast_ref::<Float64Array>();
    let won = first.column(2).as_any().downcast_ref::<BooleanArray>();
    let unknown = first.column(6).as_any().downcast_ref::<StringArray>();
    assert_eq!(amount.map(|column| column.value(0)), Some(1500.5));
    assert_eq!(won.map(|column| column.value(0)), Some(true));
    assert_eq!(unknown.map(|column| column.value(0)), Some("00123"));
    assert!(batches[1].column(6).is_null(0));

    let log = log.lock().unwrap();
    let pages: Vec<_> = log
        .iter()
        .filter(|(path, _)| path.ends_with("/results"))
        .map(|(_, query)| {
            (
                query.get("locator").cloned(),
                query.get("maxRecords").cloned(),
            )
        })
        .collect();
    assert_eq!(
        pages,
        [
            (None, Some("10".to_string())),
            (Some("next".to_string()), Some("10".to_string())),
            (Some("next".to_string()), Some("10".to_string())),
        ]
    );
}

fn record(id: &str) -> Map<String, Value> {
    Map::from_iter([("Id".to_string(), Value::String(id.to_string()))])
}

#[tokio::test]
async fn soql_query_binds_parameters_and_retries_a_failed_page_alone() {
    let (url, log) = serve(vec![
        Route::new(
            api("/query/01g-2000"),
            &QueryResponse {
                done: true,
                next_records_url: None,
                records: vec![record("b")],
                total_size: 2,
            },
        )
        .failing(1),
        Route::new(
            api("/query"),
            &QueryResponse {
                done: false,
                next_records_url: Some(api("/query/01g-2000")),
                records: vec![record("a")],
                total_size: 2,
            },
        )
        .query("q=SELECT"),
    ])
    .await;
    let credentials = credentials(&url);
    let (input_tx, input_rx) = mpsc::channel(1);
    let (output_tx, output_rx) = mpsc::channel(16);
    let processor = flowgen_salesforce::restapi::query::ProcessorBuilder::new()
        .config(Arc::new(Query {
            name: "accounts".to_string(),
            credentials_path: credentials.path().to_path_buf(),
            query: Source::Inline("SELECT Id FROM Account WHERE Name = :name".to_string()),
            parameters: Some(HashMap::from([(
                "name".to_string(),
                Parameter::Value(Value::String("O'Brien".to_string())),
            )])),
            query_operation: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
        }))
        .receiver(input_rx)
        .sender(output_tx)
        .task_id(1)
        .task_type("salesforce_restapi_query")
        .task_context(task_context())
        .build()
        .unwrap();

    let emitted = run(processor, input_tx, output_rx).await;

    let pages: Vec<QueryResponse> = emitted
        .iter()
        .map(|event| serde_json::from_value(event.data_as_json().unwrap()).unwrap())
        .collect();
    let ids: Vec<_> = pages
        .iter()
        .flat_map(|page| &page.records)
        .map(|record| record["Id"].clone())
        .collect();
    assert_eq!(ids, ["a", "b"]);
    let log = log.lock().unwrap();
    let sent = log
        .iter()
        .find_map(|(path, query)| match path.ends_with("/query") {
            true => query.get("q").cloned(),
            false => None,
        });
    assert_eq!(
        sent.as_deref(),
        Some(r"SELECT Id FROM Account WHERE Name = 'O\'Brien'")
    );
}
