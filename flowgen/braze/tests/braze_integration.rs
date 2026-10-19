//! Runs the Braze tasks through their public `Processor` against an
//! in-process HTTP server standing in for a Braze instance, set as each
//! task's `base_url`. Covers what the operations send and emit, retries, and
//! startup validation. Needs nothing else, so it isn't `#[ignore]`d.

use flowgen_braze::export_user_data::{ExportOperation, ExportUserData};
use flowgen_braze::messaging::{Messaging, MessagingOperation};
use flowgen_braze::processor::{Error, Operation, Payload, ProcessorBuilder};
use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_core::task::runner::Runner;
use serde::Serialize;
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// A request the test server received.
#[derive(Clone, Debug)]
struct Received {
    path: String,
    body: String,
}

/// What the test server answers to one path.
struct Route {
    path: &'static str,
    status: axum::http::StatusCode,
    body: String,
}

fn ok<T: Serialize>(path: &'static str, response: &T) -> Route {
    Route {
        path,
        status: axum::http::StatusCode::OK,
        body: serde_json::to_string(response).unwrap(),
    }
}

fn failing(path: &'static str) -> Route {
    Route {
        path,
        status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
        body: String::new(),
    }
}

type Log = Arc<Mutex<Vec<Received>>>;

/// Serves `routes`, answering 404 to anything else, and records every request.
async fn serve(routes: Vec<Route>) -> (String, Log) {
    let log: Log = Arc::default();
    let state = (Arc::new(routes), Arc::clone(&log));
    let app = axum::Router::new()
        .fallback(
            |axum::extract::State((routes, log)): axum::extract::State<(Arc<Vec<Route>>, Log)>,
             uri: axum::http::Uri,
             body: String| async move {
                log.lock().await.push(Received {
                    path: uri.path().to_string(),
                    body,
                });
                match routes.iter().find(|route| route.path == uri.path()) {
                    Some(route) => (route.status, route.body.clone()),
                    None => (axum::http::StatusCode::NOT_FOUND, String::new()),
                }
            },
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}"), log)
}

#[derive(Serialize)]
struct Credentials {
    api_key: &'static str,
}

/// A credentials file with a test key, kept until the returned handle drops.
fn credentials() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        serde_json::to_string(&Credentials { api_key: "test" }).unwrap(),
    )
    .unwrap();
    file
}

fn event(data: Value) -> Event {
    EventBuilder::new()
        .data(EventData::Json(data))
        .subject("a".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .unwrap()
}

/// `request` as a payload written in the config.
fn payload<T: Serialize>(request: &T) -> Option<Payload> {
    match serde_json::to_value(request).unwrap() {
        Value::Object(body) => Some(Payload::Body(body)),
        _ => None,
    }
}

/// Runs `config` for one event and returns what it emits.
async fn run<C: Operation>(config: C) -> Result<Vec<Value>, Error> {
    let (input_tx, input_rx) = mpsc::channel(1);
    let (output_tx, mut output_rx) = mpsc::channel(16);
    let task_context = flowgen_core::task::context::TaskContextBuilder::new()
        .flow_name("a".to_string())
        .task_manager(Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        ))
        .cache(Arc::new(flowgen_core::cache::memory::MemoryCache::new()))
        .retry(flowgen_core::retry::RetryConfig {
            max_attempts: Some(2),
            initial_backoff: Duration::from_millis(1),
        })
        .build()
        .unwrap();
    let processor = ProcessorBuilder::new()
        .config(Arc::new(config))
        .receiver(input_rx)
        .sender(output_tx)
        .task_id(1)
        .task_type("braze")
        .task_context(Arc::new(task_context))
        .build()?;

    let running = tokio::spawn(processor.run());
    let _ = input_tx.send(event(Value::Null)).await;
    drop(input_tx);
    running.await.unwrap()?;

    let mut emitted = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        emitted.push(event.data_as_json().unwrap());
    }
    Ok(emitted)
}

fn messaging(
    base_url: &str,
    credentials: PathBuf,
    operation: MessagingOperation,
    payload: Option<Payload>,
) -> Messaging {
    Messaging {
        name: "messaging".to_string(),
        operation,
        credentials_path: credentials,
        base_url: base_url.to_string(),
        campaign_id: None,
        payload,
        depends_on: None,
        retry: None,
        max_in_flight: None,
    }
}

#[tokio::test]
async fn users_by_ids_sends_the_payload_and_emits_the_response() {
    let (url, received) = serve(vec![ok(
        "/users/export/ids",
        &braze::export::ExportUsersByIdsResponse {
            invalid_user_ids: vec!["user-2".to_string()],
            message: Some("success".to_string()),
            users: Vec::new(),
            extra: Map::new(),
        },
    )])
    .await;
    let credentials = credentials();

    let emitted = run(ExportUserData {
        name: "export".to_string(),
        operation: ExportOperation::UsersByIds,
        credentials_path: credentials.path().to_path_buf(),
        base_url: url,
        payload: payload(&braze::export::ExportUsersByIdsRequest {
            external_ids: vec!["user-1".to_string(), "user-2".to_string()],
            ..Default::default()
        }),
        depends_on: None,
        retry: None,
        max_in_flight: None,
    })
    .await
    .unwrap();

    let emitted: braze::export::ExportUsersByIdsResponse =
        serde_json::from_value(emitted[0].clone()).unwrap();
    assert_eq!(emitted.invalid_user_ids, ["user-2"]);
    let sent: braze::export::ExportUsersByIdsRequest =
        serde_json::from_str(&received.lock().await[0].body).unwrap();
    assert_eq!(sent.external_ids, ["user-1", "user-2"]);
}

#[tokio::test]
async fn send_transactional_puts_the_campaign_in_the_path() {
    let (url, received) = serve(vec![ok(
        "/transactional/v1/campaigns/campaign-1/send",
        &braze::messaging::SendTransactionalMessageResponse {
            dispatch_id: Some("dispatch-1".to_string()),
            metadata: Map::new(),
            status: Some("queued".to_string()),
            extra: Map::new(),
        },
    )])
    .await;
    let credentials = credentials();

    let emitted = run(Messaging {
        campaign_id: Some("campaign-1".to_string()),
        ..messaging(
            &url,
            credentials.path().to_path_buf(),
            MessagingOperation::SendTransactional,
            payload(&braze::messaging::SendTransactionalMessageRequest {
                recipient: braze::messaging::TransactionalRecipient {
                    external_user_id: Some("user-1".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
    })
    .await
    .unwrap();

    let emitted: braze::messaging::SendTransactionalMessageResponse =
        serde_json::from_value(emitted[0].clone()).unwrap();
    assert_eq!(emitted.dispatch_id.as_deref(), Some("dispatch-1"));
    assert_eq!(received.lock().await.len(), 1);
}

#[tokio::test]
async fn a_failed_send_is_not_retried_but_a_failed_send_id_is() {
    let (url, received) = serve(vec![
        failing("/campaigns/trigger/send"),
        failing("/sends/id/create"),
    ])
    .await;
    let credentials = credentials();

    run(messaging(
        &url,
        credentials.path().to_path_buf(),
        MessagingOperation::TriggerCampaign,
        payload(&braze::messaging::SendTriggeredCampaignsRequest {
            campaign_id: "campaign-1".to_string(),
            ..Default::default()
        }),
    ))
    .await
    .unwrap();
    run(messaging(
        &url,
        credentials.path().to_path_buf(),
        MessagingOperation::CreateSendIds,
        payload(&braze::messaging::CreateSendIdsRequest {
            campaign_id: "campaign-1".to_string(),
            ..Default::default()
        }),
    ))
    .await
    .unwrap();

    let received = received.lock().await;
    let count = |path: &str| received.iter().filter(|r| r.path == path).count();
    assert_eq!(count("/campaigns/trigger/send"), 1);
    assert!(count("/sends/id/create") > 1);
}

#[tokio::test]
async fn a_missing_field_stops_the_task_at_startup() {
    let (url, received) = serve(Vec::new()).await;
    let credentials = credentials();

    let result = run(messaging(
        &url,
        credentials.path().to_path_buf(),
        MessagingOperation::SendTransactional,
        payload(&braze::messaging::SendTransactionalMessageRequest::default()),
    ))
    .await;

    assert!(matches!(result, Err(Error::MissingField("campaign_id"))));
    assert!(received.lock().await.is_empty());
}
