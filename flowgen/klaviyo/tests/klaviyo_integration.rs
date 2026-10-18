//! Runs the Klaviyo tasks through their public `Processor` against an
//! in-process HTTP server standing in for the Klaviyo API, set as each task's
//! `base_url`. Covers what each operation sends and emits, retries, and
//! startup validation. Needs nothing else, so it isn't `#[ignore]`d.

use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_core::task::runner::Runner;
use flowgen_klaviyo::campaigns::{CampaignOperation, Campaigns};
use flowgen_klaviyo::lists::{ListOperation, Lists};
use flowgen_klaviyo::processor::{Error, Operation, Payload, ProcessorBuilder};
use flowgen_klaviyo::templates::{TemplateOperation, Templates};
use klaviyo::types;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// A request the test server received.
#[derive(Clone, Debug)]
struct Received {
    method: String,
    path: String,
    query: HashMap<String, String>,
    body: String,
}

/// What the test server answers to one method, path, and page cursor.
struct Route {
    method: &'static str,
    path: &'static str,
    cursor: Option<&'static str>,
    status: axum::http::StatusCode,
    body: String,
}

fn ok<T: Serialize>(method: &'static str, path: &'static str, response: &T) -> Route {
    Route {
        method,
        path,
        cursor: None,
        status: axum::http::StatusCode::OK,
        body: serde_json::to_string(response).unwrap(),
    }
}

fn page<T: Serialize>(path: &'static str, cursor: &'static str, response: &T) -> Route {
    Route {
        cursor: Some(cursor),
        ..ok("GET", path, response)
    }
}

fn failing(method: &'static str, path: &'static str) -> Route {
    Route {
        method,
        path,
        cursor: None,
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
             method: axum::http::Method,
             uri: axum::http::Uri,
             axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
             body: String| async move {
                let cursor = query.get("page[cursor]").cloned();
                log.lock().await.push(Received {
                    method: method.to_string(),
                    path: uri.path().to_string(),
                    query,
                    body,
                });
                match routes.iter().find(|route| {
                    route.method == method.as_str()
                        && route.path == uri.path()
                        && route.cursor == cursor.as_deref()
                }) {
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
        serde_json::to_string(&Credentials { api_key: "pk_test" }).unwrap(),
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

fn data<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap()
}

/// Runs `config` for one event and returns what it emits.
async fn run<C: Operation>(config: C, input: Value) -> Result<Vec<Value>, Error> {
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
        .task_type("klaviyo")
        .task_context(Arc::new(task_context))
        .build()?;

    let running = tokio::spawn(processor.run());
    let _ = input_tx.send(event(input)).await;
    drop(input_tx);
    running.await.unwrap()?;

    let mut emitted = Vec::new();
    while let Ok(event) = output_rx.try_recv() {
        emitted.push(event.data_as_json().unwrap());
    }
    Ok(emitted)
}

fn object_links(path: &str) -> types::ObjectLinks {
    types::ObjectLinks {
        self_: format!("https://a.klaviyo.com/api/{path}"),
    }
}

fn templates(base_url: &str, credentials: PathBuf) -> Templates {
    Templates {
        name: "save_template".to_string(),
        operation: TemplateOperation::Upsert,
        credentials_path: credentials,
        base_url: Some(base_url.to_string()),
        template_id: None,
        filter: None,
        payload: Some(Payload::FromEvent { from_event: true }),
        depends_on: None,
        retry: None,
        max_in_flight: None,
    }
}

fn template_found(id: &str, name: &str) -> types::GetTemplateDndResponseCollection {
    types::GetTemplateDndResponseCollection {
        data: vec![types::GetTemplateDndResponseCollectionDataItem {
            attributes: types::GetTemplateDndResponseCollectionDataItemAttributes {
                amp: None,
                created: None,
                definition: None,
                editor_type: "CODE".to_string(),
                html: "<p>a</p>".to_string(),
                name: name.to_string(),
                screenshot_url: None,
                text: None,
                updated: None,
            },
            id: id.to_string(),
            links: object_links(&format!("templates/{id}")),
            type_: types::TemplateEnum::Template,
        }],
        links: None,
        meta: None,
    }
}

fn template_not_found() -> types::GetTemplateDndResponseCollection {
    types::GetTemplateDndResponseCollection {
        data: Vec::new(),
        links: None,
        meta: None,
    }
}

fn template_updated(id: &str, name: &str) -> types::PatchTemplateDndResponse {
    types::PatchTemplateDndResponse {
        data: types::PatchTemplateDndResponseData {
            attributes: types::PatchTemplateDndResponseDataAttributes {
                amp: None,
                created: None,
                definition: None,
                editor_type: "CODE".to_string(),
                html: "<p>b</p>".to_string(),
                name: name.to_string(),
                screenshot_url: None,
                text: None,
                updated: None,
            },
            id: id.to_string(),
            links: object_links(&format!("templates/{id}")),
            type_: types::TemplateEnum::Template,
        },
        links: None,
        meta: None,
    }
}

fn template_created(id: &str, name: &str) -> types::PostTemplateDndResponse {
    types::PostTemplateDndResponse {
        data: types::PostTemplateDndResponseData {
            attributes: types::PostTemplateDndResponseDataAttributes {
                amp: None,
                created: None,
                definition: None,
                editor_type: "CODE".to_string(),
                html: "<p>b</p>".to_string(),
                name: name.to_string(),
                screenshot_url: None,
                text: None,
                updated: None,
            },
            id: id.to_string(),
            links: object_links(&format!("templates/{id}")),
            type_: types::TemplateEnum::Template,
        },
        links: None,
        meta: None,
    }
}

fn template_attributes(name: &str) -> Value {
    data(
        &types::TemplateCreateHtmlOrDndQueryResourceObjectAttributes {
            name: name.to_string(),
            editor_type: "CODE".to_string(),
            html: Some("<p>b</p>".to_string()),
            ..Default::default()
        },
    )
}

#[tokio::test]
async fn upsert_updates_the_template_with_the_same_name() {
    let (url, received) = serve(vec![
        ok("GET", "/api/templates", &template_found("T1", "Spring")),
        ok(
            "PATCH",
            "/api/templates/T1",
            &template_updated("T1", "Spring"),
        ),
    ])
    .await;
    let credentials = credentials();

    let emitted = run(
        templates(&url, credentials.path().to_path_buf()),
        template_attributes("Spring"),
    )
    .await
    .unwrap();

    let emitted: types::PatchTemplateDndResponse =
        serde_json::from_value(emitted[0].clone()).unwrap();
    assert_eq!(emitted.data.id, "T1");
    let received = received.lock().await;
    assert_eq!(
        received[0].query.get("filter").map(String::as_str),
        Some("equals(name,'Spring')")
    );
    assert_eq!(
        (received[1].method.as_str(), received[1].path.as_str()),
        ("PATCH", "/api/templates/T1")
    );
    let sent: types::TemplateUpdateHtmlOrDndQuery =
        serde_json::from_str(&received[1].body).unwrap();
    assert_eq!(sent.data.attributes.html.as_deref(), Some("<p>b</p>"));
}

#[tokio::test]
async fn upsert_creates_the_template_when_none_has_the_name() {
    let (url, received) = serve(vec![
        ok("GET", "/api/templates", &template_not_found()),
        ok("POST", "/api/templates", &template_created("T2", "Spring")),
    ])
    .await;
    let credentials = credentials();

    let emitted = run(
        templates(&url, credentials.path().to_path_buf()),
        template_attributes("Spring"),
    )
    .await
    .unwrap();

    let emitted: types::PostTemplateDndResponse =
        serde_json::from_value(emitted[0].clone()).unwrap();
    assert_eq!(emitted.data.id, "T2");
    let methods: Vec<String> = received
        .lock()
        .await
        .iter()
        .map(|request| request.method.clone())
        .collect();
    assert_eq!(methods, ["GET", "POST"]);
}

fn list(id: &str) -> types::ListListResponseObjectResource {
    types::ListListResponseObjectResource {
        attributes: types::ListListResponseObjectResourceAttributes {
            created: None,
            name: Some(id.to_string()),
            opt_in_process: None,
            updated: None,
        },
        id: id.to_string(),
        links: object_links(&format!("lists/{id}")),
        relationships: None,
        type_: types::ListEnum::List,
    }
}

fn lists_page(
    ids: &[&str],
    next: Option<String>,
) -> types::GetListListResponseCollectionCompoundDocument {
    types::GetListListResponseCollectionCompoundDocument {
        data: ids.iter().map(|id| list(id)).collect(),
        included: Vec::new(),
        links: Some(types::CollectionLinks {
            next,
            prev: None,
            self_: "https://a.klaviyo.com/api/lists".to_string(),
        }),
        meta: None,
    }
}

#[tokio::test]
async fn list_emits_one_event_per_page() {
    let next = "https://a.klaviyo.com/api/lists?page%5Bcursor%5D=second".to_string();
    let (url, received) = serve(vec![
        ok("GET", "/api/lists", &lists_page(&["L1", "L2"], Some(next))),
        page("/api/lists", "second", &lists_page(&["L3"], None)),
    ])
    .await;
    let credentials = credentials();

    let emitted = run(
        Lists {
            name: "lists".to_string(),
            operation: ListOperation::List,
            credentials_path: credentials.path().to_path_buf(),
            base_url: Some(url),
            list_id: None,
            filter: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
        },
        Value::Null,
    )
    .await
    .unwrap();

    let ids: Vec<Vec<String>> = emitted
        .into_iter()
        .map(|page| {
            let page: types::GetListListResponseCollectionCompoundDocument =
                serde_json::from_value(page).unwrap();
            page.data.into_iter().map(|list| list.id).collect()
        })
        .collect();
    assert_eq!(ids, [vec!["L1", "L2"], vec!["L3"]]);
    assert_eq!(received.lock().await.len(), 2);
}

fn campaigns(base_url: &str, credentials: PathBuf, operation: CampaignOperation) -> Campaigns {
    Campaigns {
        name: "campaigns".to_string(),
        operation,
        credentials_path: credentials,
        base_url: Some(base_url.to_string()),
        campaign_id: Some("C1".to_string()),
        message_id: None,
        template_id: Some("T1".to_string()),
        filter: None,
        payload: None,
        depends_on: None,
        retry: None,
        max_in_flight: None,
    }
}

fn message(id: &str) -> types::CampaignMessageResponseObjectResource {
    types::CampaignMessageResponseObjectResource {
        attributes: types::CampaignMessageResponseObjectResourceAttributes {
            created_at: None,
            definition: None,
            send_times: None,
            updated_at: None,
        },
        id: id.to_string(),
        links: object_links(&format!("campaign-messages/{id}")),
        relationships: None,
        type_: types::CampaignMessageEnum::CampaignMessage,
    }
}

fn messages(ids: &[&str]) -> types::GetCampaignMessageResponseCollectionCompoundDocument {
    types::GetCampaignMessageResponseCollectionCompoundDocument {
        data: ids.iter().map(|id| message(id)).collect(),
        included: Vec::new(),
        links: None,
        meta: None,
    }
}

fn assigned(id: &str) -> types::PostCampaignMessageResponse {
    types::PostCampaignMessageResponse {
        data: types::PostCampaignMessageResponseData {
            attributes: types::PostCampaignMessageResponseDataAttributes {
                channel: "email".to_string(),
                content:
                    types::PostCampaignMessageResponseDataAttributesContent::EmailContentSubObject(
                        types::EmailContentSubObject {
                            bcc_email: None,
                            cc_email: None,
                            from_email: None,
                            from_label: None,
                            preview_text: None,
                            reply_to_email: None,
                            subject: None,
                        },
                    ),
                created_at: None,
                label: "a".to_string(),
                render_options: None,
                send_times: None,
                updated_at: None,
            },
            id: id.to_string(),
            links: object_links(&format!("campaign-messages/{id}")),
            relationships: None,
            type_: types::CampaignMessageEnum::CampaignMessage,
        },
        links: None,
        meta: None,
    }
}

#[tokio::test]
async fn assign_template_covers_every_message_of_the_campaign() {
    let (url, received) = serve(vec![
        ok(
            "GET",
            "/api/campaigns/C1/campaign-messages",
            &messages(&["M1", "M2"]),
        ),
        ok(
            "POST",
            "/api/campaign-message-assign-template",
            &assigned("M1"),
        ),
    ])
    .await;
    let credentials = credentials();

    let emitted = run(
        campaigns(
            &url,
            credentials.path().to_path_buf(),
            CampaignOperation::AssignTemplate,
        ),
        Value::Null,
    )
    .await
    .unwrap();

    assert_eq!(emitted.len(), 2);
    let assigned: Vec<(String, String)> = received
        .lock()
        .await
        .iter()
        .filter(|request| request.method == "POST")
        .map(|request| {
            let sent: types::CampaignMessageAssignTemplateQuery =
                serde_json::from_str(&request.body).unwrap();
            let template = sent.data.relationships.template.data.unwrap();
            (sent.data.id, template.id)
        })
        .collect();
    assert_eq!(
        assigned,
        [
            ("M1".to_string(), "T1".to_string()),
            ("M2".to_string(), "T1".to_string())
        ]
    );
}

#[tokio::test]
async fn a_failed_send_is_not_retried_but_a_failed_lookup_is() {
    let (url, received) = serve(vec![
        failing("POST", "/api/campaign-send-jobs"),
        failing("GET", "/api/campaigns/C1"),
    ])
    .await;
    let credentials = credentials();

    run(
        campaigns(
            &url,
            credentials.path().to_path_buf(),
            CampaignOperation::Send,
        ),
        Value::Null,
    )
    .await
    .unwrap();
    run(
        campaigns(
            &url,
            credentials.path().to_path_buf(),
            CampaignOperation::Get,
        ),
        Value::Null,
    )
    .await
    .unwrap();

    let received = received.lock().await;
    let sends = received
        .iter()
        .filter(|request| request.path == "/api/campaign-send-jobs")
        .count();
    let lookups = received
        .iter()
        .filter(|request| request.path == "/api/campaigns/C1")
        .count();
    assert_eq!(sends, 1);
    assert!(lookups > 1);
}

#[tokio::test]
async fn a_missing_field_stops_the_task_at_startup() {
    let (url, received) = serve(Vec::new()).await;
    let credentials = credentials();

    let result = run(
        Campaigns {
            filter: None,
            ..campaigns(
                &url,
                credentials.path().to_path_buf(),
                CampaignOperation::List,
            )
        },
        Value::Null,
    )
    .await;

    assert!(matches!(result, Err(Error::MissingField("filter"))));
    assert!(received.lock().await.is_empty());
}
