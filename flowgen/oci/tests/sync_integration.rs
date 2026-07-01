//! Integration test for `oci_sync` against a real OCI Distribution
//! registry running in a Docker container.
//!
//! Reproduces the production GHCR artifact shape that broke the
//! bootstrap sync flows: an empty config layer plus N file layers,
//! where each file layer carries the relative path in
//! `org.opencontainers.image.title`. Confirms `oci_sync` emits one
//! event per file layer, in order, with `completion_tx` attached to
//! the final event so downstream buffers can detect end-of-batch.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a
//! default `cargo test` skips it on developer machines without
//! Docker; CI runs the ignored set explicitly.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use flowgen_oci::sync::config::Processor as OciSyncConfig;
use flowgen_oci::sync::processor::ProcessorBuilder;
use oci_client::client::{ClientConfig, ClientProtocol, Config, ImageLayer};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use tokio::sync::mpsc;

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_emits_one_event_per_file_layer_with_completion_on_last() {
    let registry = GenericImage::new("registry", "2.8.3")
        .with_exposed_port(5000.tcp())
        .with_wait_for(WaitFor::message_on_stderr("listening on"))
        .start()
        .await
        .expect("failed to start registry container");
    let port = registry
        .get_host_port_ipv4(5000)
        .await
        .expect("failed to read mapped host port");

    let registry_host = format!("127.0.0.1:{port}");
    let reference: Reference = format!("{registry_host}/flowgen/resources:latest")
        .parse()
        .expect("reference parse");

    let soql_bytes = b"SELECT Id, Name FROM Account\n".to_vec();
    let schema_bytes = br#"{ "fields": [] }"#.to_vec();

    let push_client = Client::new(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    });

    let layer_soql = ImageLayer::new(
        soql_bytes.clone(),
        "application/octet-stream".to_string(),
        Some(annotation("salesforce/query_account.soql")),
    );
    let layer_schema = ImageLayer::new(
        schema_bytes.clone(),
        "application/octet-stream".to_string(),
        Some(annotation("salesforce/account_schema.json")),
    );
    let config = Config::oci_v1(b"{}".to_vec(), None);

    push_client
        .push(
            &reference,
            &[layer_soql.clone(), layer_schema.clone()],
            config,
            &RegistryAuth::Anonymous,
            None,
        )
        .await
        .expect("push test artifact to registry");

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull_repo".to_string(),
        artifact: format!("{registry_host}/flowgen/resources:latest"),
        credentials_path: None,
        force_pull: false,
        depends_on: None,
        retry: None,
    });

    let (trigger_tx, trigger_rx) = mpsc::channel(8);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(16);

    let processor = ProcessorBuilder::new()
        .config(sync_config)
        .receiver(trigger_rx)
        .sender(downstream_tx)
        .task_id(1)
        .task_type("oci_sync")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build oci_sync processor");

    let handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    let (completion_state, _completion_rx) = flowgen_core::event::new_completion_channel(1);
    trigger_tx
        .send(
            flowgen_core::event::EventBuilder::new()
                .data(flowgen_core::event::EventData::Json(
                    serde_json::json!({"trigger": true}),
                ))
                .subject("tick".to_string())
                .task_id(0)
                .task_type("generate")
                .completion_tx(completion_state)
                .build()
                .expect("build trigger event"),
        )
        .await
        .expect("send trigger event");

    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), downstream_rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) | Err(_) if !events.is_empty() => break,
            _ => continue,
        }
    }

    drop(trigger_tx);
    let _ = handle.await;

    assert_eq!(
        events.len(),
        2,
        "expected one event per file layer, got {}: {:?}",
        events.len(),
        events
            .iter()
            .map(|e| e.data_as_json().ok())
            .collect::<Vec<_>>()
    );

    let paths: Vec<String> = events
        .iter()
        .map(|e| {
            e.data_as_json()
                .unwrap()
                .get("path")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(
        paths,
        vec![
            "salesforce/query_account.soql".to_string(),
            "salesforce/account_schema.json".to_string(),
        ],
        "layer paths must round-trip through the title annotation"
    );

    for (i, event) in events.iter().enumerate() {
        let is_last = i == events.len() - 1;
        assert_eq!(
            event.completion_tx.is_some(),
            is_last,
            "event {i} completion_tx presence does not match end-of-batch",
        );
    }
}

fn annotation(path: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert(
        "org.opencontainers.image.title".to_string(),
        path.to_string(),
    );
    map
}

fn test_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
        as Arc<dyn flowgen_core::cache::Cache>;
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("build TaskContext"),
    )
}
