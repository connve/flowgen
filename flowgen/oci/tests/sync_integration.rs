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
        ..Default::default()
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

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_unpacks_tar_gzip_layer_into_per_file_events() {
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
    let reference: Reference = format!("{registry_host}/flowgen/docker-image:latest")
        .parse()
        .expect("reference parse");

    let tar_bytes = build_tar_bytes(&[
        ("flow.yaml", b"name: docker-flow\n"),
        ("processors/sms.yaml", b"kind: sms\n"),
    ]);
    let tar_gzip_bytes = gzip_bytes(&tar_bytes);

    let push_client = Client::new(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    });

    let layer = ImageLayer::new(
        tar_gzip_bytes,
        "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
        None,
    );
    let config = Config::oci_v1(b"{}".to_vec(), None);

    push_client
        .push(&reference, &[layer], config, &RegistryAuth::Anonymous, None)
        .await
        .expect("push tar+gzip artifact to registry");

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull_docker".to_string(),
        artifact: format!("{registry_host}/flowgen/docker-image:latest"),
        ..Default::default()
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
        "expected one event per file entry inside the tar+gzip layer, got {}",
        events.len(),
    );

    let mut extracted: Vec<(String, String)> = events
        .iter()
        .map(|e| {
            let data = e.data_as_json().unwrap();
            (
                data.get("path").unwrap().as_str().unwrap().to_string(),
                data.get("content").unwrap().as_str().unwrap().to_string(),
            )
        })
        .collect();
    extracted.sort();
    assert_eq!(
        extracted,
        vec![
            ("flow.yaml".to_string(), "name: docker-flow\n".to_string(),),
            ("processors/sms.yaml".to_string(), "kind: sms\n".to_string(),),
        ],
        "tar entry paths and contents must round-trip through the extractor",
    );

    let last = events.last().unwrap();
    assert!(
        last.completion_tx.is_some(),
        "completion_tx must attach to the final file event, not the layer",
    );
}

fn build_tar_bytes(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, content) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder.append_data(&mut header, name, *content).unwrap();
    }
    builder.into_inner().unwrap()
}

fn gzip_bytes(input: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut encoder, input).unwrap();
    encoder.finish().unwrap()
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

// ---------------------------------------------------------------------------
// Extended layer variants: uncompressed tar, whiteouts, non-file entries,
// tar-bomb size caps.
// ---------------------------------------------------------------------------

/// Boots a fresh registry container and returns `(container, host)` so
/// the container stays alive for the test's lifetime.
async fn boot_registry() -> (testcontainers::ContainerAsync<GenericImage>, String) {
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
    let host = format!("127.0.0.1:{port}");
    (registry, host)
}

/// Pushes a single layer under `<host>/<repo>:latest` and returns the
/// pushable reference. `media_type` picks between raw / tar / tar+gzip.
async fn push_layer(host: &str, repo: &str, media_type: &str, bytes: Vec<u8>) -> String {
    let reference: Reference = format!("{host}/{repo}:latest").parse().expect("reference");
    let push_client = Client::new(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    });
    let layer = ImageLayer::new(bytes, media_type.to_string(), None);
    let config = Config::oci_v1(b"{}".to_vec(), None);
    push_client
        .push(&reference, &[layer], config, &RegistryAuth::Anonymous, None)
        .await
        .expect("push single-layer artifact to registry");
    reference.to_string()
}

/// Runs `oci_sync` against `artifact` and returns whatever events the
/// downstream channel receives before the sender drops or the deadline
/// elapses. Fails the test if `oci_sync.run()` returns an error.
async fn run_sync_and_collect(sync_config: Arc<OciSyncConfig>) -> Vec<flowgen_core::event::Event> {
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
        processor.run().await.expect("oci_sync run must succeed");
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
    events
}

/// Same as `run_sync_and_collect` but expects the trigger event to
/// re-surface downstream with `event.error = Some(_)` after retries are
/// exhausted. Returns the error string on the event. Panics if no error
/// event lands within the deadline.
async fn run_sync_expecting_error(sync_config: Arc<OciSyncConfig>) -> String {
    // Use a small retry budget so a `permanent`-classified error surfaces
    // fast even when the underlying processor's default retry count is
    // large.
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

    // Wait for the error event carrying the failure string.
    let deadline = Duration::from_secs(15);
    let error_event = tokio::time::timeout(deadline, downstream_rx.recv())
        .await
        .expect("error event never arrived within deadline")
        .expect("downstream channel closed before error event");

    let err = error_event
        .error
        .clone()
        .expect("error event must carry an error field, got clean event");

    drop(trigger_tx);
    handle.abort();
    err
}

/// Builds a tar archive from `(name, entry_type, content)` triples so
/// tests can exercise Regular / Directory / Symlink / Link / whiteout
/// entries in one archive.
fn build_tar_with_types(entries: &[(&str, tar::EntryType, &[u8])]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, entry_type, content) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(*entry_type);
        header.set_mode(0o644);
        // Directories and symlinks carry no payload; regular files do.
        let payload: &[u8] = match entry_type {
            tar::EntryType::Regular => content,
            _ => &[],
        };
        header.set_size(payload.len() as u64);
        if matches!(entry_type, tar::EntryType::Symlink) {
            header
                .set_link_name(std::str::from_utf8(content).unwrap())
                .unwrap();
        }
        header.set_cksum();
        builder.append_data(&mut header, name, payload).unwrap();
    }
    builder.into_inner().unwrap()
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_unpacks_uncompressed_tar_layer() {
    let (_registry, host) = boot_registry().await;
    let tar_bytes = build_tar_bytes(&[
        ("flows/a.yaml", b"name: a\n"),
        ("flows/b.yaml", b"name: b\n"),
    ]);

    let reference = push_layer(
        &host,
        "flowgen/plain-tar",
        "application/vnd.oci.image.layer.v1.tar",
        tar_bytes,
    )
    .await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        ..Default::default()
    });
    let events = run_sync_and_collect(sync_config).await;

    let mut extracted: Vec<(String, String)> = events
        .iter()
        .map(|e| {
            let data = e.data_as_json().unwrap();
            (
                data.get("path").unwrap().as_str().unwrap().to_string(),
                data.get("content").unwrap().as_str().unwrap().to_string(),
            )
        })
        .collect();
    extracted.sort();
    assert_eq!(
        extracted,
        vec![
            ("flows/a.yaml".to_string(), "name: a\n".to_string()),
            ("flows/b.yaml".to_string(), "name: b\n".to_string()),
        ],
        "uncompressed tar (no gzip) must unpack the same as tar+gzip",
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_skips_directory_symlink_hardlink_and_whiteout_entries() {
    let (_registry, host) = boot_registry().await;

    // One archive that stresses every skip path documented in the 0.122
    // changelog. Only the two Regular entries should surface downstream.
    let tar_bytes = build_tar_with_types(&[
        ("flows/", tar::EntryType::Directory, b""),
        ("flows/real.yaml", tar::EntryType::Regular, b"name: real\n"),
        ("flows/link.yaml", tar::EntryType::Symlink, b"real.yaml"),
        ("flows/hardlink.yaml", tar::EntryType::Link, b"real.yaml"),
        // Docker whiteout marker for `flows/removed.yaml`; must be dropped.
        (
            "flows/.wh.removed.yaml",
            tar::EntryType::Regular,
            b"tombstone",
        ),
        (
            "processors/other.yaml",
            tar::EntryType::Regular,
            b"kind: x\n",
        ),
    ]);
    let tar_gzip = gzip_bytes(&tar_bytes);

    let reference = push_layer(
        &host,
        "flowgen/skip-entries",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        tar_gzip,
    )
    .await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        ..Default::default()
    });
    let events = run_sync_and_collect(sync_config).await;

    let mut paths: Vec<String> = events
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
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "flows/real.yaml".to_string(),
            "processors/other.yaml".to_string()
        ],
        "directory, symlink, hardlink and .wh.* whiteouts must all be skipped",
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_rejects_layer_exceeding_max_file_size() {
    let (_registry, host) = boot_registry().await;

    // Single file just over a 1 KiB cap — should trip `max_file_size`.
    let big_file = vec![b'a'; 2048];
    let tar_bytes = build_tar_bytes(&[("large.yaml", &big_file)]);
    let tar_gzip = gzip_bytes(&tar_bytes);

    let reference = push_layer(
        &host,
        "flowgen/too-big-file",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        tar_gzip,
    )
    .await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        max_file_size: 1024,
        max_total_size: 10 * 1024 * 1024,
        ..Default::default()
    });
    let err = run_sync_expecting_error(sync_config).await;
    assert!(
        err.contains("max_file_size") || err.contains("FileTooLarge"),
        "error must reference the per-file cap, got: {err}"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_rejects_artifact_exceeding_max_total_size() {
    let (_registry, host) = boot_registry().await;

    // Two files, each 800 bytes; per-file cap allows them individually
    // but the cumulative total (1600 B) trips the 1 KiB total cap.
    let payload_a = vec![b'a'; 800];
    let payload_b = vec![b'b'; 800];
    let tar_bytes = build_tar_bytes(&[("a.yaml", &payload_a), ("b.yaml", &payload_b)]);
    let tar_gzip = gzip_bytes(&tar_bytes);

    let reference = push_layer(
        &host,
        "flowgen/too-big-total",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        tar_gzip,
    )
    .await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        max_file_size: 4096,
        max_total_size: 1024,
        ..Default::default()
    });
    let err = run_sync_expecting_error(sync_config).await;
    assert!(
        err.contains("max_total_size") || err.contains("ArtifactTooLarge"),
        "error must reference the cumulative cap, got: {err}"
    );
}

/// Raw layer holding a non-UTF-8 blob (e.g. a WASM module, compiled
/// artefact, image). Regression test for the pre-0.123 bug where any
/// non-UTF-8 layer would fail the entire pull with `InvalidLayerEncoding`.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_emits_bytes_event_for_binary_raw_layer() {
    let (_registry, host) = boot_registry().await;

    // Bytes that are guaranteed invalid UTF-8: a lone continuation byte
    // followed by an unpaired surrogate high byte.
    let binary_payload: Vec<u8> = vec![0x80, 0xFF, 0xC3, 0x28, 0xE2, 0x82, 0x28];
    assert!(
        std::str::from_utf8(&binary_payload).is_err(),
        "payload must be invalid UTF-8 for this test to be meaningful",
    );

    let reference = push_layer(
        &host,
        "flowgen/binary-raw",
        "application/octet-stream",
        binary_payload.clone(),
    )
    .await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        ..Default::default()
    });

    let events = run_sync_and_collect(sync_config).await;
    assert_eq!(events.len(), 1, "expected one event for the raw layer");

    let event = &events[0];
    match &event.data {
        flowgen_core::event::EventData::Bytes(bytes) => {
            assert_eq!(bytes.as_ref(), binary_payload.as_slice());
        }
        other => panic!("expected EventData::Bytes for binary layer, got {other:?}"),
    }

    let meta = event
        .meta
        .as_ref()
        .expect("binary layer event must carry meta");
    assert!(
        meta.contains_key("path"),
        "meta.path must be set for binary layer"
    );
    assert!(
        meta.contains_key("digest"),
        "meta.digest must be set for binary layer"
    );
    assert!(
        meta.contains_key("artifact_digest"),
        "meta.artifact_digest must be set for binary layer",
    );
}

/// Tar layer whose entries include a non-UTF-8 payload. Text entries must
/// still emit the historical JSON `FileEvent` shape; the binary entry must
/// emit `EventData::Bytes` with path + digests in meta.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_emits_bytes_event_for_binary_tar_entry() {
    let (_registry, host) = boot_registry().await;

    let text_payload = b"name: text-flow\n".to_vec();
    let binary_payload: Vec<u8> = vec![0x80, 0xFF, 0xC3, 0x28];
    assert!(
        std::str::from_utf8(&binary_payload).is_err(),
        "binary payload must be invalid UTF-8",
    );

    let tar_bytes = build_tar_bytes(&[
        ("flows/text.yaml", &text_payload),
        ("blobs/module.wasm", &binary_payload),
    ]);
    let tar_gzip = gzip_bytes(&tar_bytes);

    let reference = push_layer(
        &host,
        "flowgen/mixed-tar",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        tar_gzip,
    )
    .await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        ..Default::default()
    });

    let events = run_sync_and_collect(sync_config).await;
    assert_eq!(events.len(), 2, "expected one event per tar entry");

    let mut saw_text = false;
    let mut saw_binary = false;
    for event in &events {
        match &event.data {
            flowgen_core::event::EventData::Json(_) => {
                let data = event.data_as_json().expect("text event carries JSON data");
                assert_eq!(
                    data.get("path").and_then(|v| v.as_str()),
                    Some("flows/text.yaml")
                );
                assert_eq!(
                    data.get("content").and_then(|v| v.as_str()),
                    Some("name: text-flow\n")
                );
                saw_text = true;
            }
            flowgen_core::event::EventData::Bytes(bytes) => {
                assert_eq!(bytes.as_ref(), binary_payload.as_slice());
                let meta = event.meta.as_ref().expect("binary event must carry meta");
                assert_eq!(
                    meta.get("path").and_then(|v| v.as_str()),
                    Some("blobs/module.wasm"),
                );
                assert!(meta.contains_key("digest"));
                assert!(meta.contains_key("artifact_digest"));
                saw_binary = true;
            }
            other => panic!("unexpected event data variant: {other:?}"),
        }
    }
    assert!(saw_text, "text tar entry must emit JSON FileEvent");
    assert!(saw_binary, "binary tar entry must emit EventData::Bytes");
}

/// Pushes multiple tar+gzip layers under `<host>/<repo>:latest` without
/// title annotations, so `oci_sync` classifies them as Docker image
/// layers and runs the merge pass. Returns the pushable reference.
async fn push_docker_layers(host: &str, repo: &str, layers: Vec<Vec<u8>>) -> String {
    let reference: Reference = format!("{host}/{repo}:latest").parse().expect("reference");
    let push_client = Client::new(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    });
    let image_layers: Vec<ImageLayer> = layers
        .into_iter()
        .map(|bytes| {
            ImageLayer::new(
                bytes,
                "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                None,
            )
        })
        .collect();
    let config = Config::oci_v1(b"{}".to_vec(), None);
    push_client
        .push(
            &reference,
            &image_layers,
            config,
            &RegistryAuth::Anonymous,
            None,
        )
        .await
        .expect("push docker-style multi-layer artifact");
    reference.to_string()
}

/// Docker image layers must merge with overlay-fs semantics: later
/// layers override earlier layers, `.wh.<name>` markers delete files,
/// and `.wh..wh..opq` markers hide entire subtrees.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_merges_docker_layers_with_whiteout_and_override() {
    let (_registry, host) = boot_registry().await;

    // Layer 1: three files across two directories.
    let layer1 = gzip_bytes(&build_tar_bytes(&[
        ("flows/a.yaml", b"v1: a"),
        ("flows/b.yaml", b"v1: b"),
        ("configs/keep.yaml", b"v1: keep"),
    ]));
    // Layer 2: override `a.yaml`, delete `b.yaml`, keep `keep.yaml`.
    let layer2 = gzip_bytes(&build_tar_bytes(&[
        ("flows/a.yaml", b"v2: a"),
        ("flows/.wh.b.yaml", b""),
    ]));

    let reference = push_docker_layers(&host, "flowgen/docker-merge", vec![layer1, layer2]).await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        ..Default::default()
    });

    let events = run_sync_and_collect(sync_config).await;

    let file_events: BTreeMap<String, String> = events
        .iter()
        .filter_map(|e| {
            let data = e.data_as_json().ok()?;
            let path = data.get("path")?.as_str()?.to_string();
            let content = data.get("content")?.as_str()?.to_string();
            Some((path, content))
        })
        .collect();

    assert_eq!(
        file_events.len(),
        2,
        "expected merged output to contain exactly `flows/a.yaml` (v2) and `configs/keep.yaml`, got: {file_events:?}",
    );
    assert_eq!(
        file_events.get("flows/a.yaml").map(String::as_str),
        Some("v2: a"),
        "upper layer must override lower layer content",
    );
    assert_eq!(
        file_events.get("configs/keep.yaml").map(String::as_str),
        Some("v1: keep"),
        "unmodified lower-layer file must survive the merge",
    );
    assert!(
        !file_events.contains_key("flows/b.yaml"),
        ".wh.b.yaml must delete flows/b.yaml from the merged output",
    );
}

/// Opaque whiteout (`.wh..wh..opq`) hides every file in the parent
/// directory from lower layers.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_docker_opaque_whiteout_hides_lower_directory() {
    let (_registry, host) = boot_registry().await;

    // Layer 1: two files under `secret/` plus one under `public/`.
    let layer1 = gzip_bytes(&build_tar_bytes(&[
        ("secret/one.yaml", b"one"),
        ("secret/two.yaml", b"two"),
        ("public/keep.yaml", b"keep"),
    ]));
    // Layer 2: opaque whiteout on `secret/` + a fresh file in the
    // same directory that must survive.
    let layer2 = gzip_bytes(&build_tar_bytes(&[
        ("secret/.wh..wh..opq", b""),
        ("secret/fresh.yaml", b"fresh"),
    ]));

    let reference = push_docker_layers(&host, "flowgen/docker-opaque", vec![layer1, layer2]).await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        ..Default::default()
    });

    let events = run_sync_and_collect(sync_config).await;
    let file_events: BTreeMap<String, String> = events
        .iter()
        .filter_map(|e| {
            let data = e.data_as_json().ok()?;
            let path = data.get("path")?.as_str()?.to_string();
            let content = data.get("content")?.as_str()?.to_string();
            Some((path, content))
        })
        .collect();

    assert!(
        !file_events.contains_key("secret/one.yaml"),
        "opaque whiteout must hide lower-layer file `secret/one.yaml`",
    );
    assert!(
        !file_events.contains_key("secret/two.yaml"),
        "opaque whiteout must hide lower-layer file `secret/two.yaml`",
    );
    assert_eq!(
        file_events.get("secret/fresh.yaml").map(String::as_str),
        Some("fresh"),
        "file added in the whiteout layer must survive",
    );
    assert_eq!(
        file_events.get("public/keep.yaml").map(String::as_str),
        Some("keep"),
        "sibling directory must be untouched by opaque whiteout on `secret/`",
    );
}

/// Shares one MemoryCache across two syncs so the second tick can hit
/// the HEAD-check skip path. Returns downstream events from that run.
async fn run_sync_with_shared_cache(
    sync_config: Arc<OciSyncConfig>,
    cache: Arc<dyn flowgen_core::cache::Cache>,
) -> Vec<flowgen_core::event::Event> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    let task_context = Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("build TaskContext"),
    );

    let (trigger_tx, trigger_rx) = mpsc::channel(8);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(16);

    let processor = ProcessorBuilder::new()
        .config(sync_config)
        .receiver(trigger_rx)
        .sender(downstream_tx)
        .task_id(1)
        .task_type("oci_sync")
        .task_context(task_context)
        .build()
        .await
        .expect("build oci_sync processor");

    let handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        processor.run().await.expect("oci_sync run must succeed");
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
    events
}

/// Regression: the HEAD-check must actually short-circuit the second
/// tick when the tag is unchanged. Before the fix the cache stored the
/// per-platform manifest digest returned by `pull_manifest_and_config`,
/// while HEAD returned the (differing) index digest for multi-arch
/// tags — so the compare always missed and every tick re-pulled every
/// layer. Now we cache whatever HEAD saw, so the compare hits.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_second_tick_skips_when_manifest_unchanged() {
    let (_registry, host) = boot_registry().await;

    let tar_gzip = gzip_bytes(&build_tar_bytes(&[("flows/one.yaml", b"content")]));
    let reference = push_layer(
        &host,
        "flowgen/skip-when-unchanged",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        tar_gzip,
    )
    .await;

    let sync_config = Arc::new(OciSyncConfig {
        name: "pull".to_string(),
        artifact: reference,
        ..Default::default()
    });

    let cache: Arc<dyn flowgen_core::cache::Cache> =
        Arc::new(flowgen_core::cache::memory::MemoryCache::new());

    let first = run_sync_with_shared_cache(Arc::clone(&sync_config), Arc::clone(&cache)).await;
    assert!(
        !first.is_empty(),
        "first tick must emit at least one file event"
    );

    let second = run_sync_with_shared_cache(sync_config, cache).await;
    assert!(
        second.is_empty(),
        "second tick against an unchanged tag must emit zero events, got {}",
        second.len()
    );
}
