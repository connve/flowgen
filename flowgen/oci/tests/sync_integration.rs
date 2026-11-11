//! Integration tests for `oci_sync` against a real OCI Distribution
//! registry running in a Docker container.
//!
//! Every pull emits one snapshot event, `{digest, files}`: titled layers
//! as authored in manifest order, untitled layers merged as a Docker
//! overlay, tar entries unpacked and binary content base64-encoded. The
//! manifest digest is remembered only once the flow completed the
//! snapshot successfully; an unchanged artifact then emits nothing.
//!
//! Also round-trips an `oci_push` release: the pushed artifact (empty
//! config, workspace artifact type, one tar+gzip layer) is read back by
//! `oci_sync` under every tag with the digest `oci_push` reported.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a
//! default `cargo test` skips it on developer machines without
//! Docker; CI runs the ignored set explicitly.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use flowgen_core::event::{
    new_completion_channel, Event, EventBuilder, EventData, SharedCompletionTx,
};
use flowgen_core::task::context::TaskContext;
use flowgen_oci::sync::{OciSync, ProcessorBuilder};
use oci_client::client::{ClientConfig, ClientProtocol, Config, ImageLayer};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use sha2::{Digest, Sha256};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::mpsc;

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

fn title(path: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(
        "org.opencontainers.image.title".to_string(),
        path.to_string(),
    )])
}

/// The digest of a blob holding `content`.
fn blob_digest(content: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(content))
}

fn test_task_context() -> Arc<TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("Build the TaskManager"),
    );
    let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
        as Arc<dyn flowgen_core::cache::Cache>;
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("Build the TaskContext"),
    )
}

fn config(artifact: String) -> Arc<OciSync> {
    Arc::new(OciSync {
        name: "pull".to_string(),
        artifact,
        ..Default::default()
    })
}

/// Boots a fresh registry container and returns `(container, host)` so
/// the container stays alive for the test's lifetime. The host port is
/// reserved on 127.0.0.1 first, since one Docker picks itself can already
/// be held there by another process, and it is polled until it serves,
/// which can lag the container's own listening log.
async fn boot_registry() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("Bind a free host port");
    let port = listener
        .local_addr()
        .expect("Read the bound address")
        .port();
    drop(listener);
    let registry = GenericImage::new("registry", "2.8.3")
        .with_wait_for(WaitFor::message_on_stderr("listening on"))
        .with_mapped_port(port, 5000.tcp())
        .start()
        .await
        .expect("Start the registry container");
    let host = format!("127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !matches!(
        tokio::time::timeout(Duration::from_secs(1), serves(&host)).await,
        Ok(true)
    ) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "The registry serves on {host} within 30s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (registry, host)
}

/// Whether the registry on `host` answers `GET /v2/`.
async fn serves(host: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    match tokio::net::TcpStream::connect(host).await {
        Ok(mut stream) => {
            let request = format!("GET /v2/ HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
            let mut status = [0u8; 12];
            stream.write_all(request.as_bytes()).await.is_ok()
                && stream.read_exact(&mut status).await.is_ok()
                && &status == b"HTTP/1.1 200"
        }
        Err(_) => false,
    }
}

fn push_client() -> Client {
    Client::new(ClientConfig {
        protocol: ClientProtocol::Http,
        ..Default::default()
    })
}

/// Pushes `layers` with an empty config under `<host>/<repo>:latest` and
/// returns the reference.
async fn push(host: &str, repo: &str, layers: &[ImageLayer]) -> String {
    let reference: Reference = format!("{host}/{repo}:latest")
        .parse()
        .expect("Parse the reference");
    push_client()
        .push(
            &reference,
            layers,
            Config::oci_v1(b"{}".to_vec(), None),
            &RegistryAuth::Anonymous,
            None,
        )
        .await
        .expect("Push the artifact");
    reference.to_string()
}

/// Pushes a single untitled layer; `media_type` picks raw, tar or tar+gzip.
async fn push_layer(host: &str, repo: &str, media_type: &str, bytes: Vec<u8>) -> String {
    push(
        host,
        repo,
        &[ImageLayer::new(bytes, media_type.to_string(), None)],
    )
    .await
}

/// Pushes untitled tar+gzip layers, so `oci_sync` merges them as Docker
/// image layers.
async fn push_docker_layers(host: &str, repo: &str, layers: &[Vec<u8>]) -> String {
    let layers: Vec<ImageLayer> = layers
        .iter()
        .map(|bytes| {
            ImageLayer::new(
                bytes.clone(),
                "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                None,
            )
        })
        .collect();
    push(host, repo, &layers).await
}

/// The digest the registry reports for the manifest `artifact` points at.
async fn manifest_digest(artifact: &str) -> String {
    push_client()
        .fetch_manifest_digest(
            &artifact.parse().expect("Parse the reference"),
            &RegistryAuth::Anonymous,
        )
        .await
        .expect("Fetch the manifest digest")
}

fn trigger(completion: SharedCompletionTx) -> Event {
    EventBuilder::new()
        .data(EventData::Json(serde_json::json!({"trigger": true})))
        .subject("tick".to_string())
        .task_id(0)
        .task_type("generate")
        .completion_tx(completion)
        .build()
        .expect("Build the trigger event")
}

/// Sends one trigger carrying `completion` through `oci_sync` and returns
/// every event it emits before it stops.
async fn sync(
    config: Arc<OciSync>,
    task_context: Arc<TaskContext>,
    completion: SharedCompletionTx,
) -> Vec<Event> {
    let (trigger_tx, trigger_rx) = mpsc::channel(1);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(16);

    let processor = ProcessorBuilder::new()
        .config(config)
        .receiver(trigger_rx)
        .sender(downstream_tx)
        .task_id(1)
        .task_type("oci_sync")
        .task_context(task_context)
        .build()
        .await
        .expect("Build the oci_sync processor");
    let handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        processor.run().await
    });

    trigger_tx
        .send(trigger(completion))
        .await
        .expect("Send the trigger event");
    drop(trigger_tx);

    let mut events = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(30), downstream_rx.recv())
        .await
        .expect("The sync stops within 30s")
    {
        events.push(event);
    }
    handle
        .await
        .expect("The sync does not panic")
        .expect("The sync runs");
    events
}

/// Completes `events` as a leaf would, failing them with `error` when
/// given, and gives the digest save it triggers time to run.
async fn complete(events: &[Event], error: Option<&str>) {
    for completion in events
        .iter()
        .filter_map(|event| event.completion_tx.as_ref())
    {
        match error {
            Some(error) => completion.signal_completion_with_error(error.to_string()),
            None => completion.signal_completion(None),
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Runs one sync and completes its events as a succeeding leaf would.
async fn run_once(config: Arc<OciSync>, task_context: Arc<TaskContext>) -> Vec<Event> {
    let events = sync(config, task_context, new_completion_channel(1).0).await;
    complete(&events, None).await;
    events
}

/// Runs one failing sync and returns the error of the trigger it forwards
/// once retries run out.
async fn sync_error(config: Arc<OciSync>) -> String {
    let events = sync(config, test_task_context(), new_completion_channel(1).0).await;
    assert_eq!(
        events.len(),
        1,
        "The failed trigger is forwarded once, got {}",
        events.len()
    );
    events[0]
        .error
        .clone()
        .expect("The forwarded trigger carries the error")
}

/// The data of the one snapshot in `events`.
fn snapshot_of(events: &[Event]) -> serde_json::Value {
    assert_eq!(
        events.len(),
        1,
        "One snapshot per pull, got {}",
        events.len()
    );
    events[0].data_as_json().expect("The snapshot is JSON")
}

fn files_of(snapshot: &serde_json::Value) -> &[serde_json::Value] {
    snapshot["files"]
        .as_array()
        .expect("The snapshot lists files")
}

fn file_at<'a>(snapshot: &'a serde_json::Value, path: &str) -> &'a serde_json::Value {
    files_of(snapshot)
        .iter()
        .find(|file| file["path"] == path)
        .unwrap_or_else(|| panic!("The snapshot holds {path}"))
}

/// The content of each file of `snapshot` by path, all of them text.
fn text_files_of(snapshot: &serde_json::Value) -> BTreeMap<&str, &str> {
    files_of(snapshot)
        .iter()
        .map(|file| {
            (
                file["path"].as_str().expect("A file has a path"),
                file["content"].as_str().expect("A text file has content"),
            )
        })
        .collect()
}

fn decoded(file: &serde_json::Value) -> Vec<u8> {
    let encoded = file["content_base64"]
        .as_str()
        .expect("A binary file has content_base64");
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("The content_base64 is standard base64")
}

/// The GHCR artifact shape: an empty config plus one layer per file, each
/// titled with its path.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_emits_one_snapshot_of_titled_layers_with_the_trigger_completion() {
    let (_registry, host) = boot_registry().await;
    let layers = [
        ImageLayer::new(
            b"select 1\n".to_vec(),
            "application/octet-stream".to_string(),
            Some(title("queries/a.sql")),
        ),
        ImageLayer::new(
            br#"{ "fields": [] }"#.to_vec(),
            "application/octet-stream".to_string(),
            Some(title("schemas/b.json")),
        ),
    ];
    let artifact = push(&host, "flowgen/resources", &layers).await;

    let (completion, mut completed) = new_completion_channel(1);
    let events = sync(
        config(artifact.clone()),
        test_task_context(),
        Arc::clone(&completion),
    )
    .await;
    let snapshot = snapshot_of(&events);

    assert_eq!(
        snapshot["digest"],
        manifest_digest(&artifact).await.as_str(),
        "The snapshot carries the manifest digest"
    );
    let paths: Vec<&str> = files_of(&snapshot)
        .iter()
        .map(|file| file["path"].as_str().expect("A file has a path"))
        .collect();
    assert_eq!(
        paths,
        ["queries/a.sql", "schemas/b.json"],
        "Layer titles become the paths, in manifest order"
    );
    assert_eq!(
        text_files_of(&snapshot),
        BTreeMap::from([
            ("queries/a.sql", "select 1\n"),
            ("schemas/b.json", r#"{ "fields": [] }"#),
        ]),
        "Each layer's bytes become its file's content"
    );
    for (file, layer) in files_of(&snapshot).iter().zip(&layers) {
        assert_eq!(
            file["digest"],
            layer.sha256_digest().as_str(),
            "Each file carries the digest of its layer"
        );
    }

    assert!(
        events[0]
            .completion_tx
            .as_ref()
            .is_some_and(|tx| Arc::ptr_eq(tx, &completion)),
        "The snapshot carries the trigger's completion"
    );
    assert!(
        completed.try_recv().is_err(),
        "The trigger is not completed before the snapshot is"
    );
    complete(&events, None).await;
    assert!(
        matches!(completed.try_recv(), Ok(Ok(None))),
        "Completing the snapshot completes the trigger"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_unpacks_a_tar_gzip_layer_into_snapshot_files() {
    let (_registry, host) = boot_registry().await;
    let layer = gzip_bytes(&build_tar_bytes(&[
        ("a.yaml", b"name: a\n"),
        ("nested/b.yaml", b"name: b\n"),
    ]));
    let artifact = push_layer(
        &host,
        "flowgen/docker-image",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        layer.clone(),
    )
    .await;

    let snapshot = snapshot_of(&run_once(config(artifact), test_task_context()).await);

    assert_eq!(
        text_files_of(&snapshot),
        BTreeMap::from([("a.yaml", "name: a\n"), ("nested/b.yaml", "name: b\n")]),
        "Tar entry paths and contents round-trip through the extractor"
    );
    for file in files_of(&snapshot) {
        assert_eq!(
            file["digest"],
            blob_digest(&layer).as_str(),
            "Each tar entry carries the digest of its layer"
        );
    }
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_unpacks_uncompressed_tar_layer() {
    let (_registry, host) = boot_registry().await;
    let artifact = push_layer(
        &host,
        "flowgen/plain-tar",
        "application/vnd.oci.image.layer.v1.tar",
        build_tar_bytes(&[
            ("flows/a.yaml", b"name: a\n"),
            ("flows/b.yaml", b"name: b\n"),
        ]),
    )
    .await;

    let snapshot = snapshot_of(&run_once(config(artifact), test_task_context()).await);

    assert_eq!(
        text_files_of(&snapshot),
        BTreeMap::from([("flows/a.yaml", "name: a\n"), ("flows/b.yaml", "name: b\n")]),
        "An uncompressed tar unpacks the same as tar+gzip"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_skips_directory_symlink_hardlink_and_whiteout_entries() {
    let (_registry, host) = boot_registry().await;
    let tar_bytes = build_tar_with_types(&[
        ("flows/", tar::EntryType::Directory, b""),
        ("flows/real.yaml", tar::EntryType::Regular, b"name: real\n"),
        ("flows/link.yaml", tar::EntryType::Symlink, b"real.yaml"),
        ("flows/hardlink.yaml", tar::EntryType::Link, b"real.yaml"),
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
    let artifact = push_layer(
        &host,
        "flowgen/skip-entries",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        gzip_bytes(&tar_bytes),
    )
    .await;

    let snapshot = snapshot_of(&run_once(config(artifact), test_task_context()).await);

    assert_eq!(
        text_files_of(&snapshot),
        BTreeMap::from([
            ("flows/real.yaml", "name: real\n"),
            ("processors/other.yaml", "kind: x\n"),
        ]),
        "Directory, symlink, hardlink and .wh.* whiteout entries are skipped"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_rejects_layer_exceeding_max_file_size() {
    let (_registry, host) = boot_registry().await;
    let big_file = vec![b'a'; 2048];
    let artifact = push_layer(
        &host,
        "flowgen/too-big-file",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        gzip_bytes(&build_tar_bytes(&[("large.yaml", &big_file)])),
    )
    .await;

    let err = sync_error(Arc::new(OciSync {
        name: "pull".to_string(),
        artifact,
        max_file_size: 1024,
        max_total_size: 10 * 1024 * 1024,
        ..Default::default()
    }))
    .await;
    assert!(
        err.contains("max_file_size") || err.contains("FileTooLarge"),
        "The error names the per-file cap, got: {err}"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_rejects_artifact_exceeding_max_total_size() {
    let (_registry, host) = boot_registry().await;
    // Each file fits the per-file cap, together they exceed the total cap.
    let payload_a = vec![b'a'; 800];
    let payload_b = vec![b'b'; 800];
    let artifact = push_layer(
        &host,
        "flowgen/too-big-total",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        gzip_bytes(&build_tar_bytes(&[
            ("a.yaml", &payload_a),
            ("b.yaml", &payload_b),
        ])),
    )
    .await;

    let err = sync_error(Arc::new(OciSync {
        name: "pull".to_string(),
        artifact,
        max_file_size: 4096,
        max_total_size: 1024,
        ..Default::default()
    }))
    .await;
    assert!(
        err.contains("max_total_size") || err.contains("ArtifactTooLarge"),
        "The error names the cumulative cap, got: {err}"
    );
}

/// A non-UTF-8 raw layer, such as a WASM module, is kept as base64 instead
/// of failing the pull.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_base64_encodes_a_binary_raw_layer() {
    let (_registry, host) = boot_registry().await;
    let binary: Vec<u8> = vec![0x80, 0xFF, 0xC3, 0x28, 0xE2, 0x82, 0x28];
    assert!(
        std::str::from_utf8(&binary).is_err(),
        "The payload is invalid UTF-8"
    );
    let artifact = push_layer(
        &host,
        "flowgen/binary-raw",
        "application/octet-stream",
        binary.clone(),
    )
    .await;

    let snapshot = snapshot_of(&run_once(config(artifact), test_task_context()).await);

    assert_eq!(files_of(&snapshot).len(), 1, "One file for the raw layer");
    let raw = file_at(&snapshot, "layer-0");
    assert!(raw.get("content").is_none(), "A binary file has no content");
    assert_eq!(
        decoded(raw),
        binary,
        "The content_base64 holds the layer bytes"
    );
    assert_eq!(
        raw["digest"],
        blob_digest(&binary).as_str(),
        "The file carries the digest of its layer"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_keeps_text_and_base64_encodes_binary_tar_entries() {
    let (_registry, host) = boot_registry().await;
    let binary: Vec<u8> = vec![0x80, 0xFF, 0xC3, 0x28];
    assert!(
        std::str::from_utf8(&binary).is_err(),
        "The payload is invalid UTF-8"
    );
    let artifact = push_layer(
        &host,
        "flowgen/mixed-tar",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        gzip_bytes(&build_tar_bytes(&[
            ("flows/text.yaml", b"name: text\n"),
            ("blobs/module.wasm", &binary),
        ])),
    )
    .await;

    let snapshot = snapshot_of(&run_once(config(artifact), test_task_context()).await);

    assert_eq!(files_of(&snapshot).len(), 2, "One file per tar entry");
    let text = file_at(&snapshot, "flows/text.yaml");
    assert_eq!(text["content"], "name: text\n", "A text file has content");
    assert!(
        text.get("content_base64").is_none(),
        "A text file has no content_base64"
    );
    let wasm = file_at(&snapshot, "blobs/module.wasm");
    assert!(
        wasm.get("content").is_none(),
        "A binary file has no content"
    );
    assert_eq!(
        decoded(wasm),
        binary,
        "The content_base64 holds the entry bytes"
    );
}

/// Docker image layers merge with overlay semantics: later layers override
/// earlier ones and `.wh.<name>` markers delete files.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_merges_docker_layers_with_whiteout_and_override() {
    let (_registry, host) = boot_registry().await;
    let lower = gzip_bytes(&build_tar_bytes(&[
        ("flows/a.yaml", b"v1: a"),
        ("flows/b.yaml", b"v1: b"),
        ("configs/keep.yaml", b"v1: keep"),
    ]));
    let upper = gzip_bytes(&build_tar_bytes(&[
        ("flows/a.yaml", b"v2: a"),
        ("flows/.wh.b.yaml", b""),
    ]));
    let artifact = push_docker_layers(
        &host,
        "flowgen/docker-merge",
        &[lower.clone(), upper.clone()],
    )
    .await;

    let snapshot = snapshot_of(&run_once(config(artifact), test_task_context()).await);

    assert_eq!(
        text_files_of(&snapshot),
        BTreeMap::from([("configs/keep.yaml", "v1: keep"), ("flows/a.yaml", "v2: a")]),
        "The upper layer overrides a.yaml, deletes b.yaml and keeps keep.yaml"
    );
    assert_eq!(
        file_at(&snapshot, "flows/a.yaml")["digest"],
        blob_digest(&upper).as_str(),
        "An overridden file carries the digest of the upper layer"
    );
    assert_eq!(
        file_at(&snapshot, "configs/keep.yaml")["digest"],
        blob_digest(&lower).as_str(),
        "A kept file carries the digest of the lower layer"
    );
}

/// An opaque whiteout (`.wh..wh..opq`) hides every file of its directory
/// from lower layers.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_docker_opaque_whiteout_hides_lower_directory() {
    let (_registry, host) = boot_registry().await;
    let lower = gzip_bytes(&build_tar_bytes(&[
        ("hidden/one.yaml", b"one"),
        ("hidden/two.yaml", b"two"),
        ("public/keep.yaml", b"keep"),
    ]));
    let upper = gzip_bytes(&build_tar_bytes(&[
        ("hidden/.wh..wh..opq", b""),
        ("hidden/fresh.yaml", b"fresh"),
    ]));
    let artifact = push_docker_layers(&host, "flowgen/docker-opaque", &[lower, upper]).await;

    let snapshot = snapshot_of(&run_once(config(artifact), test_task_context()).await);

    assert_eq!(
        text_files_of(&snapshot),
        BTreeMap::from([("hidden/fresh.yaml", "fresh"), ("public/keep.yaml", "keep")]),
        "The opaque whiteout hides the lower hidden/ files, keeps the fresh one and the sibling directory"
    );
}

/// The digest cached is the one HEAD reports, so for a multi-arch tag it
/// matches the index digest the next HEAD returns.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_second_tick_skips_when_manifest_unchanged() {
    let (_registry, host) = boot_registry().await;
    let artifact = push_layer(
        &host,
        "flowgen/skip-when-unchanged",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        gzip_bytes(&build_tar_bytes(&[("flows/one.yaml", b"content")])),
    )
    .await;
    let config = config(artifact);
    let ctx = test_task_context();

    let first = run_once(Arc::clone(&config), Arc::clone(&ctx)).await;
    snapshot_of(&first);

    let second = run_once(config, ctx).await;
    assert!(
        second.is_empty(),
        "The second tick against an unchanged tag emits nothing, got {}",
        second.len()
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn oci_sync_remembers_the_digest_only_once_the_flow_succeeded() {
    let (_registry, host) = boot_registry().await;
    let artifact = push_layer(
        &host,
        "flowgen/cursor",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        gzip_bytes(&build_tar_bytes(&[("a.txt", b"a")])),
    )
    .await;
    let config = config(artifact);
    let ctx = test_task_context();

    let failed = sync(
        Arc::clone(&config),
        Arc::clone(&ctx),
        new_completion_channel(1).0,
    )
    .await;
    snapshot_of(&failed);
    complete(&failed, Some("boom")).await;

    let retried = sync(
        Arc::clone(&config),
        Arc::clone(&ctx),
        new_completion_channel(1).0,
    )
    .await;
    assert_eq!(
        retried.len(),
        1,
        "A failed flow leaves the digest unsaved, so the snapshot is emitted again"
    );
    complete(&retried, None).await;

    let unchanged = sync(config, ctx, new_completion_channel(1).0).await;
    assert!(
        unchanged.is_empty(),
        "A succeeded flow saves the digest, so the unchanged artifact emits nothing, got {}",
        unchanged.len()
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_pushed_workspace_is_read_back_by_oci_sync_under_every_tag() {
    use flowgen_oci::push::client::{ArtifactFile, Push};

    let (_registry, host) = boot_registry().await;
    let push = Push {
        repository: format!("{host}/flowgen/workspace"),
        credentials_path: None,
        annotations: BTreeMap::from([(
            "org.opencontainers.image.revision".to_string(),
            "abc123".to_string(),
        )]),
    };
    let files = vec![
        ArtifactFile {
            path: "flows/a.yaml".to_string(),
            content: b"name: a\n".to_vec(),
        },
        ArtifactFile {
            path: "resources/scripts/s.rhai".to_string(),
            content: b"event".to_vec(),
        },
    ];

    let pushed = push
        .push(files, &["abc123".to_string(), "latest".to_string()])
        .await
        .expect("Push the workspace");
    assert!(pushed.digest.starts_with("sha256:"));

    let reference: Reference = format!("{host}/flowgen/workspace:latest")
        .parse()
        .expect("Parse the reference");
    let (manifest, digest) = push_client()
        .pull_image_manifest(&reference, &RegistryAuth::Anonymous)
        .await
        .expect("Pull the pushed manifest");
    assert_eq!(digest, pushed.digest);
    assert_eq!(
        manifest.artifact_type.as_deref(),
        Some(flowgen_oci::push::client::ARTIFACT_TYPE)
    );
    assert_eq!(
        manifest.config.media_type,
        flowgen_oci::push::client::EMPTY_MEDIA_TYPE
    );
    assert_eq!(
        manifest
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("org.opencontainers.image.revision"))
            .map(String::as_str),
        Some("abc123")
    );
    assert_eq!(manifest.layers.len(), 1, "The workspace is one layer");

    for tag in ["abc123", "latest"] {
        let events = run_once(
            config(format!("{host}/flowgen/workspace:{tag}")),
            test_task_context(),
        )
        .await;
        let snapshot = snapshot_of(&events);
        assert_eq!(snapshot["digest"], pushed.digest.as_str(), "Tag {tag}");
        assert_eq!(
            text_files_of(&snapshot),
            BTreeMap::from([
                ("flows/a.yaml", "name: a\n"),
                ("resources/scripts/s.rhai", "event"),
            ]),
            "Tag {tag}"
        );
        for file in files_of(&snapshot) {
            assert_eq!(
                file["digest"],
                manifest.layers[0].digest.as_str(),
                "Tag {tag}: each file carries the digest of the workspace layer"
            );
        }
    }
}
