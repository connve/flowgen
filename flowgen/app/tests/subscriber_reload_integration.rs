//! A flow with `nats_jetstream_subscribe` on an existing durable is reloaded
//! while a message is in its flow: the message must go to the reloaded flow,
//! later messages must be processed, and the durable must take the reloaded
//! flow's settings.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use async_nats::jetstream;
use flowgen::config::{FlowConfig, FlowConfigRaw};
use flowgen::flow::FlowBuilder;
use flowgen_core::cache::memory::MemoryCache;
use flowgen_core::cache::Cache;
use flowgen_core::identity::FlowIdentity;
use flowgen_core::peer::PeerRegistry;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const STREAM: &str = "triggers";
const DURABLE: &str = "audience_runner";

async fn start_nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.11.8-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start nats container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("map nats port");
    (container, format!("nats://127.0.0.1:{port}"))
}

/// A flow reading the durable, with `then` as the task after the subscriber.
fn flow(url: &str, ack_timeout: &str, then: &str) -> Arc<FlowConfig> {
    flow_led(url, ack_timeout, then, false)
}

fn flow_led(url: &str, ack_timeout: &str, then: &str, leader_elected: bool) -> Arc<FlowConfig> {
    let yaml = format!(
        r#"
flow:
  name: runner
  require_leader_election: {leader_elected}
  tasks:
    - nats_jetstream_subscribe:
        name: consume_trigger
        url: {url}
        subject: triggers.run
        durable_name: {DURABLE}
        stream:
          name: {STREAM}
          create_or_update: false
        max_in_flight: 1
        max_ack_pending: 10
        ack_timeout: {ack_timeout}
        batch_expires: 1s
{then}
"#
    );
    let value: serde_json::Value = serde_yaml::from_str(&yaml).expect("flow yaml");
    let raw: FlowConfigRaw = serde_json::from_value(value).expect("flow config");
    Arc::new(FlowConfig::from_path(raw, "runner".to_string(), None).expect("flow config"))
}

/// Starts the flow and returns how to stop it and wait for it.
async fn start(
    config: Arc<FlowConfig>,
    cache: &Arc<dyn Cache>,
) -> (
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    start_on(config, cache, None).await
}

/// Starts the flow on the pod named by `pod`, which joins the pods sharing `cache`.
async fn start_on(
    config: Arc<FlowConfig>,
    cache: &Arc<dyn Cache>,
    pod: Option<Arc<PeerRegistry>>,
) -> (
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let mut builder = FlowBuilder::new()
        .config(config)
        .cache(Arc::clone(cache))
        .system_cache(Arc::clone(cache))
        .client_registry(Arc::new(
            flowgen_core::client_registry::ClientRegistry::new(),
        ));
    if let Some(pod) = pod {
        builder = builder
            .holder_identity(pod.identity().to_string())
            .peer_registry(pod);
    }
    let mut flow = builder.build().expect("build flow");
    flow.init().await.expect("init flow");
    let stop = flow.cancellation_token();
    (stop, flow.run())
}

async fn publish(context: &jetstream::Context, n: u32) {
    context
        .publish("triggers.run", format!(r#"{{"n": {n}}}"#).into())
        .await
        .expect("publish")
        .await
        .expect("publish ack");
}

async fn consumer_info(context: &jetstream::Context) -> jetstream::consumer::Info {
    let mut consumer: jetstream::consumer::PullConsumer = context
        .get_stream(STREAM)
        .await
        .expect("stream")
        .get_consumer(DURABLE)
        .await
        .expect("consumer");
    consumer.info().await.expect("consumer info").clone()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_reloaded_flow_gets_the_message_in_flight_and_updates_the_durable() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
    let (_nats, url) = start_nats().await;
    let context = jetstream::new(async_nats::connect(&url).await.expect("connect"));
    context
        .create_stream(jetstream::stream::Config {
            name: STREAM.to_string(),
            subjects: vec!["triggers.>".to_string()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    context
        .get_stream(STREAM)
        .await
        .expect("stream")
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some(DURABLE.to_string()),
            filter_subject: "triggers.run".to_string(),
            ack_wait: Duration::from_secs(5),
            backoff: vec![Duration::from_secs(5), Duration::from_secs(30)],
            max_ack_pending: 10,
            ..Default::default()
        })
        .await
        .expect("create the durable as an older version left it");
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new());

    let hanging = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a server that never answers");
    let hanging_port = hanging.local_addr().expect("server address").port();
    let held = tokio::spawn(async move {
        let mut connections = Vec::new();
        while let Ok((connection, _)) = hanging.accept().await {
            connections.push(connection);
        }
    });
    let stuck = format!(
        r#"
    - http_request:
        name: stuck
        endpoint: https://127.0.0.1:{hanging_port}/run
        method: POST
"#
    );
    let (stop, running) = start(flow(&url, "900s", &stuck), &cache).await;
    publish(&context, 1).await;
    let mut in_flight = false;
    for _ in 0..50 {
        if consumer_info(&context).await.num_ack_pending == 1 {
            in_flight = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(in_flight, "the first flow holds the message");

    stop.cancel();
    tokio::time::timeout(Duration::from_secs(40), running)
        .await
        .expect("the first flow stops")
        .expect("the first flow did not panic");

    let record = r#"
    - script:
        name: record
        code: |
          ctx.cache.put("seen." + event.data.n.to_string(), "1", 300);
          event
"#;
    let (stop, running) = start(flow(&url, "900s", record), &cache).await;
    publish(&context, 2).await;
    publish(&context, 3).await;

    let mut seen = Vec::new();
    for _ in 0..75 {
        let keys = cache.list_keys("").await.expect("list keys");
        seen = (1..=3)
            .filter(|n| keys.iter().any(|key| key.ends_with(&format!("seen.{n}"))))
            .collect();
        if seen.len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let info = consumer_info(&context).await;
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(40), running).await;
    held.abort();

    assert_eq!(
        seen,
        [1, 2, 3],
        "the reloaded flow processes the message that was in flight and the later ones"
    );
    assert_eq!(
        info.num_ack_pending, 0,
        "no message is left waiting for an ack"
    );
    assert_eq!(info.config.ack_wait, Duration::from_secs(900));
    assert!(info.config.backoff.is_empty());
}

/// Seen marks of the `record` task, among 1..=`up_to`.
async fn seen(cache: &Arc<dyn Cache>, up_to: u32) -> Vec<u32> {
    let keys = cache.list_keys("").await.expect("list keys");
    (1..=up_to)
        .filter(|n| keys.iter().any(|key| key.ends_with(&format!("seen.{n}"))))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_flow_that_loses_its_leader_hands_the_message_in_flight_to_the_new_one() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,flowgen=debug,flowgen_core=debug")
        .with_test_writer()
        .try_init();
    let (_nats, url) = start_nats().await;
    let context = jetstream::new(async_nats::connect(&url).await.expect("connect"));
    context
        .create_stream(jetstream::stream::Config {
            name: STREAM.to_string(),
            subjects: vec!["triggers.>".to_string()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    context
        .get_stream(STREAM)
        .await
        .expect("stream")
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some(DURABLE.to_string()),
            filter_subject: "triggers.run".to_string(),
            ack_wait: Duration::from_secs(900),
            ..Default::default()
        })
        .await
        .expect("create the durable");
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new());
    let pod_a = Arc::new(PeerRegistry::new(Arc::clone(&cache), "pod-a".to_string()));
    pod_a.register().await.expect("pod-a registers");
    let pods_alive = tokio_util::sync::CancellationToken::new();
    pod_a.spawn_renewal(pods_alive.clone());
    let flow_key = FlowIdentity::new("runner").as_key();
    let pod_b_name = (0..)
        .map(|i| format!("pod-b{i}"))
        .find(|name| {
            let mut pods = vec!["pod-a".to_string(), name.clone()];
            pods.sort();
            flowgen_core::peer::preferred_peer(&flow_key, &pods) == Some(name.as_str())
        })
        .expect("a pod the flow prefers");
    let pod_b = Arc::new(PeerRegistry::new(Arc::clone(&cache), pod_b_name));

    let hanging = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a server that never answers");
    let hanging_port = hanging.local_addr().expect("server address").port();
    let held = tokio::spawn(async move {
        let mut connections = Vec::new();
        while let Ok((connection, _)) = hanging.accept().await {
            connections.push(connection);
        }
    });
    let stuck = format!(
        r#"
    - http_request:
        name: stuck
        endpoint: https://127.0.0.1:{hanging_port}/run
        method: POST
"#
    );
    let (stop_a, running_a) = start_on(
        flow_led(&url, "900s", &stuck, true),
        &cache,
        Some(Arc::clone(&pod_a)),
    )
    .await;
    publish(&context, 1).await;
    let mut in_flight = false;
    for _ in 0..150 {
        if consumer_info(&context).await.num_ack_pending == 1 {
            in_flight = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(in_flight, "pod-a leads the flow and holds the message");

    pod_b.register().await.expect("pod-b registers");
    pod_b.mark_ready().await.expect("pod-b is ready");
    pod_b.spawn_renewal(pods_alive.clone());
    let record = r#"
    - script:
        name: record
        code: |
          ctx.cache.put("seen." + event.data.n.to_string(), "1", 300);
          event
"#;
    let (stop_b, running_b) = start_on(
        flow_led(&url, "900s", record, true),
        &cache,
        Some(Arc::clone(&pod_b)),
    )
    .await;

    let mut handed_over = false;
    for _ in 0..600 {
        if seen(&cache, 1).await == [1] {
            handed_over = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    publish(&context, 2).await;
    let mut later = Vec::new();
    for _ in 0..75 {
        later = seen(&cache, 2).await;
        if later.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop_a.cancel();
    stop_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(40), running_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(40), running_b).await;
    pods_alive.cancel();
    held.abort();

    assert!(
        handed_over,
        "pod-b gets the message pod-a held within two minutes, not after the 900s ack wait"
    );
    assert_eq!(later, [1, 2], "pod-b processes the later messages");
}
