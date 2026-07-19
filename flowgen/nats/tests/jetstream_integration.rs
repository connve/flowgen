//! Integration tests for the NATS JetStream Publisher / Subscriber
//! processors against a real NATS server in a Docker container.
//!
//! Covers the end-to-end round-trip an operator's YAML flow exercises:
//! publish an event onto a subject, consume it back via a durable
//! pull-based subscriber, and check that the flow-completion channel
//! wires up so the subscriber's ack fires only after the downstream
//! leaves signal completion.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use flowgen_core::event::{EventBuilder, EventData};
use flowgen_nats::jetstream::config::{
    Config as JsConfig, DiscardPolicy, RetentionPolicy, StreamOptions,
};
use flowgen_nats::jetstream::publisher::PublisherBuilder;
use flowgen_nats::jetstream::subscriber::SubscriberBuilder;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;

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

fn stream_options(name: &str, subject: &str) -> StreamOptions {
    StreamOptions {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        create_or_update: true,
        retention: Some(RetentionPolicy::WorkQueue),
        discard: Some(DiscardPolicy::Old),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn publisher_writes_event_to_stream() {
    let (_nats, url) = start_nats().await;
    let stream = stream_options("pub_only_stream", "pub.only");

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "pub.only".to_string(),
        stream: Some(stream),
        ..Default::default()
    });

    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, mut out_rx) = mpsc::channel(4);

    let publisher = PublisherBuilder::new()
        .config(pub_config)
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(0)
        .task_type("nats_jetstream_publisher")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build publisher");

    let handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = publisher.run().await;
    });

    let event = EventBuilder::new()
        .data(EventData::Json(serde_json::json!({"hello": "world"})))
        .subject("pub.only".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    in_tx.send(event).await.expect("send event");

    // Publisher emits an ack event downstream after a successful publish.
    let ack_event = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("ack event within timeout")
        .expect("channel open");
    let ack = ack_event.data_as_json().expect("ack data as json");
    assert_eq!(
        ack.get("stream").and_then(|s| s.as_str()),
        Some("pub_only_stream")
    );
    assert!(ack.get("sequence").and_then(|s| s.as_u64()).is_some());

    drop(in_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn publisher_and_subscriber_round_trip_delivers_the_message() {
    let (_nats, url) = start_nats().await;

    let stream = stream_options("rt_stream", "rt.subject");

    // Publisher side.
    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "rt.subject".to_string(),
        stream: Some(stream.clone()),
        ..Default::default()
    });
    let (pub_tx, pub_rx) = mpsc::channel(4);
    let (pub_out_tx, _pub_out_rx) = mpsc::channel(4);
    let publisher = PublisherBuilder::new()
        .config(pub_config)
        .receiver(pub_rx)
        .sender(pub_out_tx)
        .task_id(0)
        .task_type("nats_jetstream_publisher")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build publisher");

    // Subscriber side — pull the same subject through a durable
    // consumer on the same stream.
    let sub_config = Arc::new(JsConfig {
        name: "subscriber".to_string(),
        url: url.clone(),
        subject: "rt.subject".to_string(),
        stream: Some(stream),
        durable_name: Some("rt_consumer".to_string()),
        max_messages: Some(10),
        ack_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    });
    let (sub_out_tx, mut sub_out_rx) = mpsc::channel(4);
    let subscriber = SubscriberBuilder::new()
        .config(sub_config)
        .sender(sub_out_tx)
        .task_id(1)
        .task_type("nats_jetstream_subscriber")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build subscriber");

    let pub_handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = publisher.run().await;
    });
    let sub_handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = subscriber.run().await;
    });

    // Give the subscriber a moment to declare its durable consumer.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let event = EventBuilder::new()
        .data(EventData::Json(serde_json::json!({"payload": 42})))
        .subject("rt.subject".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    pub_tx.send(event).await.expect("publish event");

    let delivered = tokio::time::timeout(Duration::from_secs(10), sub_out_rx.recv())
        .await
        .expect("subscriber must deliver the message")
        .expect("channel open");

    // Signal completion so the subscriber acks the JetStream message
    // instead of holding it until ack_timeout fires.
    if let Some(arc) = delivered.completion_tx.as_ref() {
        arc.signal_completion(None);
    }

    // Subject is preserved end-to-end.
    assert_eq!(delivered.subject, "rt.subject");

    drop(pub_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    sub_handle.abort();
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscriber_completion_channel_wires_up_ack() {
    // Publish two messages; subscriber must deliver each one with a
    // completion_tx attached so downstream leaves can signal ack.
    // Without completion_tx the JetStream message would never get
    // acked and would sit as pending forever.
    let (_nats, url) = start_nats().await;
    let stream = stream_options("ack_stream", "ack.subject");

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "ack.subject".to_string(),
        stream: Some(stream.clone()),
        ..Default::default()
    });
    let sub_config = Arc::new(JsConfig {
        name: "subscriber".to_string(),
        url: url.clone(),
        subject: "ack.subject".to_string(),
        stream: Some(stream),
        durable_name: Some("ack_consumer".to_string()),
        max_messages: Some(10),
        ack_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    });

    let (pub_tx, pub_rx) = mpsc::channel(4);
    let (pub_out_tx, _pub_out_rx) = mpsc::channel(4);
    let publisher = PublisherBuilder::new()
        .config(pub_config)
        .receiver(pub_rx)
        .sender(pub_out_tx)
        .task_id(0)
        .task_type("nats_jetstream_publisher")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build publisher");

    let (sub_out_tx, mut sub_out_rx) = mpsc::channel(4);
    let subscriber = SubscriberBuilder::new()
        .config(sub_config)
        .sender(sub_out_tx)
        .task_id(1)
        .task_type("nats_jetstream_subscriber")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build subscriber");

    let pub_handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = publisher.run().await;
    });
    let sub_handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = subscriber.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 0..2 {
        let event = EventBuilder::new()
            .data(EventData::Json(serde_json::json!({"idx": i})))
            .subject("ack.subject".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .expect("build event");
        pub_tx.send(event).await.expect("publish event");
    }

    for _ in 0..2 {
        let delivered = tokio::time::timeout(Duration::from_secs(10), sub_out_rx.recv())
            .await
            .expect("subscriber must deliver a message")
            .expect("channel open");
        assert!(
            delivered.completion_tx.is_some(),
            "subscriber must attach a completion_tx so leaves can ack the message"
        );
        if let Some(arc) = delivered.completion_tx.as_ref() {
            arc.signal_completion(None);
        }
    }

    drop(pub_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    sub_handle.abort();
}
