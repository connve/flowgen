//! Flows with full channels: a big fan-out still reaches the end, and stopping
//! a flow whose tasks are stuck on full channels doesn't hang.

use flowgen::config::{FlowConfig, FlowConfigRaw};
use flowgen::flow::{Flow, FlowBuilder};
use flowgen_core::cache::memory::MemoryCache;
use flowgen_core::cache::Cache;
use std::sync::Arc;
use std::time::Duration;

fn flow(yaml: &str, cache: &Arc<dyn Cache>) -> Flow {
    let raw = FlowConfigRaw::parse("a.yaml", yaml).unwrap();
    let config = FlowConfig::from_path(raw, "a".to_string(), None).unwrap();
    FlowBuilder::new()
        .config(Arc::new(config))
        .cache(Arc::clone(cache))
        .system_cache(Arc::clone(cache))
        .client_registry(Arc::new(
            flowgen_core::client_registry::ClientRegistry::new(),
        ))
        .build()
        .unwrap()
}

async fn cached(cache: &Arc<dyn Cache>, suffix: &str) -> Option<String> {
    let keys = cache.list_keys("").await.unwrap();
    let key = keys.iter().find(|key| key.ends_with(suffix))?;
    match cache.get(key).await {
        Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).to_string()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fan_out_larger_than_a_channel_reaches_the_end_through_a_buffer() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new());
    let mut flow = flow(
        r#"
flow:
  name: a
  tasks:
    - generate:
        name: trigger
        interval: 10ms
        count: 1
    - script:
        name: source
        code: |
          let items = [];
          for i in 0..1000 { items.push(i); }
          event.data = items;
          event
    - iterate:
        name: each
    - buffer:
        name: batch
        size: 100
        flush_on_completion: true
    - script:
        name: count
        max_in_flight: 1
        code: |
          let seen = ctx.cache.get("count.seen");
          let total = if seen == () { 0 } else { parse_int(seen) };
          ctx.cache.put("count.seen", (total + event.data.batch_size).to_string(), 300);
          event
"#,
        &cache,
    );
    flow.init().await.unwrap();
    let _handles = flow.spawn_all_tasks().await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut seen = None;
    while tokio::time::Instant::now() < deadline {
        seen = cached(&cache, "count.seen").await;
        if seen.as_deref() == Some("1000") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(seen.as_deref(), Some("1000"));
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_a_flow_whose_tasks_wait_on_full_channels_finishes() {
    let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new());
    let mut flow = flow(
        r#"
flow:
  name: a
  tasks:
    - generate:
        name: trigger
        interval: 1ms
    - script:
        name: pass
        code: event
    - script:
        name: slow
        max_in_flight: 1
        code: |
          let total = 0;
          for i in 0..20000 { total += i; }
          event
"#,
        &cache,
    );
    flow.init().await.unwrap();
    let handles = flow.spawn_all_tasks().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    handles.task_context.cancellation_token.cancel();
    let stopped = tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join_all(handles.background_handles),
    )
    .await;

    assert!(
        stopped.is_ok(),
        "every task stops once the flow is cancelled"
    );
}
