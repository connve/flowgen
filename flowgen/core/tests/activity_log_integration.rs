//! Integration test for how `FlowActivityLayer` backfills `duration_ms` on
//! `task.handle` spans in formatted log lines.
//!
//! The layer records the field through `Span::current()` from inside
//! `on_event`, which only reaches the formatter under a global dispatcher;
//! a scoped one (`set_default`) turns the nested call into a no-op. The
//! global dispatcher can be installed once per process, so this lives in its
//! own test binary with a single test.
//!
//! No external dependency, not `#[ignore]`d.

use flowgen_core::flow::activity::OtlpMetricsStore;
use flowgen_core::flow::activity_layer::FlowActivityLayer;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing::{info, info_span};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;

#[derive(Clone, Default)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.0.lock() {
            Ok(mut inner) => inner.write(buf),
            Err(_) => Err(std::io::Error::other("log buffer poisoned")),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn records_duration_once_per_task_handle_span() {
    let buf = SharedBuf::default();
    let writer = buf.clone();
    Registry::default()
        .with(FlowActivityLayer::new(OtlpMetricsStore::builder().build()))
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(false)
                .with_writer(move || writer.clone()),
        )
        .init();

    let flow_span = info_span!("flow.run", flow = "demo");
    let _flow = flow_span.enter();
    let handle_span = info_span!("task.handle", duration_ms = tracing::field::Empty);
    let _handle = handle_span.enter();
    info!("first");
    info!("second");
    info!("third");

    let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 3, "unexpected log output: {output}");
    assert!(
        output.contains("duration_ms="),
        "duration_ms not recorded: {output}"
    );
    for line in lines {
        assert!(
            line.matches("duration_ms=").count() <= 1,
            "duration_ms repeated: {line}"
        );
    }
}
