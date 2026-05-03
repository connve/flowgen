# Telemetry

Flowgen ships with OpenTelemetry built in. When enabled, the worker exports metrics and distributed traces over OTLP/gRPC to any compatible collector — Tempo, Jaeger, Honeycomb, Datadog, Grafana Cloud, the OpenTelemetry Collector, and so on. Logs continue to flow through the standard tracing infrastructure (stderr by default).

## Configuration

```yaml
telemetry:
  enabled: true
  otlp_endpoint: "http://localhost:4317"
  service_name: flowgen
  metrics_export_interval: "60s"
```

| Field | Default | Description |
|---|---|---|
| `enabled` | required | Set `true` to start the OTLP exporter. When `false` or omitted, no telemetry is exported but tracing logs still go to stderr. |
| `otlp_endpoint` | `http://localhost:4317` | OTLP/gRPC endpoint of the collector. |
| `service_name` | `flowgen` | `service.name` resource attribute. Set this per-deployment so traces are easy to filter. |
| `metrics_export_interval` | `60s` | How often metric snapshots are pushed. Accepts human-readable durations: `30s`, `1m`, `5m`. |

When `telemetry` is omitted entirely, flowgen runs without any OTLP exporter — useful for local development.

## What gets exported

### Traces

Every task handler invocation produces a span. The span hierarchy mirrors the flow's task wiring: an event entering a source task creates a root span, and each downstream handler creates a child span linked through tracing context propagation.

Standard span names:

| Span | Where |
|---|---|
| `task.run` | Task lifecycle (init + event loop). One per task per worker tenure. |
| `task.handle` | A single event handler invocation. One per processed event per task. |
| `task_manager.start` | Worker-level task manager startup. |
| `task_manager.register` | Task registration. |
| `task_manager.shutdown` | Graceful shutdown. |

Standard span attributes on `task.handle` and `task.run`:

| Attribute | Description |
|---|---|
| `task` | Task name (from YAML). |
| `task_id` | Index in the flow's task list. |
| `task_type` | Task type (`script`, `http_request`, etc.). |

Connector-specific spans add their own attributes — request IDs, query handles, message offsets — so traces are searchable by external identifiers.

### Metrics

OpenTelemetry metrics are derived from tracing spans. Every span produces a duration histogram, and counters track invocation rate and error rate. Metric names follow the span hierarchy:

- `task.handle.duration` — per-event handler latency.
- `task.handle.count` — total invocations.
- `task.handle.errors` — invocations that returned an error after retries.

These appear in your collector with the `service.name` attribute set to whatever you configured — filter on it to isolate one flowgen deployment from the rest of your fleet.

### Logs

Application logs go to stderr through the standard `tracing` subscriber and are not pushed via OTLP. Use your container runtime's log shipping (Fluent Bit, Vector, Loki agents) to collect them. Spans and logs share the same context, so a span ID printed in a log line correlates exactly with the matching trace in the collector.

## Verifying the export

The simplest local setup is the OpenTelemetry Collector:

```yaml
# docker-compose.yml fragment
services:
  otel-collector:
    image: otel/opentelemetry-collector:latest
    ports:
      - "4317:4317"   # OTLP gRPC
    command: ["--config=/etc/otelcol/config.yaml"]
    volumes:
      - ./otel-config.yaml:/etc/otelcol/config.yaml
```

Point flowgen at it:

```yaml
telemetry:
  enabled: true
  otlp_endpoint: "http://localhost:4317"
  service_name: flowgen-dev
```

Run a flow, then check the collector's debug exporter or downstream backend for spans named `task.handle` with `service.name=flowgen-dev`.

## Tuning the export interval

`metrics_export_interval` controls how often metric snapshots are pushed. Lower values give finer-grained dashboards but increase network and collector load. Defaults to `60s`, which is appropriate for production. For development or low-throughput flows, drop to `10s` to see results quickly.

Spans are exported in batches as they end — there is no separate trace interval.

## What flowgen does not export

- **Per-event payloads.** Spans carry attributes (task name, IDs, byte counts) but never the event body. If you need full payload tracing, add a `log` task explicitly.
- **Process-level metrics** (CPU, memory, file descriptors). Use a node exporter or your runtime's standard metrics for those.
- **OTLP/HTTP.** The exporter uses gRPC only. If your collector requires HTTP, run a small OpenTelemetry Collector instance as a sidecar.

## Related

- [Flows](/docs/concepts/flows) — how task wiring affects span hierarchy.
- [Retry](/docs/concepts/retry) — retried calls produce one `task.handle` span per attempt with the retry attempt number in attributes.
