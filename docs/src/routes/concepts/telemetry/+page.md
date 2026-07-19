# Telemetry

Flowgen splits telemetry signals by transport:

- **Metrics** and **traces** go through OpenTelemetry over OTLP/gRPC (for the `remote` backend). Any OTLP-compatible collector works — OpenTelemetry Collector, VictoriaMetrics, Grafana Cloud, Tempo, Honeycomb, Datadog, and so on.
- **Logs** always go to stdout as JSON via `tracing_subscriber::fmt::json()`. In production a K8s log shipper (Fluent Bit, Vector, Grafana Alloy) collects the stream and forwards it to Loki, VictoriaLogs, Elasticsearch, or whichever log store the operator runs.

Two backends switch how signals are handled in-process:

- `memory` — no network I/O. Metrics/traces are dropped; logs still go to stdout, and a copy is kept in a bounded per-flow ring buffer that the admin UI reads through the built-in `LogsQuery`. Intended for demo and single-node dev.
- `remote` — metrics/traces push over OTLP/gRPC to `endpoint`; logs remain on stdout for the log shipper. The admin UI's live activity view depends on an out-of-process log query backend in this mode.

## Configuration

```yaml
telemetry:
  enabled: true
  backend:
    type: remote
    endpoint: "http://otel-collector:4317"
  service_name: flowgen
  metrics_export_interval: "60s"
```

| Field | Default | Description |
|---|---|---|
| `enabled` | required | Set `true` to initialize the provider. When `false` the whole telemetry stack is skipped. |
| `backend` | in-memory | Backend selection. Omit for the in-memory backend. |
| `backend.type` | — | Either `memory` or `remote`. |
| `backend.endpoint` | — | Required for `remote`. gRPC endpoint of the collector. |
| `backend.logs_per_flow` | `1000` | Memory backend only. Log records retained per flow before oldest entries are dropped. |
| `backend.metrics_per_flow` | `1000` | Memory backend only. Metric samples retained per flow before oldest entries are dropped. |
| `service_name` | `flowgen` | `service.name` resource attribute. Set per deployment so telemetry from multiple flowgen instances stays separable. |
| `metrics_export_interval` | `60s` | How often metric snapshots are pushed. Human-readable durations: `30s`, `1m`, `5m`. Ignored by the memory backend. |

Omitting the whole `telemetry` block is equivalent to `enabled: false`.

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

Metrics are derived from tracing spans. Every span produces a duration histogram, and counters track invocation rate and error rate:

- `task.handle.duration` — per-event handler latency.
- `task.handle.count` — total invocations.
- `task.handle.errors` — invocations that returned an error after retries.

All metrics carry the `service.name` resource attribute — filter on it to isolate one deployment from the rest of a fleet.

### Logs

Logs are written as JSON to stdout by `tracing_subscriber::fmt::json()`. Each line carries the message body plus every structured field from the `tracing` macro and — critically — the full parent-span field hierarchy under `spans`. That means every event inside a `task.handle` scope inherits `flow`, `task`, `task_id`, and `task_type` without the caller having to spell them out.

In production the K8s log shipper picks up stdout and forwards it to the configured log store. In `memory` mode the same JSON stream is parsed into an in-process per-flow ring buffer that the admin UI reads for its activity view.

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
  backend:
    type: remote
    endpoint: "http://localhost:4317"
  service_name: flowgen-dev
```

Run a flow, then check the collector's debug exporter or downstream backend for spans named `task.handle` with `service.name=flowgen-dev`.

## Tuning the export interval

`metrics_export_interval` controls how often metric snapshots are pushed. Lower values give finer-grained dashboards but increase network and collector load. Defaults to `60s`, which is appropriate for production. For development or low-throughput flows, drop to `10s` to see results quickly.

Spans are exported in batches as they end. Logs are written to stdout per-event with no buffering; downstream aggregation is the log shipper's concern.

## What flowgen does not export

- **Per-event payloads.** Spans carry attributes (task name, IDs, byte counts) but never the event body. If you need full payload tracing, add a `log` task explicitly.
- **Process-level metrics** (CPU, memory, file descriptors). Use a node exporter or your runtime's standard metrics for those.
- **OTLP/HTTP.** The exporter uses gRPC only. If your collector requires HTTP, run a small OpenTelemetry Collector instance as a sidecar.

## Related

- [Flows](/docs/flowgen/concepts/flows) — how task wiring affects span hierarchy.
- [Retry](/docs/flowgen/concepts/retry) — retried calls produce one `task.handle` span per attempt with the retry attempt number in attributes.
