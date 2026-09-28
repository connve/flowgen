# Telemetry

Flowgen splits telemetry signals by transport:

- **Metrics** and **traces** go through OpenTelemetry over OTLP/gRPC (for the `remote` backend). Any OTLP-compatible collector works — OpenTelemetry Collector, VictoriaMetrics, Grafana Cloud, Tempo, Honeycomb, Datadog, and so on.
- **Logs** always go to stdout as JSON via `tracing_subscriber::fmt::json()`. In production a K8s log shipper (Fluent Bit, Vector, Grafana Alloy) collects the stream and forwards it to Loki, VictoriaLogs, Elasticsearch, or whichever log store the operator runs.

Two backends switch how signals are handled in-process:

- `memory` (default) — metrics/traces are dropped; logs still go to stdout, and a copy is kept in a bounded per-flow ring buffer that the web UI reads. With `cache` configured, pods run in cluster mode: each pod serves its ring buffer and flow counters to the other pods on an internal port, and the web UI shows every pod. See [Multiple pods](#multiple-pods).
- `remote` — metrics/traces push over OTLP/gRPC to `endpoint`; logs remain on stdout for the log shipper. The web UI's live activity view depends on an out-of-process log query backend in this mode.

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
| `backend.type` | — | `memory` or `remote`. |
| `backend.endpoint` | — | Required for `remote`. gRPC endpoint of the collector. |
| `backend.logs_per_flow` | `1000` | Memory backend only. Log records retained per flow and level on each pod before oldest entries are dropped. |
| `backend.metrics_per_flow` | `1000` | Memory backend only. Metric samples retained per flow before oldest entries are dropped. |
| `backend.port` | `8082` | Memory backend only. Port of the internal endpoint the other pods read logs and flow counters from in cluster mode. |
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

In production the K8s log shipper picks up stdout and forwards it to the configured log store. With the `memory` backend the same JSON stream is parsed into an in-process per-flow ring buffer that the web UI reads for its activity view.

## Multiple pods

With the `memory` backend and `cache` configured, pods run in cluster mode and the web UI merges every pod, whichever pod serves the request: logs are ordered by timestamp, and each flow's event, warning, and error counts are summed with the latest timestamps winning the status. Nothing else needs to be configured; a single pod is a cluster of one.

- Pods find each other through the peer registry in the cache. Without `cache`, or with the `remote` backend, the web UI shows only the pod that serves the request.
- Each pod advertises `$POD_IP:<port>`. The Helm chart sets `POD_IP`; other deployments set it from the pod IP.
- Pods authenticate to each other with a random token that the first pod generates and stores in the system cache bucket under `cluster.token`. Nothing needs to be configured. The token is not reachable through `ctx.cache`; anyone with access to the system bucket can read it, which is the same access that controls flows and leases.
- The internal endpoint uses plain HTTP. Keep the port out of Services and Ingresses, and allow it only between flowgen pods with a NetworkPolicy. The Helm chart declares the port as `flowgen.cluster.port` (default `8082`); `flowgen.cluster.networkPolicy: true` renders that NetworkPolicy, which also blocks every port the chart does not declare.
- A pod that does not answer within 3 seconds is left out of the result. **Monitor → Pods** in the web UI lists every pod with its status, the reason it is unreachable, and the number of flows it runs: every flow without leader election, plus the leader-elected flows whose lease it holds. The Pods tab shows `<reachable>/<total>` while a pod is missing. Outside cluster mode, Pods lists only the pod that served the page. The same data is available at `GET /api/cluster`.
- Logs and counters live in memory, so a restarted pod's history and counts are gone. Use the log shipper's store for long-term history.

### Regenerating the token

If the token leaks, replace it:

```bash
curl -X POST https://flowgen.example.com/flowgen/api/cluster/token
```

With `web.auth` configured, the request needs a signed-in session like the rest of `/api`; without it, anyone who can reach the web UI can call it. Deleting the `cluster.token` key from the system cache bucket has the same effect: the next pod to need it generates a new one.

Every pod switches to the new token within 30 seconds. Until then, some pods may miss others' logs and counters, and Monitor → Pods shows `Rejected the cluster token` for them. The old token is rejected as soon as a pod has read the new one.

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
