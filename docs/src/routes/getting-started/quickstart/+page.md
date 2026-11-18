# Quick Start

This guide walks you through creating your first flowgen flow.

## Prerequisites

- Flowgen binary ([Installation](/docs/flowgen/getting-started/installation))
- NATS JetStream (optional — for distributed cache, messaging, leader election)

## Your first flow

Create a file called `hello.yaml`:

```yaml
flow:
  tasks:
    - generate:
        name: ticker
        interval: "5s"
        payload:
          message: hello from flowgen

    - script:
        name: transform
        code: |
          let data = event.data;
          #{
            original: data.message,
            processed_at: timestamp_to_iso(timestamp_now()),
            uppercased: data.message.to_upper()
          }

    - log:
        name: output
```

This flow generates an event every 5 seconds, transforms it with a Rhai script, and logs the result.

## Configuration

Flowgen reads its configuration from a YAML file (`config.yaml`). A minimal setup only needs `flows.path`:

```yaml
flows:
  path: /etc/flowgen/flows/
```

Full configuration with all sections:

```yaml
# Cache shared by every replica (NATS KV). Without it, a local redb file.
cache:
  type: nats
  credentials_path: /etc/nats/credentials.json
  url: "{{env.NATS_URL}}"
  # runtime:
  #   db_name: flowgen_cache
  # system:
  #   db_name: flowgen_system
  # history: 10
  # tombstone_ttl: "1h"

# Flow discovery. Recursively loads all .yaml/.yml/.json files.
flows:
  path: /etc/flowgen/flows/
  # Cache-backed flow loading (for git-synced flows):
  # cache:
  #   enabled: true

# External resource files (SQL, templates, scripts).
resources:
  path: /etc/flowgen/resources/
  # cache:
  #   enabled: true

# OpenTelemetry metrics, traces, and logs.
# telemetry:
#   enabled: true
#   backend:
#     type: remote
#     endpoint: "http://otel-collector:4317"
#   service_name: flowgen
#   metrics_export_interval: "60s"

# HTTP server for webhooks, AI gateway, health checks.
http_server:
  enabled: true
  port: 3000
  # path: "/api/flowgen/workers/v1"
  # credentials_path: /etc/http/credentials.json

# MCP server for exposing flows as LLM tools.
# mcp_server:
#   enabled: true
#   port: 3001
#   path: "/mcp/v1"
```

Without cache configured, flowgen keeps the cache in a local redb file at `.flowgen/state.redb` (single-node). See [Caching](/docs/flowgen/concepts/caching) for details.

## Running

```bash
flowgen --config config.yaml
```

Flowgen discovers all YAML files in the configured flows directory and starts processing them.

Without `--config`, flowgen runs on the workspace in `FLOWGEN_HOME`, or `~/flowgen`. The first run creates it as a git repository with `flows/` and `resources/`. The web UI, HTTP server, MCP server and AI gateway start on `127.0.0.1`, so only this machine reaches them, and the web UI opens in the browser at `http://127.0.0.1:8080/flowgen/`. The cache is a redb file in the workspace's `.flowgen/` folder, which git ignores. `APP_` environment variables change any setting, for example `APP_WEB__PORT=9000`.

## What happens under the hood

1. Flowgen loads all flow YAML files from the configured path.
2. For each flow, it creates internal event channels connecting tasks in sequence.
3. The `generate` task produces events on the configured interval.
4. Each event flows through the `script` task for transformation.
5. The `log` task prints the final event to stdout.

## Next steps

- Learn about [Flows](/docs/flowgen/concepts/flows) and how tasks connect.
- Explore [NATS JetStream](/docs/flowgen/nats/subscribe) for durable message consumption.
- See [Script (Rhai)](/docs/flowgen/core/script) for the full scripting API.
