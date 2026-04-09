# Quick Start

This guide walks you through creating your first flowgen flow.

## Prerequisites

- A Kubernetes cluster (or local setup with minikube/kind)
- NATS JetStream running (for caching and messaging)
- Flowgen binary or container image

## Your first flow

Create a file called `hello.yaml`:

```yaml
flow:
  name: hello_world
  tasks:
    - generate:
        name: ticker
        interval: "5s"
        code: |
          #{
            message: "hello from flowgen",
            timestamp: timestamp_now()
          }

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

Flowgen reads its configuration from a YAML file. Here is a minimal example:

```yaml
cache:
  enabled: true
  cache_type: nats
  credentials_path: /etc/nats/credentials.json
  url: nats://localhost:4222

flows:
  path: /etc/flowgen/flows/

worker:
  event_buffer_size: 1000
```

## Running

```bash
flowgen --config config.yaml
```

Flowgen discovers all YAML files in the configured flows directory and starts processing them.

## What happens under the hood

1. Flowgen loads all flow YAML files from the configured path.
2. For each flow, it creates internal event channels connecting tasks in sequence.
3. The `generate` task produces events on the configured interval.
4. Each event flows through the `script` task for transformation.
5. The `log` task prints the final event to stdout.

## Next steps

- Learn about [Flows](/docs/concepts/flows) and how tasks connect.
- Explore [NATS JetStream](/docs/nats/subscriber) for durable message consumption.
- See [Script (Rhai)](/docs/core/script) for the full scripting API.
