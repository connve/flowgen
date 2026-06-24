# OCI Sync

Pulls an OCI artifact (manifest + layers) from a registry and emits one event per layer. Downstream tasks decide what to do with each layer's content — parse it, store it, transform it.

Works with any OCI-compliant registry: GHCR, ECR, GAR, ACR, Artifactory, Harbor, Docker Hub, Quay, self-hosted.

Each event contains `{path, content, digest, artifact_digest}`. The shape mirrors [Git Sync](/docs/flowgen/git/sync) so the same downstream pipeline (buffer → diff → cache write) works with either source.

## Configuration

```yaml
- oci_sync:
    name: pull_flows
    artifact: "registry.example.com/your-org/your-flows:prod"
    credentials_path: /etc/flowgen/credentials/registry.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `artifact` | string | required | Full OCI reference, e.g. `registry.example.com/org/flows:prod` or `registry.example.com/org/flows@sha256:abcd…`. |
| `credentials_path` | string | | Path to a JSON credentials file. Two formats are auto-detected; see [credentials](/docs/flowgen/oci#credentials). Anonymous auth if omitted. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Example: sync flows from a registry into the NATS KV cache

```yaml
flow:
  name: oci_sync_flows
  tasks:
    - generate:
        name: trigger
        interval: "30s"

    - oci_sync:
        name: pull_repo
        artifact: "registry.example.com/your-org/your-flows:prod"
        credentials_path: /etc/flowgen/credentials/registry.json

    - buffer:
        name: collect_layers
        size: 10000
        timeout: "5s"

    - script:
        name: write_cache_keys
        code: |
          let actions = [];
          for layer in event.data.batch {
              let parsed = parse_yaml(layer.content);
              let key = "flowgen.flows." + parsed.flow.name;
              actions.push(#{
                  action: "put",
                  key: key,
                  content: layer.content,
              });
          }
          actions

    - nats_kv_store:
        name: save_to_kv
        operation: put
        bucket: flowgen_system
        key: "{{event.data.key}}"
        credentials_path: /etc/nats/credentials.json
```

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). Each layer emitted produces an event with `event.data` containing:

| Field | Type | Description |
|---|---|---|
| `path` | string | File path in the artifact, derived from the layer's `org.opencontainers.image.title` annotation set during `oras push`. Falls back to `layer-<index>` if the annotation is missing. |
| `content` | string | Layer blob as UTF-8. Non-UTF-8 layers (e.g. binary blobs) produce an error. |
| `digest` | string | Layer blob digest (`sha256:…`). |
| `artifact_digest` | string | Whole-artifact manifest digest. The same value across all events from one pull. |

## Bootstrap flows

Two end-to-end bootstrap flows reconcile an OCI artifact into the system cache. They tick on an interval, list existing cache entries, and emit one put per layer and one delete per orphaned key:

- [`examples/oci/system_sync_flows.yaml`](https://github.com/connve/flowgen/blob/main/examples/oci/system_sync_flows.yaml) — keys each entry by `flow.name` parsed from the layer body so the filename is incidental. The reconciler reads from `flowgen.flows.*` and starts, stops, and hot-reloads flows accordingly.
- [`examples/oci/system_sync_resources.yaml`](https://github.com/connve/flowgen/blob/main/examples/oci/system_sync_resources.yaml) — keys each entry by the layer's relative path under `flowgen.resources.*`. The runtime `ResourceLoader` reads from the same keys when tasks reference `resource: <path>`. See [Resources](/docs/flowgen/concepts/resources).

Both skip the rest of their pipeline when the artifact digest has not moved, so the only cost on a no-change tick is a manifest fetch plus a `list_keys` round-trip.
