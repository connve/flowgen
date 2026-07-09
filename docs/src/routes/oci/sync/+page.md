# OCI Sync

Pulls an OCI artifact (manifest + layers) from a registry and emits one event per file. Downstream tasks decide what to do with each file's content — parse it, store it, transform it.

Works with any OCI-compliant registry: GHCR, ECR, GAR, ACR, Artifactory, Harbor, Docker Hub, Quay, self-hosted.

Each UTF-8 text event contains `{path, content, digest, artifact_digest}` on `event.data`. The shape mirrors [Git Sync](/docs/flowgen/git/sync) so the same downstream pipeline (buffer → diff → cache write) works with either source. Binary layers (or tar entries) that cannot be safely decoded as UTF-8 arrive as `EventData::Bytes` instead, with `{path, digest, artifact_digest}` moved to `event.meta` so downstream tasks such as `object_store::write` can still route by key.

Both `oras push` artifacts and Docker container images are supported. `oras`-style layers — identified by the presence of the `org.opencontainers.image.title` annotation — emit one event per layer using the annotation as the path. Docker image layers — layers without a title annotation — are merged in manifest order with overlay-fs semantics: later layers override earlier ones, `.wh.<name>` markers delete files from lower layers, and `.wh..wh..opq` markers hide entire subtrees. Only the surviving final state is emitted downstream.

## Configuration

```yaml
- oci_sync:
    name: pull_flows
    artifact: "{{env.OCI_FLOWS_ARTIFACT}}"
    credentials_path: /etc/flowgen/credentials/registry.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `artifact` | string | required | Full OCI reference, e.g. `registry.example.com/org/flows:prod` or `registry.example.com/org/flows@sha256:abcd…`. Supports `{{env.VAR_NAME}}` templates. |
| `credentials_path` | string | | Path to a JSON credentials file. Two formats are auto-detected; see [credentials](/docs/flowgen/oci#credentials). Anonymous auth if omitted. |
| `force_pull` | bool | `false` | Bypass the manifest-digest cache and re-pull every tick. Use only to re-seed a downstream cache mutated out of band; leave off in steady state. |
| `max_file_size` | int | `10485760` (10 MB) | Maximum uncompressed size, in bytes, for any single file extracted from a tar or tar+gzip layer. Applies to raw layers as their whole-blob size. |
| `max_total_size` | int | `104857600` (100 MB) | Maximum cumulative uncompressed size, in bytes, across every file pulled from one artifact. Guards against tar-bomb layers. |
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
        artifact: "{{env.OCI_FLOWS_ARTIFACT}}"
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
        url: "{{env.NATS_URL}}"
```

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) for UTF-8 files, [`EventData::Bytes`](/docs/flowgen/concepts/events) for binary files. Each file emitted produces one event.

For UTF-8 files, `event.data` contains:

| Field | Type | Description |
|---|---|---|
| `path` | string | File path in the artifact. For raw layers, taken from the layer's `org.opencontainers.image.title` annotation (falls back to `layer-<index>` if missing). For tar and tar+gzip layers, taken from the tar entry path with any leading `/` stripped. |
| `content` | string | File content as UTF-8. |
| `digest` | string | Layer blob digest (`sha256:…`). Multiple files extracted from one tar layer share the same digest. |
| `artifact_digest` | string | Whole-artifact manifest digest. The same value across all events from one pull. |

For binary files, `event.data` carries the raw bytes and the same routing fields (`path`, `digest`, `artifact_digest`) land on `event.meta` instead. Downstream tasks match `EventData::Bytes` directly and read `event.meta.path` to know where the payload belongs.

## Layer formats

The layer's `mediaType` in the manifest dispatches the extractor:

| Media type suffix | Handling |
|---|---|
| anything else (e.g. `application/octet-stream`, `application/yaml`) | Raw layer — the blob bytes are the file. One event per layer. Path from `org.opencontainers.image.title`. |
| `+gzip` (e.g. `application/vnd.oci.image.layer.v1.tar+gzip`, `application/vnd.docker.image.rootfs.diff.tar.gzip`) | tar+gzip archive — decompressed and unpacked. One event per file entry. Path from the tar entry. |
| `.tar` or `+tar` (e.g. `application/vnd.oci.image.layer.v1.tar`) | tar archive — unpacked. One event per file entry. |

Inside tar and tar+gzip layers, directories, symlinks, and hardlinks are skipped. Only regular files produce events. Docker whiteout markers (`.wh.<name>`, `.wh..wh..opq`) are consumed by the layer-merge pass rather than emitted downstream.

This means both push paths work:

- `oras push {{env.OCI_FLOWS_ARTIFACT}} flow.yaml:application/yaml` — one raw layer per file, path from the `:mediaType` annotation.
- `docker push {{env.OCI_FLOWS_ARTIFACT}}` (from a Dockerfile that `COPY`s YAML files across multiple stages) — layers are merged into a single overlay-fs snapshot; the final state emits one event per surviving file.

## Packaging with Docker

When `oras` is not available in your build environment, `docker build` + `docker push` is the supported alternative. Because every file in the image's final rootfs is emitted as a downstream event, the final stage of the Dockerfile must be `FROM scratch` — any base image (e.g. `busybox`, `alpine`) drops its own `/bin`, `/lib`, `/etc` binaries into the layer, which then arrive at your bootstrap pipeline as `EventData::Bytes` and break tasks like `buffer` that expect JSON.

The recommended layout ships flows and resources together in one artifact, called a **workspace**. Both top-level directories live side by side in the image, and the bootstrap flow routes each layer by its prefix:

```
flowgen-workspace image
├── flows/
│   └── ...*.yaml
└── resources/
    └── ...
```

### Dockerfile

```dockerfile
FROM scratch
LABEL org.opencontainers.image.title="flowgen-workspace"
COPY flows/     /flows/
COPY resources/ /resources/
```

### Build and push

```sh
docker build -t registry.example.com/org/flowgen-workspace:prod .
docker push registry.example.com/org/flowgen-workspace:prod
```

If your CI mandates a base image for provenance scanning, use a multi-stage build and keep the final stage `FROM scratch`:

```dockerfile
FROM your-registry/base:latest AS source
COPY flows/     /source/flows/
COPY resources/ /source/resources/

FROM scratch
LABEL org.opencontainers.image.title="flowgen-workspace"
COPY --from=source /source/ /
```

Only the final stage ends up in the pushed image, so the layer stays clean.

### Verifying the layer

Before pushing, confirm the image's layer contains only your YAML and resource files — nothing from a base image:

```sh
docker save registry.example.com/org/flowgen-workspace:prod -o flowgen-workspace.tar
mkdir -p /tmp/flowgen-workspace && tar -xf flowgen-workspace.tar -C /tmp/flowgen-workspace
tar -tzf /tmp/flowgen-workspace/blobs/sha256/*   # or /tmp/flowgen-workspace/*/layer.tar on older Docker
```

Every listed entry should live under `flows/` or `resources/`. Any `bin/`, `lib/`, `etc/`, `usr/` entries mean the final stage picked up a base image rootfs — fix the Dockerfile to end in `FROM scratch` before pushing.

## Bootstrap flow

[`examples/oci/system_sync_workspace.yaml`](https://github.com/connve/flowgen/blob/main/examples/oci/system_sync_workspace.yaml) reconciles an OCI artifact into the system cache end-to-end. One artifact carries both `flows/` and `resources/`; the bootstrap routes each layer by its top-level directory — `flows/*` are keyed by `flow.name` parsed from the layer body, `resources/*` are keyed by the path with the `resources/` prefix stripped, and any layer outside those two prefixes (e.g. a `README.md`) is dropped. It ticks on an interval, lists existing cache entries under both prefixes, and emits one put per layer and one delete per orphaned key.

The flow skips the rest of its pipeline when the artifact digest has not moved, so the only cost on a no-change tick is a manifest HEAD plus a `list_keys` round-trip. See [Resources](/docs/flowgen/concepts/resources) for how the runtime `ResourceLoader` reads back from `flowgen.resources.*`.

## Change detection

Each tick issues an HTTP HEAD against the artifact's manifest URL to read `Docker-Content-Digest` from the response header. The digest is compared against the last successful pull, cached under `flow.{flow_name}.oci_digest.{artifact}` in the shared cache. On a match, the layer blobs are not fetched and the source emits only the upstream completion signal — one line per tick in the logs:

```
INFO flowgen_oci::sync::processor: OCI manifest digest unchanged since last pull, skipping layer fetch artifact=… digest=sha256:…
```

Works for both mutable tags (`:prod`) and immutable digests (`@sha256:…`) — the digest is authoritative in either case, so a re-tag of `:prod` to a new release still triggers a pull.

The cached digest is persisted only after every layer event was sent, so a mid-pull failure causes the next tick to re-emit the full batch.

Set `force_pull: true` to bypass the cache — use only to re-seed a downstream cache mutated out of band.
