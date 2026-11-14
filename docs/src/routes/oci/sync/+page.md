# OCI Sync

Pulls an OCI artifact (manifest + layers) from a registry, or from an OCI Image Layout on the same machine, and emits one event with every file of the artifact. Downstream tasks decide what to do with each file's content — parse it, store it, transform it. Use [`iterate`](/docs/flowgen/core/iterate) on `files` for one event per file.

Works with any OCI-compliant registry: GHCR, ECR, GAR, ACR, Artifactory, Harbor, Docker Hub, Quay, self-hosted.

The event holds `{digest, files}`, the shape of [Git Sync](/docs/flowgen/git/sync) with the manifest digest for the commit, so the same downstream tasks work with either source.

Both `oras push` artifacts and Docker container images are supported. `oras`-style layers — identified by the presence of the `org.opencontainers.image.title` annotation — give one file per layer using the annotation as the path. Docker image layers — layers without a title annotation — are merged in manifest order with overlay-fs semantics: later layers override earlier ones, `.wh.<name>` markers delete files from lower layers, and `.wh..wh..opq` markers hide entire subtrees. Only the surviving final state is emitted downstream.

## Configuration

```yaml
- oci_sync:
    name: pull_configs
    artifact: "{{env.OCI_ARTIFACT}}"
    credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/registry.json"
```

To load flowgen's own flows and resources from a registry, see [Loading flows from an OCI registry](/docs/flowgen/oci/guides/loading-flows).

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `artifact` | string | required | Full OCI reference, e.g. `registry.example.com/team/configs:prod` or `registry.example.com/team/configs@sha256:abcd…`, or a tag in an OCI Image Layout folder on the same machine, `oci-layout://<folder>:<tag>`. A tag the layout does not hold yet emits nothing. Supports `{{env.VAR_NAME}}` templates. |
| `credentials_path` | string | | Path to a JSON credentials file. Two formats are auto-detected; see [credentials](/docs/flowgen/oci#credentials). Anonymous auth if omitted. |
| `force_pull` | bool | `false` | Bypass the manifest-digest cache and re-pull every tick. Use only to re-seed a downstream cache mutated out of band; leave off in steady state. |
| `max_file_size` | int | `10485760` (10 MB) | Maximum uncompressed size, in bytes, for any single file extracted from a tar or tar+gzip layer. Applies to raw layers as their whole-blob size. |
| `max_total_size` | int | `104857600` (100 MB) | Maximum cumulative uncompressed size, in bytes, across every file pulled from one artifact. Guards against tar-bomb layers. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). One event per pull, with `event.data` containing:

| Field | Type | Description |
|---|---|---|
| `digest` | string | Whole-artifact manifest digest. |
| `files` | array | Every file of the artifact, each with the fields below. |

Each file holds:

| Field | Type | Description |
|---|---|---|
| `path` | string | File path in the artifact. For raw layers, taken from the layer's `org.opencontainers.image.title` annotation (falls back to `layer-<index>` if missing). For tar and tar+gzip layers, taken from the tar entry path with any leading `/` stripped. |
| `content` | string | File content as UTF-8; absent for a binary file. |
| `content_base64` | string | Content of a binary file, base64-encoded; absent for a UTF-8 file. |
| `digest` | string | Layer blob digest (`sha256:…`). Multiple files extracted from one tar layer share the same digest. |

## Layer formats

The layer's `mediaType` in the manifest dispatches the extractor:

| Media type suffix | Handling |
|---|---|
| anything else (e.g. `application/octet-stream`, `application/yaml`) | Raw layer — the blob bytes are the file. One file per layer. Path from `org.opencontainers.image.title`. |
| `+gzip` (e.g. `application/vnd.oci.image.layer.v1.tar+gzip`, `application/vnd.docker.image.rootfs.diff.tar.gzip`) | tar+gzip archive — decompressed and unpacked. One file per tar entry. Path from the tar entry. |
| `.tar` or `+tar` (e.g. `application/vnd.oci.image.layer.v1.tar`) | tar archive — unpacked. One file per tar entry. |

Inside tar and tar+gzip layers, directories, symlinks, and hardlinks are skipped. Only regular files are emitted. Docker whiteout markers (`.wh.<name>`, `.wh..wh..opq`) are consumed by the layer-merge pass rather than emitted downstream.

This means both push paths work:

- `oras push {{env.OCI_ARTIFACT}} a.yaml:application/yaml`: one raw layer per file, path from the `:mediaType` annotation.
- `docker push {{env.OCI_ARTIFACT}}`: layers are merged into a single overlay-fs snapshot, and every surviving file is emitted. Every file in the image is emitted, so build the final stage `FROM scratch`.

## Change detection

Each tick issues an HTTP HEAD against the artifact's manifest URL to read `Docker-Content-Digest` from the response header. The digest is compared against the last successful pull, cached under `flow.{flow_name}.oci_digest.{artifact}` in the shared cache. On a match, the layer blobs are not fetched and the source emits only the upstream completion signal — one line per tick in the logs:

```
INFO flowgen_oci::sync::processor: OCI manifest digest unchanged since last pull, skipping layer fetch artifact=… digest=sha256:…
```

Works for both mutable tags (`:prod`) and immutable digests (`@sha256:…`) — the digest is authoritative in either case, so a re-tag of `:prod` to a new release still triggers a pull.

The digest is cached once every task after `oci_sync` handled the event without an error, so a failed or interrupted flow pulls the same artifact again on the next tick.

Set `force_pull: true` to bypass the cache — use only to re-seed a downstream cache mutated out of band.
