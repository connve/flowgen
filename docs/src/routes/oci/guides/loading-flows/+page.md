# Loading flows from an OCI registry

Release flows and resources as an OCI artifact and let flowgen load them: a flow with [`oci_sync`](/docs/flowgen/oci/sync) pulls the artifact and writes every flow and resource into the system cache, where the runtime picks them up. Promoting or rolling back a release is moving a tag.

## Artifact layout

One artifact carries both directories side by side:

```
artifact
├── flows/
│   └── orders/sync.yaml
└── resources/
    └── scripts/transform.rhai
```

Push it with [`oci_push`](/docs/flowgen/oci/push), `oras push`, or a Docker image.

## Packaging with Docker

Every file in the image's final filesystem is emitted as an event, so the final stage of the Dockerfile must be `FROM scratch`. A base image such as `busybox` or `alpine` adds its own `/bin`, `/lib` and `/etc` files, which arrive as binary events and break tasks that expect JSON.

```dockerfile
FROM scratch
COPY flows/     /flows/
COPY resources/ /resources/
```

```sh
docker build -t registry.example.com/team/configs:prod .
docker push registry.example.com/team/configs:prod
```

If your CI requires a base image for provenance scanning, use a multi-stage build and keep the final stage `FROM scratch`:

```dockerfile
FROM your-registry/base:latest AS source
COPY flows/     /source/flows/
COPY resources/ /source/resources/

FROM scratch
COPY --from=source /source/ /
```

To check the layer before pushing, list its entries; every entry should be under `flows/` or `resources/`:

```sh
docker save registry.example.com/team/configs:prod -o configs.tar
mkdir -p /tmp/configs && tar -xf configs.tar -C /tmp/configs
tar -tzf /tmp/configs/blobs/sha256/*   # or /tmp/configs/*/layer.tar on older Docker
```

## Flow

[`config/deploy_workspace.yaml`](https://github.com/connve/flowgen/blob/main/config/deploy_workspace.yaml) mirrors the latest release in `FLOWGEN_RELEASES`, a local OCI Image Layout or a registry, into the system cache; see [Lifecycle flows](/docs/flowgen/concepts/authoring#lifecycle-flows) for its variables:

1. A script routes each file of the pull by its top-level directory. `flows/*` are keyed by the path with the `flows/` prefix and the file extension stripped, so `flows/orders/sync.yaml` becomes `orders/sync`, matching the flow's path-based identity. `resources/*` are keyed by the path with the `resources/` prefix stripped. Any other file is dropped.
2. Two [`kv_store`](/docs/flowgen/kv-store/kv-store) entry puts with `prune: true` write new and changed keys and delete the keys the artifact no longer holds.

`oci_sync` emits every file of an artifact in one event, and the flows put leaves `allow_empty` off, so an empty pull deletes no flows.

When the manifest digest has not changed, `oci_sync` skips the layer fetch, so a tick without changes costs one manifest `HEAD`. See [Resources](/docs/flowgen/concepts/resources) for how the runtime reads back from `resources.*`.
