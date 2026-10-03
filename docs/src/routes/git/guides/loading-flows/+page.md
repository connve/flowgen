# Loading flows from Git

Keep flows and resources in a Git repository and let flowgen load them: a flow with [`git_sync`](/docs/flowgen/git/sync) pulls the repository and writes every flow and resource into the system cache, where the runtime picks them up.

## Repository layout

One repository carries both directories under the configured `path`:

```
<path>/
├── flows/
│   └── orders/sync.yaml
└── resources/
    └── scripts/transform.rhai
```

## Flow

[`examples/git/sync_workspace.yaml`](https://github.com/connve/flowgen/blob/main/examples/git/sync_workspace.yaml) mirrors the directory tree into the system cache:

1. A [`buffer`](/docs/flowgen/core/buffer) with `flush_on_completion` collects each pull into one batch.
2. A script routes each file by its top-level directory. `flows/*` are keyed by the path with the `flows/` prefix and the file extension stripped, matching the flow's path-based identity. `resources/*` are keyed by the path with the `resources/` prefix stripped. Any other file, such as a `README.md`, is dropped.
3. Two [`nats_kv_store`](/docs/flowgen/nats/kv-store) entry puts with `prune: true` write new and changed keys and delete the keys the repository no longer holds.

A batch cut short by the buffer `timeout` is skipped, and the flows put leaves `allow_empty` off, so a partial or empty pull deletes no flows.

When the repository HEAD has not moved, `git_sync` skips the file walk, so a tick without changes costs one `git fetch`. See [Resources](/docs/flowgen/concepts/resources) for how the runtime reads back from `resources.*`.

## Publishing changes

To change flows from the web UI and commit them to the same repository, see [Authoring](/docs/flowgen/concepts/authoring).
