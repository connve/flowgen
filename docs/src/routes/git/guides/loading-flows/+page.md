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

[`config/deploy_workspace.yaml`](https://github.com/connve/flowgen/blob/main/config/deploy_workspace.yaml), the deploy flow of a local workspace, mirrors a release into the system cache. To deploy straight from a repository, replace its `oci_sync` with a `git_sync` of the repository:

1. A script routes each file of the pull by its top-level directory. `flows/*` are keyed by the path with the `flows/` prefix and the file extension stripped, matching the flow's path-based identity. `resources/*` are keyed by the path with the `resources/` prefix stripped. Any other file, such as a `README.md`, is dropped.
2. Two [`kv_store`](/docs/flowgen/kv-store/kv-store) entry puts with `prune: true` write new and changed keys and delete the keys the repository no longer holds.

`git_sync` emits every file of a commit in one event, and the flows put leaves `allow_empty` off, so an empty pull deletes no flows. A `path` that is not a folder at the commit fails the sync and deletes nothing.

When the branch tip has not moved, `git_sync` skips reading the files, so a tick without changes costs one shallow fetch of the tip commit. See [Resources](/docs/flowgen/concepts/resources) for how the runtime reads back from `resources.*`.

## Publishing changes

To change flows from the web UI and commit them to the same repository, see [Authoring](/docs/flowgen/concepts/authoring).
