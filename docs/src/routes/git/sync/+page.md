# Git Sync

Clones or pulls a Git repository and emits one event per file. Downstream tasks decide what to do with the content — parse it, store it, transform it.

Works with any HTTPS Git host: GitHub, GitLab, Bitbucket, Gitea, self-hosted. SSH URLs are not supported — use HTTPS + a token.

Each event contains `{path, content, commit}` where `path` is relative to the scanned directory.

## Configuration

```yaml
- git_sync:
    name: sync_flows
    repository_url: "https://git.example.com/org/configs.git"
    branch: main
    path: "flows/"
    credentials_path: /etc/flowgen/credentials/git.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `repository_url` | string | required | Git repository URL (HTTPS). |
| `branch` | string | `main` | Branch to track. |
| `path` | string | | Directory within the repo to scan. All files under this path are emitted. |
| `clone_path` | string | `<temp>/<flow_name>/<task_name>` | Local path to clone into. Defaults to a per-task subdirectory of the system temp directory so multiple `git_sync` tasks in one worker do not collide. Override only when you need a stable path on a persistent volume. Paths containing `..` are rejected. |
| `credentials_path` | string | | Path to [credentials JSON file](/docs/flowgen/git#credentials). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Example: Sync flows from Git to NATS KV

```yaml
flow:
  name: git_sync_flows
  tasks:
    - generate:
        name: trigger
        interval: "5m"

    - git_sync:
        name: pull_repo
        repository_url: "https://git.example.com/org/configs.git"
        path: "flows/"
        credentials_path: /etc/flowgen/credentials/git.json

    - script:
        name: normalize_key
        code: |
          let key = "flows." + event.data.path.replace("/", ".");
          event.data.key = key;
          event

    - nats_kv_store:
        name: save_to_kv
        operation: put
        bucket: flowgen_system
        key: "{{event.data.key}}"
        credentials_path: /etc/nats/credentials.json
```

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). Each file emitted produces an event with `event.data` containing:

| Field | Type | Description |
|---|---|---|
| `path` | string | Relative file path in the repository. |
| `content` | string | Full file content. |
| `commit` | string | HEAD commit hash. |

## Bootstrap flows

Two end-to-end bootstrap flows reconcile a Git directory tree into the system cache. They tick on an interval, list existing cache entries, and emit one put per file and one delete per orphaned key:

- [`examples/git/system_sync_flows.yaml`](https://github.com/connve/flowgen/blob/main/examples/git/system_sync_flows.yaml) — keys each entry by `flow.name` parsed from the YAML body so the filename is incidental. The reconciler reads from `flowgen.flows.*` and starts, stops, and hot-reloads flows accordingly.
- [`examples/git/system_sync_resources.yaml`](https://github.com/connve/flowgen/blob/main/examples/git/system_sync_resources.yaml) — keys each entry by the file's relative path under `flowgen.resources.*`. The runtime `ResourceLoader` reads from the same keys when tasks reference `resource: <path>`. See [Resources](/docs/flowgen/concepts/resources).

Both skip the rest of their pipeline when the repo HEAD has not moved, so the only cost on a no-change tick is a `list_keys` round-trip.
