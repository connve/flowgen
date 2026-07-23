# Git Sync

Clones or pulls a Git repository and emits one event per file. Downstream tasks decide what to do with the content — parse it, store it, transform it.

Works with any HTTPS Git host: GitHub, GitLab, Bitbucket, Gitea, self-hosted. SSH URLs are not supported — use HTTPS + a token.

Each event contains `{path, content, commit}` where `path` is relative to the scanned directory.

## Configuration

```yaml
- git_sync:
    name: sync_flows
    repository_url: "{{env.GIT_FLOWS_URL}}"
    branch: main
    path: "flows/"
    credentials_path: /etc/flowgen/credentials/git.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `repository_url` | string | required | Git repository URL (HTTPS). Supports `{{env.VAR_NAME}}` templates. |
| `branch` | string | `main` | Branch to track. |
| `path` | string | | Directory within the repo to scan. All files under this path are emitted. |
| `clone_path` | string | `<temp>/<flow_name>/<task_name>` | Local path to clone into. Defaults to a per-task subdirectory of the system temp directory so multiple `git_sync` tasks in one worker do not collide. Override only when you need a stable path on a persistent volume. Paths containing `..` are rejected. |
| `credentials_path` | string | | Path to [credentials JSON file](/docs/flowgen/git#credentials). |
| `force_pull` | bool | `false` | Bypass the HEAD-commit cache and re-walk the working tree every tick. Use only to re-seed a downstream cache mutated out of band; leave off in steady state. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Example: Sync flows from Git to NATS KV

```yaml
flow:
  tasks:
    - generate:
        name: trigger
        interval: "5m"

    - git_sync:
        name: pull_repo
        repository_url: "{{env.GIT_FLOWS_URL}}"
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
        url: "{{env.NATS_URL}}"
```

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). Each file emitted produces an event with `event.data` containing:

| Field | Type | Description |
|---|---|---|
| `path` | string | Relative file path in the repository. |
| `content` | string | Full file content. |
| `commit` | string | HEAD commit hash. |

## Bootstrap flow

[`examples/git/system_sync_workspace.yaml`](https://github.com/connve/flowgen/blob/main/examples/git/system_sync_workspace.yaml) reconciles a Git directory tree into the system cache end-to-end. One repo carries both `flows/` and `resources/` under the configured `path:`; the bootstrap routes each file by its top-level directory — `flows/*` are keyed by the file path with the `flows/` prefix and file extension stripped (matching the flow's path-based identity), `resources/*` are keyed by the path with the `resources/` prefix stripped, and any file outside those two prefixes (e.g. a `README.md`) is dropped. It ticks on an interval, lists existing cache entries under both prefixes, and emits one put per file and one delete per orphaned key.

The flow skips the rest of its pipeline when the repo HEAD has not moved, so the only cost on a no-change tick is a `git fetch` plus a `list_keys` round-trip. See [Resources](/docs/flowgen/concepts/resources) for how the runtime `ResourceLoader` reads back from `resources.*`.

## Change detection

Each tick runs `git fetch` and reads the new HEAD commit hash. The hash is compared against the last successful sync, cached under `flow.{flow_name}.git_head.{repository_url}` in the shared cache. On a match, the file walk is skipped and the source emits only the upstream completion signal — one line per tick in the logs:

```
INFO flowgen_git::sync::processor: Git HEAD unchanged since last sync, skipping file walk repository=… commit=…
```

The cached commit is persisted only after every file event was sent, so a mid-walk failure causes the next tick to re-emit the full batch.

Set `force_pull: true` to bypass the cache — use only to re-seed a downstream cache mutated out of band.
