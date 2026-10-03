# Git Sync

Clones or pulls a Git repository and emits one event per file. Downstream tasks decide what to do with the content — parse it, store it, transform it.

Works with any HTTPS Git host: GitHub, GitLab, Bitbucket, Gitea, self-hosted. SSH URLs are not supported — use HTTPS + a token.

Each event contains `{path, content, commit}` where `path` is relative to the scanned directory.

## Configuration

```yaml
- git_sync:
    name: sync_configs
    repository_url: "{{env.GIT_REPOSITORY_URL}}"
    branch: main
    path: "configs/"
    credentials_path: /etc/git/credentials.json
```

To load flowgen's own flows and resources from a repository, see [Loading flows from Git](/docs/flowgen/git/guides/loading-flows).

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

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). Each file emitted produces an event with `event.data` containing:

| Field | Type | Description |
|---|---|---|
| `path` | string | Relative file path in the repository. |
| `content` | string | Full file content. |
| `commit` | string | HEAD commit hash. |

## Change detection

Each tick runs `git fetch` and reads the new HEAD commit hash. The hash is compared against the last successful sync, cached under `flow.{flow_name}.git_head.{repository_url}` in the shared cache. On a match, the file walk is skipped and the source emits only the upstream completion signal — one line per tick in the logs:

```
INFO flowgen_git::sync::processor: Git HEAD unchanged since last sync, skipping file walk repository=… commit=…
```

The cached commit is persisted only after every file event was sent, so a mid-walk failure causes the next tick to re-emit the full batch.

Set `force_pull: true` to bypass the cache — use only to re-seed a downstream cache mutated out of band.
