# Git Sync

Clones or pulls a Git repository, or reads one on the same machine, and emits one event with every file at the commit. Downstream tasks decide what to do with the content — parse it, store it, transform it. Use [`iterate`](/docs/flowgen/core/iterate) on `files` for one event per file.

Works with any HTTPS Git host: GitHub, GitLab, Bitbucket, Gitea, self-hosted. SSH URLs are not supported — use HTTPS + a token.

The event holds `{commit, files}`, each file `{path, content}` with `path` relative to the scanned directory.

## Configuration

```yaml
- git_sync:
    name: sync_configs
    repository: "{{env.GIT_REPOSITORY_URL}}"
    branch: main
    path: "configs/"
    credentials_path: /etc/git/credentials.json
```

To load flowgen's own flows and resources from a repository, see [Loading flows from Git](/docs/flowgen/git/guides/loading-flows).

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `repository` | string | required | The repository: an HTTPS URL, cloned for each sync, or a path on this machine, read at the tip of `branch` without a clone, so only committed files are emitted. A path takes no `credentials_path` or `clone_path`. Supports `{{env.VAR_NAME}}` templates. |
| `branch` | string | `main` for a URL, the checked-out branch for a path | Branch to track. |
| `path` | string | | Directory within the repo to scan. All regular files under this path are emitted; symlinks are skipped. |
| `clone_path` | string | `<temp>/<flow_name>/<task_name>` | Local path to clone into. Defaults to a per-task subdirectory of the system temp directory so multiple `git_sync` tasks in one worker do not collide. Override only when you need a stable path on a persistent volume. Paths containing `..` are rejected. |
| `credentials_path` | string | | Path to [credentials JSON file](/docs/flowgen/git#credentials). Empty means none. |
| `force_pull` | bool | `false` | Bypass the HEAD-commit cache and re-walk the working tree every tick. Use only to re-seed a downstream cache mutated out of band; leave off in steady state. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html). One event per sync, with `event.data` containing:

| Field | Type | Description |
|---|---|---|
| `commit` | string | HEAD commit hash. |
| `files` | array | Every file, each `{path, content}`: the path relative to `path` and the full content. |

## Change detection

Each tick reads the HEAD commit, after a `git fetch` for a URL. The hash is compared against the last successful sync, cached under `flow.{flow_name}.git_head.{repository}` in the shared cache. On a match, the file walk is skipped and the source emits only the upstream completion signal — one line per tick in the logs:

```
INFO flowgen_git::sync::processor: Git HEAD unchanged since last sync, skipping file walk repository=… commit=…
```

The commit is cached once every task after `git_sync` handled the event without an error, so a failed or interrupted flow syncs the same commit again on the next tick.

Set `force_pull: true` to bypass the cache — use only to re-seed a downstream cache mutated out of band.
