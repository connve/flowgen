# Git Sync

Clones or pulls a Git repository and emits one event per file. Downstream tasks decide what to do with the content — parse it, store it, transform it.

Each event contains `{path, content, commit}` where `path` is relative to the scanned directory.

## Configuration

```yaml
- git_sync:
    name: sync_flows
    repository_url: "git@github.com:org/configs.git"
    branch: main
    path: "flows/"
    auth:
      type: ssh
      ssh_key_path: /etc/git/deploy-key
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `repository_url` | string | required | Git repository URL (SSH or HTTPS). |
| `branch` | string | `main` | Branch to track. |
| `path` | string | | Directory within the repo to scan. All files under this path are emitted. |
| `clone_path` | string | `/tmp/flowgen-repo` | Local path to clone into. |
| `auth` | object | `none` | Authentication configuration (see below). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Authentication

| Field | Type | Description |
|---|---|---|
| `type` | string | `none`, `ssh`, or `token`. |
| `ssh_key_path` | string | Path to SSH private key (for `ssh` type). |
| `ssh_known_hosts_path` | string | Path to known_hosts file (for `ssh` type). |
| `token` | string | Token for HTTPS auth (for `token` type). |

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
        repository_url: "git@github.com:org/configs.git"
        path: "flows/"
        auth:
          type: ssh
          ssh_key_path: /etc/git/deploy-key

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
