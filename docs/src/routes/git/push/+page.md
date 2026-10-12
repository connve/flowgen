# Git Push

Commits the files of an event on top of a branch and pushes the commit. Works with any HTTPS Git host; SSH URLs are not supported.

The input event carries `files`, each `{path, content}` with paths relative to `path`. `content` is required; `null` deletes the file. An optional `previous` holds the content the change was prepared against (or `null` for a file that should not exist yet): when the branch holds something else, the push fails with a conflict instead of overwriting it. A file that already holds the new content passes, so a push repeated after it landed succeeds.

When the branch moved while the commit was being built, the push fails as moved and the task's `retry` builds it again on the new tip, as long as every `previous` still holds. A change that leaves the tree as it is pushes nothing. Pushes from one task run one at a time, in no guaranteed order. A file keeps its executable bit when it is updated. A path that runs through an existing file, such as `a.yaml/b.yaml` when `a.yaml` is a file, is a conflict.

The output lists the text files under `path`; files that are not UTF-8 are left out.

## Configuration

```yaml
- git_push:
    name: commit
    repository_url: "{{env.GIT_REPOSITORY_URL}}"
    branch: main
    path: "configs/"
    credentials_path: /etc/git/credentials.json
    author:
      name: "{{event.data.author.name}}"
      email: "{{event.data.author.email}}"
    message: "{{event.data.title}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `repository_url` | string | required | Git repository URL. With `credentials_path` it must be HTTPS, or HTTP to a loopback host; SSH is not supported. |
| `branch` | string | `main` | Branch to push to. Created on the first push to an empty repository; in a repository with other branches it must exist. |
| `path` | string | | Directory within the repository the file paths are relative to. Paths that leave it are rejected. |
| `credentials_path` | string | | Path to [credentials JSON file](/docs/flowgen/git#credentials). |
| `author.name` | string | required | Commit author name. Supports templating. |
| `author.email` | string | required | Commit author email. Supports templating. |
| `committer.name` | string | author's | Committer name. Supports templating. |
| `committer.email` | string | author's | Committer email. Supports templating. |
| `signing_key_path` | string | | Path to an unencrypted OpenSSH Ed25519 private key that signs each commit. See [Signed commits](#signed-commits). |
| `message` | string | required | Commit message. Supports templating. |
| `timeout` | duration | `120s` | Time budget for each request to the git server, including the pack upload, and for fetching the branch and building the commit. |
| `connect_timeout` | duration | `10s` | TCP/TLS connect timeout. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). Conflicts, invalid paths, pushes the server rejects, and client errors (4xx other than 408 and 429) are not retried. |

### Input

```json
{
  "files": [
    { "path": "settings/a.yaml", "content": "enabled: true", "previous": null },
    { "path": "nested/b.txt", "content": null }
  ]
}
```

## Signed commits

With `signing_key_path`, each commit is signed the way `git commit -S` signs with `gpg.format=ssh`. Git hosts verify the signature against the committer's account, and branches that require signed commits accept only verified ones. Set `committer` to the account that holds the key, and keep `author` as the person behind the change:

```yaml
- git_push:
    name: commit
    repository_url: "{{env.GIT_REPOSITORY_URL}}"
    credentials_path: /etc/git/credentials.json
    signing_key_path: /etc/git/signing_key
    committer:
      name: "flowgen"
      email: "flowgen@example.com"
    author:
      name: "{{event.data.author.name}}"
      email: "{{event.data.author.email}}"
    message: "{{event.data.title}}"
```

Create the key with `ssh-keygen -t ed25519 -N "" -f signing_key`, mount the private key at `signing_key_path`, and add `signing_key.pub` as a signing key to the committer's account on the git host. The committer's email must be a verified email of that account.

## Output

| Field | Type | Description |
|---|---|---|
| `commit` | string | The branch tip after the push. |
| `changed` | bool | `false` when the files left the tree unchanged and nothing was pushed. |
| `files` | array | Every file under `path` at `commit`, as `{path, content}`. Feeds [OCI Push](/docs/flowgen/oci/push) directly. |

## Examples

- [`examples/git/push_snapshot.yaml`](https://github.com/connve/flowgen/blob/main/examples/git/push_snapshot.yaml) commits a daily API snapshot.
- [`examples/authoring/publish_workspace.yaml`](https://github.com/connve/flowgen/blob/main/examples/authoring/publish_workspace.yaml) commits approved [authoring](/docs/flowgen/concepts/authoring) changes.
