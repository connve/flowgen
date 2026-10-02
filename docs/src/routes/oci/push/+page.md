# OCI Push

Packs the files of an event into a single-layer OCI artifact and pushes it to a registry under one or more tags. The layer is a tar+gzip of the files at their paths, which is the layout [OCI Sync](/docs/flowgen/oci/sync) reads back.

The input event carries `files`, each `{path, content}` — the output of [Git Push](/docs/flowgen/git/push) fits as it is.

## Configuration

```yaml
- oci_push:
    name: release
    repository: "{{env.OCI_REPOSITORY}}"
    tags: ["{{event.data.commit}}", "latest"]
    credentials_path: /etc/registry/credentials.json
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `repository` | string | required | Registry and repository without a tag, e.g. `registry.example.com/team/workspace`. |
| `tags` | list | required | Tags to push the artifact under. Supports templating. |
| `credentials_path` | string | | Registry credentials, in either format [OCI Sync](/docs/flowgen/oci/sync) accepts. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

Tagging every release with an immutable tag (such as the commit SHA) next to a moving one lets each workspace follow the moving tag while any release can be promoted or rolled back by its own tag.

## Output

The input data, with `files` replaced by:

| Field | Type | Description |
|---|---|---|
| `digest` | string | Manifest digest, the same under every tag. |
| `references` | array | Every reference pushed, `<repository>:<tag>`. |

## Examples

- [`examples/oci/push_artifact.yaml`](https://github.com/connve/flowgen/blob/main/examples/oci/push_artifact.yaml) publishes files posted to a webhook.
- [`examples/authoring/publish_workspace.yaml`](https://github.com/connve/flowgen/blob/main/examples/authoring/publish_workspace.yaml) releases the files of an approved [authoring](/docs/flowgen/concepts/authoring) change after Git Push.
