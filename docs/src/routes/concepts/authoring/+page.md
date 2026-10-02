# Authoring

Flows and resources can be changed through the web API instead of editing the repository by hand. A change is proposed, reviewed in the web UI, and published by a flow once a signed-in user approves it. Pending changes are linked from the Flows and Resources pages and from each flow or resource they touch.

In the web UI, **New flow** and **New resource** on the list pages and **Edit** on a flow or resource open an editor that validates the file and proposes it as a change. A flow is edited as `flows/<identity>.yaml`; correct the path when the repository stores it as `.yml` or `.json`, or the change adds a second file next to it.

```
propose ──► validate + diff ──► approve (signed-in user) ──► publish flow
                                                             git_push ─► oci_push ─► oci_sync
```

## Proposing

`POST {path}/api/changes` takes a title, an optional description, and the files to add, change, or delete:

```json
{
  "title": "Sync orders every 10 minutes",
  "files": [
    { "path": "flows/orders/sync.yaml", "content": "flow:\n  tasks: ..." },
    { "path": "resources/scripts/old.rhai", "content": null }
  ]
}
```

Paths start at the workspace root and must be under `flows/` or `resources/`. Each file is recorded with the content deployed when the change was proposed — for a flow, the source synced to the flows cache, so a flow that failed to start can be fixed too — and the change carries a unified diff per file plus every validation issue. A change with issues can be reviewed but not approved.

`POST {path}/api/workspace/validate` runs the same validation without recording anything:

- flows are parsed, and an unknown field is reported with the task it is in (for example ``unknown field `intreval` ... for key `flow.tasks[0]` ``);
- task wiring (`depends_on`, duplicate names) is checked, as are the settings of `generate`, `kafka_produce` and `kafka_subscribe`, and inline scripts must compile;
- `.rhai` resources must compile and `.json` resources must parse.

## Approving

`POST {path}/api/changes/{id}/approve` and `/reject` require a signed-in user, and a member of one of `web.authoring.approver_groups` when the list is set. Groups are read from the user's `groups_claim` (default `groups`); most providers include it only when asked, for example with a `groups` scope in `web.auth.extra_scopes`. Requests authenticated with a machine key are refused, so an agent can propose changes but never publish them. Approving moves the change to `publishing`, runs the publish flow, and records `published` with the flow's result, or `failed` with the error. Publishing runs to the end even when the browser disconnects, and is recorded as failed after `web.authoring.publish_timeout` (5 minutes by default).

A failed change can be approved again, as can one left in `publishing` past the timeout, for example after a restart. [Git Push](/docs/flowgen/git/push) accepts files that already hold the proposed content, so publishing again after a partial success does not conflict.

The publish flow is any flow starting with an `http_endpoint`; `web.authoring.publish_endpoint` names its endpoint. Approvals reach it in-process, without the endpoint's credentials, so give the endpoint a `credentials_path` to keep it closed on `http_server`. It receives:

| Field | Description |
|---|---|
| `id`, `title` | The change. |
| `author` | `{name, email}` of the approving user, from the identity provider's claims. |
| `files` | `{path, content, previous}` per file. |

The approving user's identity is in `event.meta.auth`. [`examples/authoring/publish_workspace.yaml`](https://github.com/connve/flowgen/blob/main/examples/authoring/publish_workspace.yaml) commits the files with [Git Push](/docs/flowgen/git/push) — which fails when a file changed since the change was proposed — and releases the workspace with [OCI Push](/docs/flowgen/oci/push), tagged with the commit, for the [OCI Sync](/docs/flowgen/oci/sync) bootstrap to deploy.

## Machine keys

Agents, scripts, and CI reach the API with a machine key instead of a session:

```yaml
web:
  auth: { ... }
  api_credentials_path: /etc/flowgen/credentials/api.json
  authoring:
    publish_endpoint: /workspace/publish
    approver_groups: ["flowgen-approvers"]
```

```json
{ "api_keys": [{ "name": "operator-agent", "key": "a random string of at least 32 characters" }] }
```

Send the key as `Authorization: Bearer <key>`. The key's name is recorded as the proposer (`key:operator-agent`). Keys shorter than 32 characters are ignored, and keys are read at startup.

A machine key reaches only what an agent needs: reading flows, resources, logs and changes (`GET`), validating files, and proposing changes. Every other route answers `403` to a key. Machine keys apply only when `web.auth` is set; without it the API is open and anyone can approve.

## Operator agent

[`examples/authoring/flow_operator.yaml`](https://github.com/connve/flowgen/blob/main/examples/authoring/flow_operator.yaml) is an agent for the built-in Agents chat. Its tools — flows in [`examples/authoring/tools/`](https://github.com/connve/flowgen/tree/main/examples/authoring/tools) calling the web API with a machine key — list and read deployed flows and resources, validate drafts, propose changes, and read a flow's logs after it is deployed.
