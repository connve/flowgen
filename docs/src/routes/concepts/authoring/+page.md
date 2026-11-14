# Authoring

Flows and resources can be changed through the web API instead of editing the repository by hand. A change is proposed, reviewed in the web UI, and published by a flow once a signed-in user approves it. Pending changes are linked from the Flows and Resources pages and from each flow or resource they touch.

In the web UI, **New flow** and **New resource** on the list pages and **Edit** on a flow or resource open an editor that validates the file and proposes it as a change. A flow is edited as `flows/<identity>.yaml`; correct the path when the repository stores it as `.yml` or `.json`, or the change adds a second file next to it.

```
propose ──► validate + diff ──► approve (signed-in user) ──► publish flow: git_push
                                                                  │
                                         CI or release flow ◄─────┘ commit
                                         oci push ─► oci_sync
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

Paths start at the workspace root and must be under `flows/`, `resources/` or `credentials/`, all within one [target](#targets). Credentials must be [encrypted](/docs/flowgen/concepts/credentials#encrypted-credentials-in-the-workspace); a change with a credentials file that is not is refused. Each file is recorded with the version it is based on: the deployed content — for a flow, the source synced to the flows cache, so a flow that failed to start can be fixed too — with every published change that is not deployed yet applied on top, so editing a file right after an approval builds on that commit. `GET {path}/api/workspace/file?path=` returns that version. The change carries a unified diff per file plus every validation issue. A change with issues can be reviewed but not approved.

While a change is pending, its proposer can extend it: `POST {path}/api/changes/{id}/files` adds files or replaces ones it has, with the same body as `files` above, and `PATCH {path}/api/changes/{id}` sets its `title` and `description`. A replaced file keeps the version recorded when it was first added. Saving in the web UI editor adds the file to the user's most recently edited pending change in the same target, or proposes a new one.

`POST {path}/api/workspace/validate` runs the same validation without recording anything:

- flows are parsed, and an unknown field is reported with the task it is in (for example ``unknown field `intreval` ... for key `flow.tasks[0]` ``);
- task wiring (`depends_on`, duplicate names) is checked, as are the settings of `generate`, `kafka_produce` and `kafka_subscribe`, and inline scripts must compile;
- `.rhai` resources must compile and `.json` resources must parse.

## Targets

`web.authoring.targets` splits the workspace into areas with their own approvers and publish flow. A target's `paths` are folders matched on whole path segments: `flows/user` covers `flows/user/a.yaml` but not `flows/username/a.yaml`. A path covered by several targets belongs to the one with the most specific folder, so a target over `flows/system/` owns `flows/system/a.yaml` even when another covers `flows/`. All files of a change must belong to one target; a change spanning two is rejected, and a path outside every target cannot be changed through the API.

A layout with three areas:

```
flows/system/     sync, publish, release — changed in the repository only
flows/platform/   shared flows maintained by the platform team
flows/user/       flows built by business users and agents
```

```yaml
web:
  authoring:
    enabled: true
    targets:
      - name: platform
        paths: ["flows/platform/", "resources/platform/"]
        approver_groups: ["platform-engineers"]
      - name: user
        paths: ["flows/user/", "resources/user/"]
        approver_groups: ["business-leads"]
```

Without `targets`, one `workspace` target covers `flows/`, `resources/` and `credentials/` and anyone signed in may approve. The web UI offers **Edit** only on files inside a target and shows the target of each change.

Flows in different folders are kept apart at runtime too: an [`inproc_endpoint`](/docs/flowgen/core/inproc) accepts calls from its own top-level folder by default, so `user/` flows cannot call `system/commit_workspace`.

## Approving

`POST {path}/api/changes/{id}/approve` and `/reject` require a signed-in user, and a member of one of the change's target's `approver_groups` when the list is set. Groups are read from the user's claim named by `web.authoring.groups_claim` (default `groups`); most providers include it only when asked, for example with a `groups` scope in `web.auth.extra_scopes`. Requests authenticated with a machine key are refused, so an agent can propose changes but never commit them. With `?updatedAt=` set to the change's `updatedAt` as reviewed, approving fails with 409 when the change was edited since. Approving moves the change to `committing`, runs the commit flow, and records `committed` with the flow's result, or `failed` with the error. Committing runs to the end even when the browser disconnects, and is recorded as failed after the `ack_timeout` of the commit flow's `inproc_endpoint`.

A failed change can be approved again, as can one left in `committing` past the timeout, for example after a restart; such a change has `stalled` set, and the web UI offers to retry it. [Git Push](/docs/flowgen/git/push) accepts files that already hold the proposed content, so committing again after a partial success does not conflict.

The commit flow is any flow starting with an [`inproc_endpoint`](/docs/flowgen/core/inproc); the target's `commit_flow` names it by its identity (default `system/commit_workspace`), so targets can share one commit flow or commit to different repositories. Approvals call it in-process, and it has no HTTP route, so nothing else can trigger a commit. It receives:

| Field | Description |
|---|---|
| `id`, `title` | The change. |
| `target` | The change's target. |
| `author` | `{name, email}` of the approving user, from the identity provider's claims; empty strings without sign-in. |
| `files` | `{path, content, previous}` per file. |

The approving user's identity is in `event.meta.auth`. [`config/commit_workspace.yaml`](https://github.com/connve/flowgen/blob/main/config/commit_workspace.yaml) commits the files with [Git Push](/docs/flowgen/git/push), which fails when a file changed since the change was proposed, and records the commit as the change's result.

## Lifecycle flows

The commit flow, with the release and deploy flows below, is in [`config/`](https://github.com/connve/flowgen/tree/main/config). The same three flows run on one machine and in a cluster; they read where to commit, release and deploy from environment variables. A workspace started by `flowgen` without `--config` holds them in `flows/system/` and sets the variables itself. In a cluster, `flows.system: true` runs them from flowgen itself, matching its version, and the deployment sets the variables, with the credentials files mounted; the chart's `flowgen.systemFlows` values do both.

| Variable | Set by flowgen to | In a cluster |
|---|---|---|
| `FLOWGEN_REPOSITORY` | The workspace's repository, a path on this machine, in a local run. | The repository's URL. |
| `FLOWGEN_WORKSPACE_PATH` | The workspace's folder in the repository, ending in `/`, or empty for the whole repository, in a local run. | The folder, or empty. |
| `FLOWGEN_GIT_CREDENTIALS` | | The [git credentials](/docs/flowgen/git#credentials) file. |
| `FLOWGEN_RELEASES` | `oci-layout://.flowgen/releases`, an OCI Image Layout next to the cache. | The registry repository, e.g. `ghcr.io/acme/workspace`. |
| `FLOWGEN_REGISTRY_CREDENTIALS` | | The [registry credentials](/docs/flowgen/oci#credentials) file. |
| `FLOWGEN_CACHE_URL` | The `cache`: `redb://<path>` or its NATS URL. | |
| `FLOWGEN_CACHE_CREDENTIALS` | The `cache` credentials file, or empty. | |
| `FLOWGEN_SYSTEM_BUCKET` | The system bucket. | |

flowgen sets a variable only when the environment does not. An empty credentials variable means none. Without sign-in, the commit is by the git user of the machine; a commit to a URL needs the approving user, so a cluster needs `web.auth`. The release and deploy flows have `require_leader_election`, so one pod runs each.

Once a change is committed, flowgen puts its commit at `commits.latest` in the system bucket. The release flow watches that key with [`kv_watch`](/docs/flowgen/kv-store/kv-watch) and records each release under `releases.latest` as well as `releases.<commit>`, which the deploy flow watches, so an approved change is released and deployed as soon as it is committed. Both flows also look every minute for commits and releases made outside flowgen.

## Releasing

An approved change ends as a commit, the same as a change pushed by hand, and the deployment releases every commit the same way. The workspace is deployed in one of three ways:

- straight from the repository, with [Git Sync](/docs/flowgen/git/sync) in place of OCI Sync in the deploy flow; see [Loading flows from Git](/docs/flowgen/git/guides/loading-flows);
- from an artifact the repository's CI pipeline releases, deployed by [`config/deploy_workspace.yaml`](https://github.com/connve/flowgen/blob/main/config/deploy_workspace.yaml) with [OCI Sync](/docs/flowgen/oci/sync); flowgen then needs only read access to the registry;
- from an artifact a release flow in flowgen releases. [`config/release_workspace.yaml`](https://github.com/connve/flowgen/blob/main/config/release_workspace.yaml) reads each new commit with [Git Sync](/docs/flowgen/git/sync) and pushes it with [OCI Push](/docs/flowgen/oci/push), tagged with the commit and `latest`, to a local OCI Image Layout or, with `repository` set to one, a registry.

An artifact lets every environment run a release by its tag and roll back to an earlier one. Exactly one thing should release it.

A `committed` change reports its `deployment` against what this instance runs: `waiting` until its commit is released, `released` once a release is recorded under `releases.<commit>` in the system bucket, and `deployed` when no file runs as before the change. A release flow records it with a [KV Store](/docs/flowgen/kv-store/kv-store) `put` after pushing; without such a record a change goes from `waiting` to `deployed`. Set `track_deployment: false` on a target that is not deployed to this instance automatically.

## Machine keys

Agents, scripts, and CI reach the API with a machine key instead of a session:

```yaml
web:
  auth: { ... }
  api_credentials_path: /etc/flowgen/credentials/api.json
  authoring:
    enabled: true
```

```json
{ "api_keys": [{ "name": "operator-agent", "key": "a random string of at least 32 characters" }] }
```

Send the key as `Authorization: Bearer <key>`. The key's name is recorded as the proposer (`key:operator-agent`). Keys shorter than 32 characters are ignored, and keys are read at startup.

A machine key reaches only what an agent needs: reading flows, resources, logs and changes (`GET`), validating files, testing scripts, proposing changes, and adding files to and editing the pending changes it proposed. Every other route answers `403` to a key. Machine keys apply only when `web.auth` is set; without it the API is open and anyone can approve.

## Flowgen Agent

A local workspace starts with Flowgen Agent, `flows/system/flowgen_agent.yaml`, an agent for the built-in Agents chat. Its tools are the flows in `flows/system/flowgen_agent/`, served as the MCP toolset `/mcp/v1/system/flowgen_agent`. They call the web API to list and read deployed flows and resources, validate drafts, run draft Rhai scripts on sample events with `POST /api/workspace/test`, propose changes, and read a flow's logs after it is deployed. With `web.auth` set, give each tool's `http_request` a machine key with `credentials_path`.

The agent asks the AI gateway for the model `default`, served by an `llm_proxy` named `default`, such as a model flow from a hub's templates. The agent and its tools reach the servers of the run through variables that every run sets unless they are set already:

| Variable | Default |
|---|---|
| `FLOWGEN_URL` | The web server, `http://<host>:<web.port><web.path>` |
| `FLOWGEN_MCP_URL` | The MCP server, `http://<host>:<mcp_server.port><mcp_server.path>` |
| `FLOWGEN_AI_GATEWAY_URL` | The AI gateway, `http://<host>:<ai_gateway.port><ai_gateway.path>` |

`<host>` is `host`, or `127.0.0.1` when `host` is `0.0.0.0`.
