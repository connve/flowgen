# In-Process Calls

One flow calls another inside the same flowgen process and gets its result back, with no HTTP route, broker, or credentials.

- **`inproc_endpoint`** — a source that makes its flow callable at the flow's identity, e.g. `inproc/mask_contact` for `flows/inproc/mask_contact.yaml`.
- **`inproc_request`** — a processor that sends each event to a callable flow, waits until every leaf of that flow finishes, and emits the result.

Use it for sub-flows shared by several flows, a common error-handling flow, or flows that flowgen itself runs, such as the [authoring](/docs/flowgen/concepts/authoring) publish flow.

A flow with an `inproc_endpoint` runs on every pod, like a flow with an `http_endpoint`, and ignores `require_leader_election`; a call reaches the copy on the caller's pod. Calls are not queued: a call to a flow that is not running, or a flow restarting, fails and the caller's `retry` applies. For delivery that survives restarts, publish to NATS instead.

## inproc_endpoint

```yaml
- inproc_endpoint:
    name: on_call
    ack_timeout: 30s
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `ack_timeout` | duration | | How long a call waits for the flow to finish. Unbounded when unset. |
| `callers` | list | the flow's top-level folder | Identity prefixes of the flows allowed to call, e.g. `["user/", "platform/"]`. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

The caller's event data becomes this task's event data, and its `meta` is merged in, so `event.meta.auth` carries the caller's user.

By default a flow in a folder accepts calls only from flows in the same top-level folder: `system/publish_workspace` can be called by `system/...` flows but not by `user/...` flows. A flow outside any folder accepts every caller. Flowgen itself, such as an [authoring](/docs/flowgen/concepts/authoring) approval, may always call. Set `callers` to open a shared flow to other folders:

```yaml
# platform/enrich_customer.yaml, callable from user flows too
- inproc_endpoint:
    name: on_call
    callers: ["platform/", "user/"]
```

## inproc_request

```yaml
- inproc_request:
    name: mask
    flow: inproc/mask_contact
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `flow` | string | required | Identity of the flow to call. Supports templating. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). A flow that ran and failed, exceeded its `ack_timeout`, or does not accept this caller is not called again. |

### Output

The result of the called flow's last task as `event.data` (`null` when it returns nothing), with the input `meta`. A failed call forwards the event with `event.error` set.

## Example

- [`examples/inproc/mask_contact.yaml`](https://github.com/connve/flowgen/blob/main/examples/inproc/mask_contact.yaml) is a callable sub-flow.
- [`examples/inproc/call_mask_contact.yaml`](https://github.com/connve/flowgen/blob/main/examples/inproc/call_mask_contact.yaml) calls it.
