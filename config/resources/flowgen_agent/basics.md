# Flowgen basics

## Flows

A flow is a YAML file under `flows/`. Its identity is its path without the
extension: `flows/orders/sync.yaml` is `orders/sync`.

```yaml
flow:
  labels:
    display_name: "Sync orders"
    description: "One sentence on what the flow does."
  tasks:
    - http_endpoint:
        name: ingest
        endpoint: /orders
        method: POST

    - script:
        name: shape
        code: |
          #{ id: event.data.id, total: event.data.total }

    - log:
        name: sink
```

- Every task is a map with one key, its type, and a unique `name`.
- The first task is the source. Tasks run as a chain: each receives the
  events of the task before it.
- `depends_on: [a, b]` makes a task receive from the named tasks instead,
  for fan-out and fan-in. Use it only where the wiring differs from the list.
- `retry` on a task overrides the app's retry.
- Unknown fields fail validation, so use only fields from the docs page of
  the task type (read_doc) or from a working flow.

## Events

An event has `data` (the payload: JSON, Arrow rows, Avro or bytes), `meta`
(a map that travels with the event), `subject`, `id` and `timestamp`.

## Templating

Most config fields are Handlebars templates rendered against each event:
`{{event.data.user_id}}`, `{{event.meta.region}}`, `{{event.subject}}`,
`{{env.NAME}}` for environment variables.

## Resources

Queries, prompts, schemas and Rhai scripts can live in `resources/` and be
referenced by key instead of inline:

```yaml
query:
  resource: queries/recent_orders.sql
```

A referenced file is rendered as a template against the event too.

## Scripts

`script` runs Rhai. `event` is the incoming event; `ctx.meta` sets metadata
on the next event. The return value becomes the next event's data: an
object or scalar is one event, an array is one event per element, and `()`
drops the event. Test every script with test_script.

## Credentials

Secrets never go in flows or resources. A task reads them from a file:
`credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/<name>.json"`, where
`<name>` is a connection added in Connections.

## Provider options

Options of one provider sit in a block named after it, such as
`vertexai:` on `ai_completion`, `s3:` on `object_store` for `s3://` paths
and `nats:` on `kv_store` for a NATS URL. Set for another provider, the
task fails to load.
