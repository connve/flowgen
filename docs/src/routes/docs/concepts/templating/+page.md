# Templating

Most task configuration fields support templates. Before a task processes an event, its configuration is rendered against the event data — so endpoints, query parameters, table names, and other fields can pull values out of the incoming event.

Flowgen uses [Handlebars](https://handlebarsjs.com/) syntax: `{{ path.to.value }}`.

## Where templating runs

For every incoming event, the task's configuration is cloned and re-rendered with the event as input. This happens before the task's handler runs, so templates always see the current event:

```
event arrives → render(config, event) → handler(event)
```

A task's static YAML thus becomes a per-event configuration. Connectors that load credentials, schemas, or queries do so against the rendered config.

## Available variables

| Variable | Description |
|---|---|
| `event.data` | Event payload. JSON shape depends on the source (HTTP body, script return value, query result, etc.). |
| `event.meta` | Key-value metadata that travels with the event (correlation_id, user auth, custom keys set by scripts). |
| `event.id` | Optional event identifier set by the source (e.g., a message ID). |
| `event.subject` | Subject string set by the producing task. |
| `env.*` | Environment variables of the flowgen process. |

## Examples

**HTTP endpoint with a path parameter:**

```yaml
- http_request:
    name: lookup
    endpoint: "https://api.example.com/users/{{event.data.user_id}}"
    method: GET
```

**Routing key derived from event metadata:**

```yaml
- nats_jetstream_publisher:
    name: publish
    subject: "orders.{{event.meta.region}}.created"
```

**Pulling a secret from environment:**

```yaml
- http_request:
    name: enrich
    endpoint: "https://api.example.com/data"
    headers:
      Authorization: "Bearer {{env.PARTNER_API_TOKEN}}"
```

**Composing a key prefix from event data:**

```yaml
- nats_kv_store:
    name: cache_user
    operation: put
    key: "users.{{event.data.id}}"
    value: "{{event.data}}"
```

## Type preservation for object and array values

When a template is exactly `{{ path }}` (no surrounding text, no helpers), flowgen reads the value from the data tree directly instead of converting it to a string. This means objects, arrays, booleans, and numbers retain their original types:

```yaml
- nats_jetstream_publisher:
    name: forward
    # The whole event.data object is published as-is, not stringified.
    payload:
      from_event: false
      object: "{{event.data}}"
```

```yaml
- gcp_bigquery_job:
    name: load
    operation: create
    # source_uris stays a list, not a string representation of one.
    source_uris: "{{event.meta.source_uris}}"
```

If you need a stringified value, embed the template inside other text: `"key.{{event.data.id}}"` always renders as a string.

## Booleans and numbers in fields that expect them

Numeric and boolean fields (such as `port`, `enabled`, `max_results`) accept template strings that resolve to a number or boolean. The renderer parses the substituted result as JSON and re-typed it before deserialising the config:

```yaml
- http_request:
    name: paged
    endpoint: "https://api.example.com/items"
    headers:
      X-Page-Size: "{{event.data.page_size}}"
```

## What templating does not do

- It does not run on every field — fields excluded from rendering (binary blobs, sensitive secrets, raw bytes) stay as-is. Most string and structured fields are rendered.
- It does not loop or iterate. Use the `iterate` task for fan-out over arrays.
- It does not call functions or helpers (no `{{#if}}`, `{{#each}}`, etc.). For conditional logic, use a `script` task to compute the value, then template against the script's output.
- It does not silently default missing variables — referencing a missing path produces a runtime error.

## Templates and resource files

Resource-loaded content (SQL queries, prompts, HTML templates) is also rendered. See [Resources](/docs/concepts/resources) for inline-vs-file-vs-cache trade-offs.

```yaml
- gcp_bigquery_query:
    name: daily_report
    query:
      resource: queries/daily_report.sql
    parameters:
      report_date: "{{event.data.date}}"
```

The SQL file itself can use the same `{{ event.data.x }}` syntax — flowgen renders the loaded content against the event before executing the query.
