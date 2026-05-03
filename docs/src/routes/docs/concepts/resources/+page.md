# Resources

Tasks often reference content that doesn't belong in YAML — SQL queries, LLM prompts, JSON schemas, HTML templates, Rhai scripts. Flowgen lets you embed these inline for quick prototyping, load them from disk for clean version control, or fetch them from the distributed cache for hot-reload across replicas.

The format is the same `Source` enum everywhere a resource-style field appears (`query`, `code`, `prompt`, `schema`, `template`, etc.):

```yaml
# Inline.
query: "SELECT * FROM orders"

# Inline multi-line.
query: |
  SELECT id, total
  FROM orders
  WHERE created_at > '{{event.data.since}}'

# External file.
query:
  resource: queries/recent_orders.sql
```

When a resource is referenced, flowgen loads it through the configured `ResourceLoader` and renders it as a Handlebars template against the current event (see [Templating](/docs/concepts/templating)).

## Backends

The `ResourceLoader` has three backends, configured at the worker level under `resources`:

| Backend | When to use | Key resolution |
|---|---|---|
| **Filesystem** (default) | Local development, mounted ConfigMaps in Kubernetes, simple deployments. | `resources.path = "/etc/flowgen/resources"` → key `queries/orders.sql` resolves to `/etc/flowgen/resources/queries/orders.sql`. |
| **Cache** | Hot-reload across replicas, git-synced flows, multi-tenant workers. | `resources.cache.prefix = "flowgen.resources"` → key `queries/orders.sql` resolves to cache key `flowgen.resources.queries/orders.sql`. |
| **Disabled** | Inline-only flows. | All `resource:` references fail with a clear error. Default when neither `path` nor `cache` is configured. |

## Filesystem backend

```yaml
resources:
  path: /etc/flowgen/resources/
```

Resource keys are paths under `path`. Use sub-directories to organise by task type or domain:

```
/etc/flowgen/resources/
├── queries/
│   ├── recent_orders.sql
│   └── customer_summary.sql
├── prompts/
│   └── classifier.txt
└── schemas/
    └── order.avsc
```

Files are read on each task invocation — there is no in-memory caching. Edit a file and the next event picks it up. (The `git_sync` task pairs naturally with this: sync a Git repo into `resources.path` to deploy resource changes without restarting the worker.)

## Cache backend

```yaml
resources:
  cache:
    enabled: true
    prefix: flowgen.resources
    db_name: flowgen_system
```

Resource keys map to entries in the distributed cache (NATS JetStream KV). Every replica reads the same content, and updates propagate without restarting workers.

This is the recommended setup for:

- **Multi-replica deployments** where you want to update a query and have all replicas see the change atomically.
- **Git-synced flows** — a `git_sync` task can write resource files into the cache as it pulls them, and other replicas pick them up via cache subscription.
- **Multi-tenant workers** where resources are templated per tenant and provisioned through an API.

The cache backend reuses the same cache configuration as the rest of flowgen (see [Caching](/docs/concepts/caching)).

## Inline vs resource trade-offs

| Choice | Pros | Cons |
|---|---|---|
| Inline | Single file per flow, easy to read end-to-end. | Long YAML, no syntax highlighting for the embedded content, harder to diff. |
| Filesystem resource | Editor support for the actual file format, clean diffs, reuse across flows. | Requires deploying both YAML and resource files together. |
| Cache resource | Live updates, fits multi-replica deployments, fits API-driven provisioning. | Requires the cache infrastructure to be available; adds an extra read per event (cached values are small and fast). |

A common pattern is to use inline content during initial development, then promote frequently-edited content (queries, prompts) to the filesystem or cache once the flow stabilises.

## Templating in resources

Resource files are rendered as Handlebars templates against the event before the task uses them. A SQL file can reference `{{event.data.user_id}}` directly:

```sql
-- queries/recent_orders.sql
SELECT id, total, created_at
FROM `project.dataset.orders`
WHERE customer_id = '{{event.data.customer_id}}'
  AND created_at >= '{{event.data.since}}'
ORDER BY created_at DESC
LIMIT 100
```

The same templating rules from [Templating](/docs/concepts/templating) apply: simple `{{ path }}` substitution, environment variables under `{{env.X}}`, and runtime errors for missing variables.

For SQL specifically, prefer the task's `parameters` field over template substitution when the value is user-controlled — it produces a parameterised query and avoids injection. See the BigQuery and MSSQL pages for details.

## Errors

| Error | Cause |
|---|---|
| `Resource path is not configured in config.yaml` | A task references a `resource:` source but `resources` is not configured at the worker level. |
| `Error reading resource '<key>': ...` | Filesystem read failed (file does not exist, permission denied, IO error). |
| `Cache resource '<key>' is not valid UTF-8: ...` | Cache backend returned bytes that are not a UTF-8 string. Resources must be text. |
| Handlebars render error | Template references a variable that does not exist in the event context. |
