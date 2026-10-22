# Klaviyo Templates

Reads and writes Klaviyo email templates.

## Configuration

```yaml
- klaviyo_templates:
    name: save_template
    operation: upsert
    credentials_path: /etc/klaviyo/credentials.json
    payload:
      from_event: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | See the table below. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/klaviyo) file. |
| `base_url` | string | Klaviyo's | Klaviyo API address, for example a proxy. |
| `template_id` | string | | Template ID for `get`, `update`, `delete`, `render`, and `clone`. |
| `filter` | string | | Klaviyo filter for `list`, e.g. `equals(name,'Welcome')`. |
| `fields` | list | all | Template attributes `get` and `list` return, e.g. `[name, editor_type, html]`. Leaves out the drag-and-drop `definition`, which can be large. |
| `payload` | object | | The template's attributes, or `from_event: true`. See [Payload](/docs/flowgen/klaviyo#payload). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Operations

| Operation | Needs | What it does |
|---|---|---|
| `get` | `template_id` | Gets the template. |
| `list` | | Lists templates matching `filter`, or all of them. |
| `create` | `payload` | Creates a template. Attributes: `name`, `editor_type` (e.g. `CODE`), `html`, `text`. |
| `update` | `template_id`, `payload` | Updates the template. |
| `upsert` | `payload` with `name` | Updates the template with that name, or creates it. Keeps the number of templates down: Klaviyo allows 1,000 templates created through the API. |
| `delete` | `template_id` | Deletes the template. |
| `render` | `template_id`, `payload` with `context` | Renders the template with the variables in `context`. |
| `clone` | `template_id` | Copies the template, named `payload.name` if set. |

## Output

The Klaviyo response as JSON. `list` emits one event per page, with the templates in `data`. `delete` emits `{"id": "...", "done": true}`.
