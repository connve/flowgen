# Klaviyo Lists

Reads Klaviyo lists, for example to find a list's ID by its name before creating a campaign for it.

## Configuration

```yaml
- klaviyo_lists:
    name: find_list
    operation: list
    credentials_path: /etc/klaviyo/credentials.json
    filter: "equals(name,'Newsletter')"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `get` or `list`. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/klaviyo) file. |
| `base_url` | string | Klaviyo's | Klaviyo API address, for example a proxy. |
| `list_id` | string | | List ID for `get`. |
| `filter` | string | | Klaviyo filter for `list`, e.g. `equals(name,'Newsletter')`. Without it, `list` returns every list. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

The Klaviyo response as JSON. `list` emits one event per page, with the lists in `data`; a name that matches no list gives one event with an empty `data`.
