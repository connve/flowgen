# Export Users by IDs

Calls Braze `POST /users/export/ids` for every incoming event and emits the response as a JSON event downstream.

## Configuration

```yaml
- braze_export_users_ids:
    name: export_users
    credentials_path: /etc/braze/credentials.json
    external_ids:
      - "{{event.data.user_id}}"
    fields_to_export:
      - external_id
      - email
      - first_name
      - last_name
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to Braze credentials JSON file. |
| `external_ids` | list | | External identifiers to export (up to 50). Each value is rendered as a Handlebars template against the incoming event, so `"{{event.data.user_id}}"` becomes the upstream `user_id`. |
| `user_aliases` | list | | User aliases to export. Each alias needs `alias_name` and `alias_label`. |
| `device_id` | string | | Single device identifier. Rendered as a Handlebars template. |
| `braze_id` | string | | Single Braze internal identifier. Rendered as a Handlebars template. |
| `email_address` | string | | Single email address. Rendered as a Handlebars template. |
| `phone` | string | | Single phone number in E.164 format. Rendered as a Handlebars template. |
| `fields_to_export` | list | | Subset of fields returned per user. Required for accounts onboarded after 2024-08-22. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

Templates are rendered with the full event context. Use `{{event.data.<field>}}` to read from the payload and `{{event.meta.<field>}}` to read metadata set by upstream tasks. If a template renders to an empty string, that identifier is ignored. At least one identifier type must resolve to a non-empty value at runtime, otherwise the task fails permanently.

## Output

| Format | Description |
|---|---|
| JSON | Braze response containing `message`, `users`, and `invalid_user_ids`. |

## Example: Export from upstream event

```yaml
- braze_export_users_ids:
    name: export_user
    credentials_path: /etc/braze/credentials.json
    external_ids:
      - "{{event.data.user_id}}"
    fields_to_export:
      - external_id
      - email
```
