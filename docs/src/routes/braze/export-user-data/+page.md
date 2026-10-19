# Braze Export User Data

Exports Braze user profiles.

## Configuration

```yaml
- braze_export_user_data:
    name: export_users
    operation: users_by_ids
    credentials_path: /etc/braze/credentials.json
    base_url: "{{env.BRAZE_REST_ENDPOINT}}"
    payload:
      from_event: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | See the table below. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/braze) file. |
| `base_url` | string | required | REST endpoint of the Braze instance, e.g. `https://rest.iad-01.braze.com`. |
| `payload` | object | required | The request body, or `from_event: true`. See [Payload](/docs/flowgen/braze#payload). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Operations

| Operation | Endpoint | Payload |
|---|---|---|
| `users_by_ids` | `POST /users/export/ids` | `external_ids` (up to 50), `user_aliases`, `device_id`, `braze_id`, `email_address`, or `phone`, and `fields_to_export`, which accounts created after 2024-08-22 must set. |
| `users_by_segment` | `POST /users/export/segment` | `segment_id` and `fields_to_export`, and optionally `custom_attributes_to_export`, `user_last_updated_at`, `callback_endpoint`, and `output_format`. |
| `users_by_global_control_group` | `POST /users/export/global_control_group` | `fields_to_export`, which accounts created after April 2021 must set, and optionally `callback_endpoint` and `output_format`. |

## Output

The Braze response as JSON. `users_by_ids` returns the profiles in `users` and the IDs Braze didn't find in `invalid_user_ids`. The segment and control group exports run in the background: the response holds the `object_prefix` of the files Braze writes, and a temporary download `url` when no storage bucket is set up, not the users themselves. Braze calls `callback_endpoint` when the files are ready.
