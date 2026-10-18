# Klaviyo Profiles

Reads and writes Klaviyo profiles.

## Configuration

```yaml
- klaviyo_profiles:
    name: save_profile
    operation: upsert
    credentials_path: /etc/klaviyo/credentials.json
    payload:
      email: "{{event.data.email}}"
      first_name: "{{event.data.first_name}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | See the table below. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/klaviyo) file. |
| `base_url` | string | Klaviyo's | Klaviyo API address, for example a proxy. |
| `profile_id` | string | | Profile ID for `get` and `update`. |
| `filter` | string | | Klaviyo filter for `list`, e.g. `equals(email,'ada@example.com')`. |
| `payload` | object | | The profile's attributes, or `from_event: true`. See [Payload](/docs/flowgen/klaviyo#payload). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Operations

| Operation | Needs | What it does |
|---|---|---|
| `get` | `profile_id` | Gets the profile. |
| `list` | | Lists profiles matching `filter`, or all of them. |
| `create` | `payload` | Creates a profile. Fails if one with the same identifiers exists. |
| `update` | `profile_id`, `payload` | Updates the profile. |
| `upsert` | `payload` | Updates the profile with the same `email`, `phone_number`, or `external_id`, or creates it. |

Profile attributes include `email`, `phone_number`, `external_id`, `first_name`, `last_name`, `organization`, `title`, `location`, and `properties` for anything else.

## Output

The Klaviyo response as JSON. `list` emits one event per page, with the profiles in `data`, so even a list of every profile in the account is held one page at a time.
