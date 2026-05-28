# Salesforce Merge (SOAP)

Merges duplicate SObject records into a single master record via the Salesforce SOAP API. The losing records are deleted and their related records (child relationships, activities, etc.) are automatically reparented to the master. Supported types: Account, Contact, Lead, Individual.

## Configuration

```yaml
- salesforce_soapapi_merge:
    name: merge_accounts
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Account
    master_record_id: "{{event.data.master_id}}"
    record_ids_to_merge:
      - "{{event.data.duplicate_id}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to Salesforce authentication credentials. |
| `sobject_type` | string | required | SObject type (Account, Contact, Lead, or Individual). |
| `master_record_id` | string | required | Salesforce ID of the winning record. Supports templating. |
| `record_ids_to_merge` | list | required | IDs of records to merge into the master (one or two). Supports templating. |
| `master_field_overrides` | object | | Optional field values to set on the master record during merge. Supports templating. |
| `allow_duplicate_save` | bool | `false` | Bypass duplicate detection rules during merge. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `success` | bool | Whether the merge operation succeeded. |
| `merged_record_ids` | array | IDs of the victim records that were merged. |
| `updated_related_ids` | array | IDs of related records reparented to the master. |

## Examples

**Merge two duplicate accounts:**

```yaml
flow:
  name: deduplicate_accounts
  tasks:
    - generate:
        name: trigger
        payload:
          master_id: "001000000000001"
          duplicate_id: "001000000000002"
    - salesforce_soapapi_merge:
        name: merge_accounts
        credentials_path: /etc/salesforce/credentials.json
        sobject_type: Account
        master_record_id: "{{event.data.master_id}}"
        record_ids_to_merge:
          - "{{event.data.duplicate_id}}"
    - log:
        name: result
```

**Merge with field overrides and duplicate detection bypass:**

```yaml
- salesforce_soapapi_merge:
    name: merge_contacts
    credentials_path: /etc/salesforce/credentials.json
    sobject_type: Contact
    master_record_id: "{{event.data.master_id}}"
    record_ids_to_merge:
      - "{{event.data.dup_1}}"
      - "{{event.data.dup_2}}"
    master_field_overrides:
      Email: "{{event.data.preferred_email}}"
      Phone: "{{event.data.preferred_phone}}"
    allow_duplicate_save: true
```

**Event-driven deduplication pipeline:**

```yaml
flow:
  name: nats_dedup_pipeline
  tasks:
    - nats_jetstream_subscriber:
        name: duplicates
        stream:
          name: salesforce_duplicates
        subject: salesforce.duplicates.>
    - salesforce_soapapi_merge:
        name: merge
        credentials_path: /etc/salesforce/credentials.json
        sobject_type: "{{event.data.sobject_type}}"
        master_record_id: "{{event.data.master_id}}"
        record_ids_to_merge:
          - "{{event.data.duplicate_id}}"
    - log:
        name: audit
```
