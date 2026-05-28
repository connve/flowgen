<script>
  import Mermaid from '$lib/components/Mermaid.svelte';
</script>

# Salesforce Deduplication

Merge duplicate Salesforce records using the SOAP API. The losing records are deleted and their related records (child relationships, activities, attachments) are automatically reparented to the master. Supported types: Account, Contact, Lead, Individual.

## Architecture

A deduplication pipeline reads a list of duplicate pairs, iterates over them, and calls the Salesforce SOAP merge API for each pair.

<Mermaid chart={`
graph LR
  SRC[source] --> IT[iterate] --> MERGE[salesforce_soapapi_merge] --> LOG[log]
`} />

The source task is up to you — read a file from local disk, subscribe to a NATS stream, accept webhook calls, or use any other input task. The examples below use `object_store` to read a local file of duplicate pairs.

## Merge from file

Read a JSON file containing duplicate pairs and merge each one.

<Mermaid chart={`
graph LR
  A[read_duplicates] --> B[iterate] --> C[merge] --> D[audit]
`} />

The input file contains an array of objects, each with the master and duplicate record IDs:

```json
[
  {"sobject_type": "Account", "master_id": "001000000000001", "duplicate_id": "001000000000002"},
  {"sobject_type": "Account", "master_id": "001000000000003", "duplicate_id": "001000000000004"}
]
```

```yaml
flow:
  name: salesforce_dedup
  tasks:

    - object_store:
        name: read_duplicates
        operation: read
        path: file:///data/salesforce/duplicates.json

    - iterate:
        name: iterate

    - salesforce_soapapi_merge:
        name: merge
        credentials_path: /etc/sfdc/credentials.json
        sobject_type: "{{event.data.sobject_type}}"
        master_record_id: "{{event.data.master_id}}"
        record_ids_to_merge:
          - "{{event.data.duplicate_id}}"

    - log:
        name: audit
```

## Merge with field overrides

Override specific fields on the master record during merge. Useful when the losing record has more accurate data for certain fields.

```yaml
- salesforce_soapapi_merge:
    name: merge_contacts
    credentials_path: /etc/sfdc/credentials.json
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

Up to two duplicate records can be merged into the master in a single call. The `allow_duplicate_save` flag bypasses Salesforce duplicate detection rules that might otherwise block the merge.

## Output

Each merge produces a JSON result:

```json
{
  "success": true,
  "merged_record_ids": ["001000000000002"],
  "updated_related_ids": ["003000000000001", "006000000000001"]
}
```

| Field | Type | Description |
|---|---|---|
| `success` | bool | Whether the merge operation succeeded. |
| `merged_record_ids` | array | IDs of the victim records that were deleted. |
| `updated_related_ids` | array | IDs of related records reparented to the master. |

## Credentials

| File | Purpose | Contents |
|---|---|---|
| `/etc/sfdc/credentials.json` | Salesforce SOAP API | `client_id`, `client_secret`, `username`, `password`, `login_url` |

See [Credentials](/docs/flowgen/concepts/credentials) for format details.
