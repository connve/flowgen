# Google Cloud Platform

Flowgen connects to GCP via [BigQuery Query](/docs/flowgen/gcp/bigquery-query), [BigQuery Jobs](/docs/flowgen/gcp/bigquery-jobs), and [BigQuery Storage API](/docs/flowgen/gcp/bigquery-storage).

## Credentials

The `credentials_path` field points to a GCP service account JSON file. Download it from the GCP Console under IAM > Service Accounts > Keys.

```json
{
  "type": "service_account",
  "project_id": "my-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "my-sa@my-project-id.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}
```

When `credentials_path` is omitted, all BigQuery tasks fall back to Application Default Credentials in this order:

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account JSON file.
2. Workload Identity (GKE).
3. Compute Engine metadata server.
