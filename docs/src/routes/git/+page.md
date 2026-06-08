# Git

Flowgen can sync flow definitions and resources from a Git repository.

- [Git Sync](/docs/flowgen/git/sync) — pulls a remote Git repository on a schedule and reconciles flow definitions.

## Credentials

Git tasks authenticate via a JSON credentials file referenced by `credentials_path`. Only HTTPS token authentication is supported.

```json
{
  "token": "ghp_xxxxxxxxxxxx"
}
```

The token can be a GitHub PAT, GitLab deploy token, or any HTTPS basic-auth token. SSH URLs (`git@...`, `ssh://...`) are not supported.
