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

The token is presented to the server through a gix credential helper that responds to the server's `WWW-Authenticate` challenge — the token never appears in the repository URL, in `.git/config`, or in logs.

`username` is optional and defaults to `x-access-token`, which is accepted by GitHub (PATs and App installation tokens), GitLab personal/deploy tokens, and Bitbucket app passwords. Override it when the host expects a specific literal username:

```json
{
  "token": "glpat-xxxxxxxxxxxx",
  "username": "oauth2"
}
```

Common values:

| Host / token type | `username` |
|---|---|
| GitHub PAT, GitHub App | omit (defaults to `x-access-token`) |
| GitLab personal/deploy token | omit, or `oauth2` for OAuth tokens |
| Bitbucket app password | omit |
| Bitbucket Cloud repository access token | `x-token-auth` |
| Azure DevOps PAT | any non-empty string |

SSH URLs (`git@...`, `ssh://...`) are not supported.
