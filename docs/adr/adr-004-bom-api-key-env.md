---
directory or file:
  - config/
status: Accepted
date: 2026-01-20T15:21:00.000Z
supersedes: []
---

# Context (short)

- The BoM Space Weather API key is a secret.
- Secrets must not be committed to git or stored directly in config files.

# Decision (1-3 bullets)

- Store the actual BoM API key in an environment variable.
- Store only the environment variable name in YAML config.
- Load secrets from the environment or an ignored `.env` file.

# Rationale (why)

- Prevents API keys from leaking through source control.
- Allows Docker and local environments to provide secrets without changing source code.

# Alternatives considered (bullets)

- Store the raw API key in config. Rejected because it risks secret leakage.

# Consequences (tradeoffs)

- Requires `python-dotenv` or equivalent local environment loading for `.env` workflows.
- Developers must ensure secret-bearing files remain ignored.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca80cc9ea8e68abb2eac72

