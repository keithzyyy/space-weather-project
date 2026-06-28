---
directory or file:
  - requirements-dev.txt
status: Accepted
date: 2026-01-20T10:44:00.000Z
supersedes: []
---

# Context (short)

- Local development uses Conda, but the project is intended to run in Docker.
- Installing Conda inside Docker would add unnecessary complexity.

# Decision (1-3 bullets)

- Use a Conda environment for local development if desired.
- Install Python packages with `pip` so requirements files work in Docker.
- Keep Jupyter/ipykernel in the local environment for notebook work.

# Rationale (why)

- Docker can use plain Python images and `pip install -r ...` without needing Conda.
- This keeps local development convenient while preserving deployability.

# Alternatives considered (bullets)

- Install Conda inside Docker. Rejected because it is more complex than needed.

# Consequences (tradeoffs)

- Requirements files must remain trustworthy.
- This ADR is partially refined by `adr-021-dependency-install-strategy.md`, which rejects broad `pip freeze` dumps as the requirements source of truth.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca8016a2e3ebe54b0f7474

