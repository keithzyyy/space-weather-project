---
directory or file:
  - specs/spec-02-k-index-preproc.md
  - suggestions/spec-template.md
status: Accepted
date: 2026-03-21T02:13:00.000Z
supersedes: []
---

# Context (short)

- Preprocessing needed a structured design before implementation.
- The project was moving toward spec-first feature development.

# Decision (1-3 bullets)

- Introduce markdown specs before implementation.
- Use a reusable spec template for future features.
- Capture invariants, edge cases, failure modes, and test expectations in specs.

# Rationale (why)

- Specs force clarity before coding.
- They reduce rework and help future agents generate implementation and tests from agreed behavior.

# Alternatives considered (bullets)

- Code first, document later. Rejected because it leads to inconsistency.
- Use an overly rigid spec. Avoided so the workflow can remain iterative.

# Consequences (tradeoffs)

- Slower initial development.
- Higher long-term consistency and easier onboarding.

# Link to commit (commit hash and/or commit name)

- Commit: `29e8ed0`
- Commit: `44b95ea`
- Commit: `34b49c7`
- Commit: `a11cf4d`
- Spec: `specs/spec-02-k-index-preproc.md`
- Notion source: https://app.notion.com/p/32a946dd9bca80d384b4cf002cb97410

