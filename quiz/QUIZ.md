## Idea: ask your agent/codex to act as an active examiner to drill you on your own codebase to pay off cognitive debt

## Spec
Act as a repository-grounded technical examiner.

Scope:
- [spec]
- [source module]
- [entrypoint]
- [tests]
- [linked ADRs]

Ask me five questions:
1. one architecture/data-flow question;
2. one execution-trace question;
3. one failure-mode question;
4. one spec/source/test consistency question;
5. one design-rationale question.

Do not provide answers, hints, summaries, or file excerpts until I answer.
Prefer open-ended questions; use multiple choice only when the distinction
itself is the subject being tested.

After I answer:
- grade each response as correct, partially correct, or incorrect;
- explain omissions and misconceptions;
- cite exact repository files and lines;
- distinguish documented facts from inference;
- identify no more than three concepts for later review.

Treat the specification as the intended contract, but report implementation
debt or disagreement rather than silently resolving it.

## Attempt File Convention

- Store each completed attempt in `quiz/attempts/`.
- Name files `YYYY-MM-DD-<topic>-attempt-<zero-padded-number>.md`.
- Increment attempt numbers independently for each topic.
- Keep `QUIZ.md` as the guide and template; do not maintain an attempt index.

```markdown
# 2026-07-29 — OMNI Ingestion — Attempt 1

## Repository State

- Revision: `<commit SHA or branch>`
- Working tree: `clean` or `uncommitted changes present`

## Scope

- Spec: `specs/spec-05-ingest-omni-dataset.md`
- Source: `src/ingest/omni.py`
- Entrypoint: `entrypoint/ingest_omni.py`
- Tests: `tests/omni/`
- ADRs: `<relevant ADR paths>`

## Conditions

- Mode: Closed-book
- Questions delivered: All at once

## Question 1 — Architecture and data flow

**Question**

> <Exact question asked>

**Response**

> <User response preserved verbatim>

**Grade:** Correct | Partially correct | Incorrect

**Assessment**

<What was understood correctly, followed by omissions or misconceptions.>

**Evidence**
- Contract: `specs/spec-05-ingest-omni-dataset.md:<line>`
- Implementation: `src/ingest/omni.py:<line>`
- Test evidence: `tests/omni/<test-file>.py:<line>`
- Inference: <Only include when the assessment contains an inference>

## Question 2 — Execution trace

<Repeat the same structure>

## Question 3 — Failure mode

<Repeat the same structure>

## Question 4 — Spec/source/test consistency

<Repeat the same structure>

## Question 5 — Design rationale

<Repeat the same structure>

## Attempt Summary

- Correct: 0
- Partially correct: 0
- Incorrect: 0

## Concepts for Later Review

1. `<Concept>` — `<specific gap revealed by the attempt>`
2. `<Concept>` — `<specific gap revealed by the attempt>`
3. `<Concept>` — `<specific gap revealed by the attempt>`
```
