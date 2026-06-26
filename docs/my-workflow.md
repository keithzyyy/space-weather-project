# My current workflow to make a new feature
1. Create branch.
2. Create rough spec first.
3. Use notebook only as a spike/scratchpad.
4. Refine spec until interfaces and contracts are stable.
5. Create test matrix from spec.
6. Generate/review unit tests before moving implementation into `src/`.
7. Move implementation into `src/`.
8. Add entrypoint/ only once core behavior is stable.
9. Write ADR only when a durable decision was made.
10. Promote only cross-cutting stable rules into `AGENTS.md`.

# Remarks
1. New work follows current `AGENTS.md`.
2. Component behavior follows the latest non-superseded spec.
3. Durable changes in philosophy or architecture get a new ADR.