# Useful commands

`git note`
- Attach metadata directly to your current commit without modifying the codebase or creating a new commit.
- Example: `git notes add -m "Stage: 4. Refining spec"`
  - To view: Run `git log --show-notes=*` or just `git note show`.
  - Why it works: The note stays bound to your branch’s latest commit, moving with you when you switch branches.