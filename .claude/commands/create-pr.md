---
description: Create a pull request for the current branch following project conventions
context: fork
agent: pr-author
background: false
---
# /create-pr

Create a pull request for the current branch following the project's Git & PR conventions.

## Steps

1. **Pre-flight checks** — stop and report to the user if any of the following are true:
   - Current branch is `main`
   - There are uncommitted changes (`git status`)
   - There are merge conflicts
   - The branch has no commits ahead of `main` (`git log main..HEAD`)

2. **Collect information**
   - Get the current branch name (`git branch --show-current`)
   - Get the list of commits ahead of `main` (`git log main..HEAD --oneline`)
   - Get the full diff (`git diff main`)

3. **Draft PR content** based on the diff and commits:
   - **Title**: Conventional Commits format `<type>(<scope>): <short description>`
     - `type`: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`, `ci`, `perf`, `style`
     - `scope`: `client`, `budget`, `storage`, `transform`, `codes`, `cli`, `api`, `dags`, `infra`, `web`, or omitted for project-wide
     - Breaking changes: add `!`
   - **Description**: use the template below

4. **Present the draft** to the user and wait for explicit approval before proceeding

5. **Create the PR** only after the user confirms:
   ```
   gh pr create --title "<title>" --body "<description>" --base main
   ```

## PR Description Template

```
## Summary
<concise summary of what this PR does>

## Changes
- <key change 1>
- <key change 2>

## Design
<which section of docs/design.md this implements or changes; "none" if purely internal>

## Verification
<how it was checked: unit tests, local compose run, Parquet inspected, API called, etc.>

## Notes
<optional: caveats, follow-up tasks, open questions — omit if not needed>
```

## Rules
- Never create a PR without explicit user approval
- Always target `main` as the base branch
- One logical change per PR
- **PR title must follow Conventional Commits** — it becomes the squash-merged commit on `main`
- If the PR changes `storage/models.py`, it must include the Alembic migration
- If the PR changes a design decision, update `docs/design.md` in the same PR
- Keep pipeline logic (`eonje/`) and DAG wiring (`dags/`) in separate PRs when they can be reviewed independently
