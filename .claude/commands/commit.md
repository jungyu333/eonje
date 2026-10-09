---
description: Stage and commit all current changes with a conventional commit message
context: fork
agent: committer
background: false
---
# /commit

Stage and commit all current changes with a conventional commit message.

## Steps

1. **Check for changes** — run `git status` and `git diff` to identify all changes
   - If there are no changes (no untracked files, no modifications), stop and report to the user

2. **Analyze changes** — review the diff to understand the nature of the changes

3. **Check recent style** — run `git log --oneline -5` to match the repository's commit message style

4. **Stage files** — add all relevant changed and untracked files
   - Never stage `.env`, anything under `data/` or `logs/`, or any file that contains an actual service key value (the literal `serviceKey` parameter name in docs or rules is fine)

5. **Commit** — create a commit with a conventional commit message
   - Format: `<type>(<scope>): <short description>`
   - `type`: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`, `ci`, `perf`, `style`
   - `scope`: see table below
   - Breaking changes (API response shape, DB schema requiring data migration): add `!` after type/scope
   - Keep the description concise (under 72 characters)
   - If the pre-commit hook fails, re-stage the auto-fixed files and create a new commit (do not amend)

## Commit Message Examples

```
feat(client): add paginated fetch for getChargerStatus
feat(storage): add raw parquet writer with run-id partitioning
feat(transform): derive transitions from charger_state
feat(dags): add ev_status_10min with budget check
fix(client): treat resultCode 03 (no data) as empty page, not error
fix(transform): dedupe observations on (stat_id, charger_id, stat_upd_dt)
test(transform): add session reconstruction cases from fixtures
docs: add collection strategy to design.md
chore(infra): add postgres init.sql for airflow/eonje databases
ci: add ruff, mypy, pytest, docker build workflow
build: pin pandas and pyarrow versions
```

## Scope Guidelines

| Scope | When to use |
| --- | --- |
| `client` | `eonje/client/**` |
| `budget` | `eonje/budget/**` |
| `storage` | `eonje/storage/**` (raw, models, db) |
| `transform` | `eonje/transform/**` |
| `codes` | `eonje/codes/**` |
| `cli` | `eonje/cli.py` |
| `api` | `api/**` |
| `dags` | `dags/**` |
| `infra` | `infra/**`, `Dockerfile` |
| `web` | `web/**` |
| (omitted) | Project-wide: root config, `.github/`, `docs/` |

## Splitting Commits

If a change touches both pipeline logic and its DAG, or logic and infra, split into separate commits:

```bash
# ❌ Avoid
git commit -m "feat: add delta fetch"

# ✅ Prefer
git commit -m "feat(client): add period-based delta fetch"
git commit -m "feat(dags): wire fetch_delta task into ev_status_10min"
```

Schema changes (`storage/models.py`) and their Alembic migration go in the **same** commit.

## Rules
- Never commit to `main` directly
- Never stage files containing secrets or credentials
- Always use a conventional commit message
- Match the scope to the module being modified
- If pre-commit hook modifies files, re-stage and create a new commit (do not amend)
