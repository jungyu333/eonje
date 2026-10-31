---
description: Review the current branch against main for boundary and data-rule violations
context: fork
agent: reviewer
background: false
---
# /review

Review the current branch against `main` following the project's boundary and data rules.

## Steps
1. Run `git diff main` to get the diff
2. Review the diff against the checklist below
3. Report findings grouped by severity: **Critical**, **Warning**, **Suggestion**

## Checklist

### Boundaries
- [ ] No `import airflow` outside `dags/`
- [ ] `dags/` does not import `eonje`; DAG files only wire `@task.docker` calls to `eonje.cli`
- [ ] No HTTP, DB, or filesystem access in `transform/`
- [ ] No business logic in `api/` — it only queries aggregate tables and shapes responses
- [ ] API field names (`statId`, `chgerId`, `stat`, ...) appear only in `client/` and `transform/observations.py`
- [ ] Settings are read via `config.py`, not `os.environ` scattered in code (DAG files excepted)
- [ ] `__init__.py` files are empty; `cli/jobs/*` contain wiring only, no business logic

### Spark
- [ ] Spark I/O only via `s3a://`, the Iceberg catalog, and JDBC (no local paths); SparkSession and I/O live in `storage/`
- [ ] Iceberg tables are accessed by catalog name only, never by writing Parquet to their path
- [ ] Keyed Iceberg tables are written with `MERGE INTO` (never plain `append`), and the `ON` condition includes the
      partition column range taken from the source
- [ ] Cross-store writes commit Iceberg first, then Postgres (state + the task's own watermark in one transaction)
- [ ] No `collect()` / `toPandas()` on large DataFrames
- [ ] `spark.sql.session.timeZone` is set explicitly
- [ ] Transforms process all unprocessed raw runs in `collected_at` order (watermark), not only the current run; each
      task keeps its own watermark and never passes runs that could still complete late

### Data Correctness
- [ ] Raw writes keep all response fields as strings; no filtering or type conversion before storage
- [ ] Every task is idempotent for its `run_id` (overwrite / on-conflict / delete-window-then-recompute)
- [ ] `statUpdDt` is not used as a transition timestamp
- [ ] Unknown status codes are stored, not dropped or raised
- [ ] Missing observation windows are excluded from occupancy denominators
- [ ] `stg.obs_status` keeps one row per `(stat_id, charger_id, observed_at)`: duplicates inside one run are reduced
      deterministically (latest `stat_upd_dt`, then tie-breakers in design.md 6); the same report seen in another run is
      a separate observation

### Budget & Collection
- [ ] Every API call is recorded in `api_call_log`
- [ ] A fetch path checks `budget/` before calling the API
- [ ] Page loops are bounded (`totalCount`-derived) and retry per page, not per run

### Schema
- [ ] Changes to `storage/models.py` have a matching Alembic migration
- [ ] Postgres bulk loads use `insert(...).on_conflict_do_*` via a staging table, not per-row `session.add`
- [ ] Changes to Iceberg tables go through `storage/iceberg.py` (additive only), not ad-hoc DDL

### Testing
- [ ] New `transform/` functions have unit tests using `tests/fixtures/`
- [ ] Both happy path and edge cases (empty page, unknown code, duplicate observation, missing `lastTedt`) are covered
- [ ] Tests do not require network or a running DB (DB tests are marked and isolated)

### General
- [ ] All function arguments and return values have type hints
- [ ] No ruff rule violations
- [ ] No service key, `.env`, or credential in the diff or fixtures
- [ ] `docs/design.md` updated if a design decision changed
