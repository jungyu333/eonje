# CLAUDE.md

## Project Overview

`eonje` (언제충전) is a Python 3.11 project managed by **uv**.
It collects EV charger status from the Korean public API (한국환경공단 EvCharger), stores raw responses,
derives occupancy history with Spark into Iceberg tables on SeaweedFS (the DW), and serves aggregated patterns from
Postgres through a FastAPI service.
Design: `docs/design.md`. Requirements: `docs/requirements.md`. Read both before changing structure.

## Architecture

```
eonje/        # Pure Python package. Never imports airflow.
  cli/        # Task entrypoint: python -m eonje.cli <job> --run-id <id> --run-after <iso8601>
              #   app.py (parse, dispatch, exit code) · registry.py (name -> job) · jobs/ (one module per DAG task, wiring only)
  config.py   # Settings (pydantic-settings, EONJE_ prefix). The only place env vars are read.
  client/     # API client: request building, pagination, retry, response validation
  budget/     # Daily call budget: usage lookup, run/skip decision, page cap
  collect/    # Collection flows (budget -> paging -> raw run). Combines client/budget/storage; HTTP client,
              #   filesystem and DB session factory are injected, never built here. Returns an outcome, not an exit code.
  storage/    # raw.py (Parquet write, collector), spark.py (SparkSession + Iceberg catalog, s3a read, JDBC staging write),
              #   iceberg.py (Iceberg table definitions), models.py (SQLAlchemy ORM), db.py (engine, staging -> ON CONFLICT)
  transform/  # Pure functions. Spark DataFrame in -> DataFrame out. No I/O.
  codes/      # Versioned code tables (YAML) + loader
api/          # FastAPI. Read-only over aggregate/prediction tables. Uses eonje.storage.models.
dags/         # Thin Airflow DAGs. @task.docker calling eonje.cli only. Loaded via GitDagBundle (deploy ref).
docker/       # One Dockerfile per runtime image: tasks/collector, tasks/spark, tasks/ml, api
infra/        # compose.node-a.yaml, compose.node-b.yaml, postgres/init.sh, migrations/ (Alembic), .env.example
tests/        # unit/ (no Airflow, no network; Spark via local session), fixtures/ (real API responses, keys removed)
docs/         # design.md, requirements.md
```

## Boundary Rules

- **Airflow is known only in `dags/`.** `eonje/` and `api/` never import `airflow`. DAG files import only `airflow` and
  stdlib; they read env vars directly (they cannot import `eonje`).
- **External I/O lives only in `client/` and `storage/`.** `transform/` is pure: no HTTP, no DB, no filesystem.
  SparkSession creation, s3a reads, and JDBC writes belong in `storage/`.
- **API vocabulary (`statId`, `chgerId`, `stat`, ...) is translated once in `transform/observations.py`** into internal
  names (`stat_id`, `charger_id`, `status_code`). Everything downstream uses internal names only.
- **Three stores, one role each:** raw Parquet in SeaweedFS (lake, append-only), Iceberg `stg`/`core` on SeaweedFS
  (DW: observations, facts, SCD2 dimensions), Postgres `mart`/`ops` (serving aggregates, current state, call log,
  quality metrics). `api/` reads Postgres only.
- **One schema source per store.** Postgres: `storage/models.py` (Alembic autogenerates from it; `api/` queries the same
  models). Iceberg: `storage/iceberg.py`, applied by an idempotent job (`CREATE TABLE IF NOT EXISTS`, add missing
  columns) — never Alembic. Alembic ignores the `iceberg` catalog schema.
- **Bulk loads never use `session.add` per row.** Postgres: Spark writes a JDBC staging table, then
  `INSERT ... ON CONFLICT`. Iceberg: Spark `append` or `MERGE INTO`.
- **Raw responses are stored as received** — all columns as strings, unknown fields kept. Type conversion happens in
  `transform/`.
- **`__init__.py` files are empty.** No logic, no re-exports. Import from the module (`from eonje.cli.app import main`).
  `__main__.py` only delegates.
- Settings are read in `config.py` only. Collection interval, daily limit, page size, served regions are all settings,
  never literals.

## Data Rules

- Every task is **idempotent per `run_id`**: rerunning the same run produces the same tables. Raw: a completed run (manifest
  present) is never fetched again, because the API only serves the last N minutes and a refetch would relabel new data
  and delete the original; only an incomplete run is cleared and fetched again. `stg.obs_status` and `core.evt_transition`: ignore on conflict (facts are append-only; corrections are new rows).
  `core.fact_session`: `MERGE`. Aggregates: delete the window, recompute.
- `statUpdDt` is **not** a transition time (operator-dependent heartbeat). Transitions are derived by comparing against
  `charger_state`.
- All timestamps are timezone-aware. Naive datetimes are rejected at the storage boundary; stored values are UTC.
- Unknown status codes are stored, never rejected. Do not hardcode status enums; map at aggregation time via `codes/`.
- Missing observation windows are "unknown", not "available". They never enter an occupancy denominator.
- The daily API limit (`EONJE_API_DAILY_LIMIT`) must never be exceeded. Every call is logged to `api_call_log`;
  `budget/` decides before any fetch.

## Spark Rules

- Collection is plain Python; every transform/aggregate after it runs on Spark (single engine, standalone, client mode).
- Read/write only via `s3a://` (SeaweedFS S3), the Iceberg catalog, and JDBC — never local paths (executors on other
  nodes cannot see them).
- **Iceberg tables only by catalog name** (`lake.stg.obs_status`), never by writing Parquet to their path. Partition by a
  KST date column (`observed_date`), not `days()` (UTC). Tables are compacted and snapshots expired daily.
- No `collect()` / `toPandas()` on large data. Set `spark.sql.session.timeZone=Asia/Seoul`.
- jars and packages are baked into the `eonje-spark` image; never fetched at runtime.
- Transforms are watermark-based: process every unprocessed raw run, not just the current one.

## Runtime & Deployment

- Images per runtime under `docker/`; image name = folder name (`eonje-collector`, `eonje-spark`, `eonje-ml`,
  `eonje-api`).
  Same entrypoint everywhere: `python -m eonje.cli <job> --run-id <id> --run-after <iso8601>`. Airflow's image never contains eonje or Spark.
- Images are pushed to GHCR tagged with the git sha. **No code on servers**: a node holds only `compose.yaml`, `.env`,
  and volumes. DAGs reach Airflow via GitDagBundle tracking the `deploy` ref, which CD moves after images are pushed.
- Node A (always on): Airflow, Postgres, SeaweedFS (S3), collector, API, cloudflared, Spark master (+worker until B).
  Node B (added on measured need): Spark worker, monitoring, local backup, model training. Stateful services stay on A.
- No inbound ports: public traffic via Cloudflare Tunnel, admin access via Tailscale.
- Details: `docs/design.md` sections 2.3, 2.5, 8.

## Error Handling Convention

- `client/`: retry transient HTTP errors with backoff (tenacity); raise after exhaustion. Never swallow a failed page
  silently — log it to `api_call_log` with the result code.
- `transform/`: raise on contract violations (missing required column); never catch broadly.
- `storage/`: raise on write failure. Writes are transactional per task.
- Unknown fields/codes in API responses are **not** errors: keep and report.

## Common Commands

All commands via **Makefile** (`make help` lists targets):

```
make install      # uv sync --all-extras
make hooks        # pre-commit install (once per clone)
make check        # lint + typecheck + test — same as CI
make test ARGS="-k cli"
make test-debug   # -x -vv -l -s, stop at first failure with locals
make build        # build runtime images under docker/ (eonje-collector:dev)
make run JOB=fetch-delta RUN_ID=manual__x RUN_AFTER=2026-10-28T01:10:00+00:00
```

## Output Language
- Write reports, questions, review findings, and summaries addressed to the user in **Korean**.
- Keep code, identifiers, code comments, test names, commit messages, and PR titles/bodies in English.
- When editing an existing file, keep the file's own language (`docs/` is written in Korean).

## Git & PR Conventions

- Managed via custom commands: `/commit`, `/create-pr`, `/review`
- Branch from `main`; never commit to `main` directly
- Squash merge PRs (`gh pr merge --squash`); PR title follows Conventional Commits
- Never commit `.env`, service keys, or fixtures containing a key
