from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
from fsspec import AbstractFileSystem
from sqlalchemy.orm import Session, sessionmaker
from tenacity.wait import wait_base

from eonje.budget.policy import decide, quota_day_bounds
from eonje.cli import exit_codes
from eonje.cli.context import JobContext
from eonje.client.evcharger import EvChargerClient
from eonje.storage.call_log import CallLogWriter, count_calls
from eonje.storage.db import make_engine, make_session_factory, session_scope
from eonje.storage.fs import raw_filesystem
from eonje.storage.raw_paths import Dataset, RawRun, partition_date, run_key_from_run_id
from eonje.storage.raw_run import RawRunWriter

JOB = "fetch-delta"
OPERATION = "getChargerStatus"

log = logging.getLogger(__name__)


def run(ctx: JobContext) -> int:

    engine = make_engine(ctx.settings)

    try:
        with httpx.Client() as http:
            return fetch_delta(
                ctx,
                http=http,
                fs=raw_filesystem(ctx.settings),
                factory=make_session_factory(engine),
            )
    finally:
        engine.dispose()


def _utcnow() -> datetime:
    return datetime.now(UTC)


def fetch_delta(
    ctx: JobContext,
    *,
    http: httpx.Client,
    fs: AbstractFileSystem,
    factory: sessionmaker[Session],
    wait: wait_base | None = None,
    now: Callable[[], datetime] = _utcnow,
) -> int:
    s = ctx.settings

    start, end = quota_day_bounds(now())
    with session_scope(factory) as session:
        used = count_calls(session, start, end)
    decision = decide(
        daily_limit=s.api_daily_limit,
        safety_ratio=s.budget_safety_ratio,
        reserve_calls=s.budget_reserve_calls,
        used_today=used,
        max_attempts=s.api_max_attempts,
    )

    log.info("budget: %s", decision.reason)

    if not decision.run:
        return exit_codes.SKIPPED

    client = EvChargerClient(
        http, s, on_call=CallLogWriter(factory, job=JOB, run_id=ctx.run_id), wait=wait
    )

    raw_run = RawRun(
        dataset=Dataset.CHARGER_STATUS,
        dt=partition_date(ctx.run_after),
        run_key=run_key_from_run_id(ctx.run_id),
    )
    writer = RawRunWriter(fs, s.raw_bucket, raw_run, ctx.run_after)

    writer.start()

    params = {"period": str(s.status_period_min)}
    for page in client.iter_pages(OPERATION, params, max_pages=decision.max_pages):
        writer.write(page)

    manifest = writer.finish()

    log.info(
        "fetched %d page(s), %d row(s) of totalCount %d into %s/dt=%s/run=%s",
        len(manifest.pages),
        manifest.row_count,
        manifest.total_count,
        raw_run.dataset,
        manifest.dt,
        manifest.run_key,
    )

    if manifest.row_count < manifest.total_count:
        log.warning(
            "run truncated by budget cap: %d of %d rows", manifest.row_count, manifest.total_count
        )
    return exit_codes.OK
