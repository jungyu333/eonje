from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

import httpx
from fsspec import AbstractFileSystem
from sqlalchemy.orm import Session, sessionmaker
from tenacity.wait import wait_base

from eonje.budget.policy import BudgetDecision, decide, quota_day_bounds
from eonje.client.evcharger import EvChargerClient
from eonje.config import Settings
from eonje.storage.postgres.call_log import CallLogWriter, count_calls
from eonje.storage.postgres.db import session_scope
from eonje.storage.raw.paths import Dataset, RawRun, partition_date, run_key_from_run_id
from eonje.storage.raw.run import RawRunWriter, RunManifest, read_manifest
from eonje.timeutil import require_aware

log = logging.getLogger(__name__)


class Outcome(StrEnum):
    COLLECTED = "collected"
    ALREADY_COMPLETE = "already_complete"
    SKIPPED_STALE = "skipped_stale"
    SKIPPED_BUDGET = "skipped_budget"


@dataclass(frozen=True)
class RunSpec:
    """데이터셋마다 다른 것: 부를 API, 저장할 데이터셋, 요청 파라미터, 지연 허용치."""

    operation: str
    dataset: Dataset
    params: Mapping[str, str]
    max_lag: timedelta


@dataclass(frozen=True)
class CollectResult:
    outcome: Outcome
    budget: BudgetDecision | None
    manifest: RunManifest | None


def utcnow() -> datetime:
    return datetime.now(UTC)


def collect_run(
    settings: Settings,
    spec: RunSpec,
    *,
    job: str,
    run_id: str,
    run_after: datetime,
    http: httpx.Client,
    fs: AbstractFileSystem,
    factory: sessionmaker[Session],
    wait: wait_base | None = None,
    now: Callable[[], datetime] = utcnow,
) -> CollectResult:
    """run_after가 수집 시각과 날짜 파티션을, now()가 예산을 세는 한도 하루를 정한다.

    job과 run_id는 api_call_log에 남는 꼬리표다.
    """
    require_aware("run_after", run_after)
    s = settings

    raw_run = RawRun(
        dataset=spec.dataset,
        dt=partition_date(run_after),
        run_key=run_key_from_run_id(run_id),
    )
    existing = read_manifest(fs, s.raw_bucket, raw_run)
    if existing is not None:
        log.info(
            "run already complete, not fetching again: %s/dt=%s/run=%s",
            raw_run.dataset,
            existing.dt,
            existing.run_key,
        )
        return CollectResult(Outcome.ALREADY_COMPLETE, None, existing)

    current = now()
    lag = current - run_after
    if abs(lag) > spec.max_lag:
        log.warning(
            "run_after %s is %s from now (%s), beyond the max lag %s; "
            "the API only returns the current state, skipping",
            run_after.isoformat(),
            lag,
            current.isoformat(),
            spec.max_lag,
        )
        return CollectResult(Outcome.SKIPPED_STALE, None, None)

    start, end = quota_day_bounds(current)
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
        return CollectResult(Outcome.SKIPPED_BUDGET, decision, None)

    client = EvChargerClient(
        http, s, on_call=CallLogWriter(factory, job=job, run_id=run_id), wait=wait
    )
    writer = RawRunWriter(fs, s.raw_bucket, raw_run, run_after, page_size=s.page_size)

    writer.start()
    for page in client.iter_pages(spec.operation, spec.params, max_pages=decision.max_pages):
        writer.write(page)
    manifest = writer.finish()

    _log_manifest(raw_run, manifest)
    return CollectResult(Outcome.COLLECTED, decision, manifest)


def _log_manifest(raw_run: RawRun, manifest: RunManifest) -> None:
    log.info(
        "fetched %d page(s), %d row(s) of totalCount %d into %s/dt=%s/run=%s",
        len(manifest.pages),
        manifest.row_count,
        manifest.total_count,
        raw_run.dataset,
        manifest.dt,
        manifest.run_key,
    )
    if manifest.truncated:
        log.warning(
            "run truncated by budget cap: %d of %d pages",
            len(manifest.pages),
            manifest.expected_pages,
        )
    elif manifest.drift:
        # 페이지 사이에 데이터가 바뀌었다. 양수면 누락, 음수면 중복 가능성
        log.warning(
            "totalCount drifted during paging: %d rows written, page 1 reported %d (drift %+d)",
            manifest.row_count,
            manifest.total_count,
            manifest.drift,
        )
