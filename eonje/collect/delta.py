"""상태 변경분 수집 흐름: 예산 판단 → 페이지 순회 → 원본 run 쓰기.

HTTP 클라이언트, 원본 파일시스템, DB 세션 팩토리는 인자로 받는다. 만드는 일과 결과를 종료 코드로
바꾸는 일은 cli job(`eonje.cli.jobs.fetch_delta`)이 한다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

import httpx
from fsspec import AbstractFileSystem
from sqlalchemy.orm import Session, sessionmaker
from tenacity.wait import wait_base

from eonje.budget.policy import BudgetDecision, decide, quota_day_bounds
from eonje.client.evcharger import EvChargerClient
from eonje.config import Settings
from eonje.storage.call_log import CallLogWriter, count_calls
from eonje.storage.db import session_scope
from eonje.storage.raw_paths import Dataset, RawRun, partition_date, run_key_from_run_id
from eonje.storage.raw_run import RawRunWriter, RunManifest
from eonje.timeutil import require_aware

OPERATION = "getChargerStatus"

log = logging.getLogger(__name__)


class Outcome(StrEnum):
    COLLECTED = "collected"
    SKIPPED_BUDGET = "skipped_budget"  # 오늘 예산으로는 1페이지도 못 받는다. 호출 없음


@dataclass(frozen=True)
class DeltaResult:
    outcome: Outcome
    budget: BudgetDecision | None
    manifest: RunManifest | None  # COLLECTED일 때만


def _utcnow() -> datetime:
    return datetime.now(UTC)


def collect_delta(
    settings: Settings,
    *,
    job: str,
    run_id: str,
    run_after: datetime,
    http: httpx.Client,
    fs: AbstractFileSystem,
    factory: sessionmaker[Session],
    wait: wait_base | None = None,
    now: Callable[[], datetime] = _utcnow,
) -> DeltaResult:
    """run_after가 수집 시각과 날짜 파티션을, now()가 예산을 세는 한도 하루를 정한다.

    job과 run_id는 api_call_log에 남는 꼬리표다.
    """
    require_aware("run_after", run_after)
    s = settings

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
        return DeltaResult(Outcome.SKIPPED_BUDGET, decision, None)

    client = EvChargerClient(
        http, s, on_call=CallLogWriter(factory, job=job, run_id=run_id), wait=wait
    )
    raw_run = RawRun(
        dataset=Dataset.CHARGER_STATUS,
        dt=partition_date(run_after),
        run_key=run_key_from_run_id(run_id),
    )
    writer = RawRunWriter(fs, s.raw_bucket, raw_run, run_after, page_size=s.page_size)

    writer.start()
    params = {"period": str(s.status_period_min)}
    for page in client.iter_pages(OPERATION, params, max_pages=decision.max_pages):
        writer.write(page)
    manifest = writer.finish()

    _log_manifest(raw_run, manifest)
    return DeltaResult(Outcome.COLLECTED, decision, manifest)


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
        # 페이지 사이에 최근 N분 창이 움직였다. 양수면 누락, 음수면 중복 가능성
        log.warning(
            "totalCount drifted during paging: %d rows written, page 1 reported %d (drift %+d)",
            manifest.row_count,
            manifest.total_count,
            manifest.drift,
        )
