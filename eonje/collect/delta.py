"""상태 변경분(getChargerStatus) 수집: 공통 흐름(collect/run.py)에 delta 설정을 넘긴다.

API 창은 '지금부터 최근 status_period_min분'이고, 지연 허용치는 settings.fetch_max_lag다.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import httpx
from fsspec import AbstractFileSystem
from sqlalchemy.orm import Session, sessionmaker
from tenacity.wait import wait_base

from eonje.collect.run import CollectResult, Outcome, RunSpec, collect_run, utcnow
from eonje.config import Settings
from eonje.storage.raw.paths import Dataset

__all__ = ["OPERATION", "DeltaResult", "Outcome", "collect_delta", "delta_spec"]

OPERATION = "getChargerStatus"

DeltaResult = CollectResult  # 기존 이름 유지 (fetch_delta 잡·테스트가 쓴다)


def delta_spec(settings: Settings) -> RunSpec:
    return RunSpec(
        operation=OPERATION,
        dataset=Dataset.CHARGER_STATUS,
        params={"period": str(settings.status_period_min)},
        max_lag=settings.fetch_max_lag,
    )


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
    now: Callable[[], datetime] = utcnow,
) -> CollectResult:
    return collect_run(
        settings,
        delta_spec(settings),
        job=job,
        run_id=run_id,
        run_after=run_after,
        http=http,
        fs=fs,
        factory=factory,
        wait=wait,
        now=now,
    )
