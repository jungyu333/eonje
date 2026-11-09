from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import httpx
from fsspec import AbstractFileSystem
from sqlalchemy.orm import Session, sessionmaker
from tenacity.wait import wait_base

from eonje.collect.run import CollectResult, RunSpec, collect_run, utcnow
from eonje.config import Settings
from eonje.storage.raw.paths import Dataset

OPERATION = "getChargerInfo"


def master_spec(settings: Settings) -> RunSpec:
    return RunSpec(
        operation=OPERATION,
        dataset=Dataset.CHARGER_INFO,
        params={},
        max_lag=settings.master_max_lag,
    )


def collect_master(
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
        master_spec(settings),
        job=job,
        run_id=run_id,
        run_after=run_after,
        http=http,
        fs=fs,
        factory=factory,
        wait=wait,
        now=now,
    )
