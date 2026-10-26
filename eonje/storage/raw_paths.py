"""원본 Parquet 경로 규칙. 스킴 없는 '버킷/키' 형태를 돌려준다 (s3fs용).
Spark는 앞에 s3a:// 를 붙여 같은 경로를 쓴다.

{bucket}/{dataset}/dt=YYYY-MM-DD/run={run_key}/page=NNN.parquet
{bucket}/{dataset}/dt=YYYY-MM-DD/run={run_key}/_manifest.json

dt는 수집 시각(collected_at)의 KST 날짜다. 같은 run이 두 날짜 파티션에 생기지 않도록
모든 호출자가 partition_date()로 정한다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum

# 한국은 서머타임이 없어 고정 오프셋으로 충분하다 (Windows에서 tzdata 없이 동작)
PARTITION_TZ = timezone(timedelta(hours=9), "KST")
MAX_PAGE_NO = 999
_RUN_KEY = re.compile(r"^[0-9A-Za-z][0-9A-Za-z_.-]*$")


class Dataset(StrEnum):
    CHARGER_STATUS = "charger_status"
    CHARGER_INFO = "charger_info"


def partition_date(collected_at: datetime) -> date:
    """수집 시각 → dt 파티션 날짜 (KST). 시간대 없는 시각은 거부한다."""
    if collected_at.tzinfo is None or collected_at.utcoffset() is None:
        raise ValueError(f"collected_at must be timezone-aware: {collected_at!r}")
    return collected_at.astimezone(PARTITION_TZ).date()


@dataclass(frozen=True)
class RawRun:
    """수집 1회의 저장 위치. dt는 partition_date(collected_at)이어야 한다."""

    dataset: Dataset
    dt: date
    run_key: str

    def __post_init__(self) -> None:
        if not _RUN_KEY.fullmatch(self.run_key):
            raise ValueError(f"run_key must be path-safe [0-9A-Za-z_.-]: {self.run_key!r}")


def run_prefix(bucket: str, run: RawRun) -> str:
    return f"{bucket}/{run.dataset}/dt={run.dt:%Y-%m-%d}/run={run.run_key}"


def page_path(bucket: str, run: RawRun, page_no: int) -> str:
    if not 1 <= page_no <= MAX_PAGE_NO:
        raise ValueError(f"page_no out of range 1..{MAX_PAGE_NO}: {page_no}")
    return f"{run_prefix(bucket, run)}/page={page_no:03d}.parquet"


def manifest_path(bucket: str, run: RawRun) -> str:
    return f"{run_prefix(bucket, run)}/_manifest.json"
