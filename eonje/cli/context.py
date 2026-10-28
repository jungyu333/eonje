"""job 하나가 받는 run 정보. 시각은 실행 시점(now)이 아니라 Airflow run의 시각을 쓴다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from eonje.config import Settings
from eonje.timeutil import require_aware


@dataclass(frozen=True)
class JobContext:
    run_id: str  # Airflow run_id 원문 (api_call_log 기록, run_key의 원천). 공백도 그대로 보존
    run_after: datetime  # Airflow dag_run.run_after. collected_at·dt 파티션의 기준
    settings: Settings

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty or blank")
        require_aware("run_after", self.run_after)
