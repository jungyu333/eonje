from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class CallRecord:
    """HTTP 시도 1회의 기록. 성공·실패 모두 일일 한도에 포함되므로 시도마다 하나씩 만든다."""

    operation: str
    page_no: int
    params: Mapping[str, str]  # 실제 요청 쿼리에서 serviceKey만 뺀 것
    called_at: datetime
    elapsed_ms: int
    attempt: int
    http_status: int | None
    result_code: str | None
    total_count: int | None


@dataclass(frozen=True)
class Page:
    operation: str
    page_no: int
    total_count: int
    items: list[dict[str, Any]]
    fetched_at: datetime
