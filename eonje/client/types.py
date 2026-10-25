from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class CallRecord:
    operation: str
    page_no: int
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
