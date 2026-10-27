from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from eonje.timeutil import KST, require_aware


@dataclass(frozen=True)
class BudgetDecision:
    run: bool
    max_pages: int  # run이 False면 0
    usable_calls: int  # 이번 판단 시점에 더 쓸 수 있는 호출 수
    reason: str


def quota_day_bounds(now: datetime) -> tuple[datetime, datetime]:
    """now가 속한 한도 하루(KST 자정 ~ 다음 자정)의 [start, end)."""
    require_aware("now", now)
    day = now.astimezone(KST).date()
    start = datetime(day.year, day.month, day.day, tzinfo=KST)
    return start, start + timedelta(days=1)


def decide(
    *,
    daily_limit: int,
    safety_ratio: float,
    reserve_calls: int,
    used_today: int,
    max_attempts: int,
) -> BudgetDecision:
    """최악의 경우(페이지마다 max_attempts번 시도)에도 한도 안에 들도록 페이지 상한을 정한다."""
    if used_today < 0 or max_attempts < 1:
        raise ValueError("used_today must be >= 0 and max_attempts >= 1")

    ceiling = math.floor(daily_limit * safety_ratio) - reserve_calls
    usable = max(0, ceiling - used_today)
    pages = usable // max_attempts

    if pages < 1:
        return BudgetDecision(False, 0, usable, f"budget exhausted: used {used_today}/{ceiling}")
    return BudgetDecision(True, pages, usable, f"ok: {usable} calls left, cap {pages} pages")
