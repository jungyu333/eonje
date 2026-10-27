from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

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
    if daily_limit < 1:
        raise ValueError(f"daily_limit must be >= 1: {daily_limit}")
    if not 0 < safety_ratio <= 1:
        raise ValueError(f"safety_ratio must be in (0, 1]: {safety_ratio}")
    if reserve_calls < 0:
        raise ValueError(f"reserve_calls must be >= 0: {reserve_calls}")
    if used_today < 0:
        raise ValueError(f"used_today must be >= 0: {used_today}")
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1: {max_attempts}")

    # Decimal(str(...)): 0.95를 적힌 그대로 계산한다 (float 곱셈은 가끔 1 작게 나온다)
    ceiling = int(Decimal(str(safety_ratio)) * daily_limit) - reserve_calls
    usable = max(0, ceiling - used_today)
    pages = usable // max_attempts

    if pages < 1:
        return BudgetDecision(False, 0, usable, f"budget exhausted: used {used_today}/{ceiling}")
    return BudgetDecision(True, pages, usable, f"ok: {usable} calls left, cap {pages} pages")
