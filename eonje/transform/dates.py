"""dim_date의 행 만들기. 순수 함수: Spark도 I/O도 쓰지 않는다.

날짜·요일·주말은 계산하고, 공휴일은 받은 목록을 그대로 따른다. 주말은 공휴일 목록에 없으므로
is_holiday가 아니다. 쉬는 날은 is_weekend 또는 is_holiday로 판단한다.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass

_SATURDAY = 6  # ISO 요일: 1=월 ... 7=일


@dataclass(frozen=True)
class DateRow:
    """core.dim_date의 한 행. 필드 순서는 테이블 컬럼 순서와 같다."""

    date: dt.date
    year: int
    month: int
    day: int
    day_of_week: int  # 1=월 ... 7=일 (ISO)
    is_weekend: bool
    is_holiday: bool
    holiday_name: str | None


def date_rows(
    start: dt.date, end: dt.date, holidays: Iterable[tuple[dt.date, str]]
) -> list[DateRow]:
    """start부터 end까지(양 끝 포함) 하루에 한 행. holidays는 (날짜, 이름) 목록이다.

    같은 날짜에 공휴일이 둘이면 이름을 받은 순서대로 ", "로 잇는다.
    기간 밖의 공휴일은 쓰지 않는다.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")

    names: dict[dt.date, list[str]] = {}
    for day, name in holidays:
        names.setdefault(day, []).append(name)

    rows: list[DateRow] = []
    for offset in range((end - start).days + 1):
        day = start + dt.timedelta(days=offset)
        weekday = day.isoweekday()
        # 빈 이름은 빼고, 같은 이름이 두 번 오면 한 번만 쓴다 (순서 유지)
        named = list(dict.fromkeys(name for name in names.get(day, []) if name))
        rows.append(
            DateRow(
                date=day,
                year=day.year,
                month=day.month,
                day=day.day,
                day_of_week=weekday,
                is_weekend=weekday >= _SATURDAY,
                is_holiday=day in names,
                holiday_name=", ".join(named) or None,
            )
        )
    return rows
