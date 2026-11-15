"""dim_date 채우기 흐름. 첫 해부터 공휴일이 발표된 마지막 해까지의 날짜 행을 만들고,
지금 테이블과 다를 때만 전체를 덮어쓴다.

공휴일 조회(fetch)와 테이블 읽기·쓰기(read, overwrite)는 밖에서 받는다. 그래서 이 흐름은
API·Spark 없이 테스트한다. 실제 연결은 load_dim_date 잡이 한다.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from eonje.transform.dates import DateRow, date_rows

log = logging.getLogger(__name__)

Holidays = Sequence[tuple[dt.date, str]]  # (날짜, 이름)


@dataclass(frozen=True)
class DimDateResult:
    """rows는 만든 행 수, changed는 지금 테이블과 다른 날짜다 (추가·삭제·내용 변경).

    changed가 비어 있으면 테이블에 쓰지 않았다.
    """

    rows: int
    changed: tuple[dt.date, ...]


def load_dim_date(
    *,
    first_year: int,
    today: dt.date,
    fetch: Callable[[int], Holidays],
    read: Callable[[], Sequence[DateRow]],
    overwrite: Callable[[Sequence[DateRow]], None],
) -> DimDateResult:
    """first_year부터 내년까지 채운다. 내년은 공휴일이 아직 발표되지 않았으면 넣지 않는다.

    올해까지의 해에 공휴일이 0건이면 응답이 잘못된 것으로 보고 실패한다 (테이블은 그대로 둔다).
    """
    this_year, next_year = today.year, today.year + 1
    if first_year > this_year:
        raise ValueError(f"first_year {first_year} is after this year {this_year}")

    holidays: list[tuple[dt.date, str]] = []
    for year in range(first_year, this_year + 1):
        found = fetch(year)
        if not found:
            raise ValueError(f"no holidays returned for {year}; dim_date is left unchanged")
        holidays.extend(found)

    upcoming = fetch(next_year)  # 아직 발표되지 않았으면 빈 목록
    holidays.extend(upcoming)
    last_year = next_year if upcoming else this_year

    rows = date_rows(dt.date(first_year, 1, 1), dt.date(last_year, 12, 31), holidays)
    before = {row.date: row for row in read()}
    after = {row.date: row for row in rows}
    days = before.keys() | after.keys()
    changed = tuple(sorted(day for day in days if before.get(day) != after.get(day)))

    if not changed:
        log.info("dim_date unchanged: %d rows (%s..%s)", len(rows), rows[0].date, rows[-1].date)
        return DimDateResult(len(rows), ())

    overwrite(rows)
    log.info(
        "dim_date updated: %d rows (%s..%s), %d changed: %s",
        len(rows),
        rows[0].date,
        rows[-1].date,
        len(changed),
        _preview(changed),
    )
    return DimDateResult(len(rows), changed)


def _preview(days: Sequence[dt.date], limit: int = 10) -> str:
    """로그에 찍을 날짜 목록. 많으면 앞의 몇 개만 보여 준다."""
    shown = ", ".join(day.isoformat() for day in days[:limit])
    return shown if len(days) <= limit else f"{shown}, ... (+{len(days) - limit})"
