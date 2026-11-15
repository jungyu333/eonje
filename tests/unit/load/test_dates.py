from collections.abc import Sequence
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from eonje.client.holidays import parse_holidays
from eonje.load.dates import load_dim_date
from eonje.transform.dates import DateRow, date_rows

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "holidays"

TODAY = date(2026, 11, 15)
FIRST_YEAR = 2026

# 2026 holidays come from the real API response; 2027 has no fixture, so a short hand-made list.
HOLIDAYS_2026 = [
    (h.day, h.name)
    for h in parse_holidays((FIXTURES / "rest_de_info_2026.json").read_text(encoding="utf-8"))
]
HOLIDAYS_2027 = [(date(2027, 1, 1), "1월1일"), (date(2027, 3, 1), "삼일절")]


class FakeFetch:
    """Returns the holidays registered per year and records every requested year."""

    def __init__(self, by_year: dict[int, list[tuple[date, str]]]) -> None:
        self.by_year = by_year
        self.calls: list[int] = []

    def __call__(self, year: int) -> list[tuple[date, str]]:
        self.calls.append(year)
        return list(self.by_year.get(year, []))


class FakeOverwrite:
    """Records every row list it is called with."""

    def __init__(self) -> None:
        self.calls: list[list[DateRow]] = []

    def __call__(self, rows: Sequence[DateRow]) -> None:
        self.calls.append(list(rows))


def _reader(rows: Sequence[DateRow]):
    return lambda: list(rows)


def _expected_rows(holidays: list[tuple[date, str]], last_year: int) -> list[DateRow]:
    return date_rows(date(FIRST_YEAR, 1, 1), date(last_year, 12, 31), holidays)


def _all_days(start: date, end: date) -> tuple[date, ...]:
    return tuple(row.date for row in date_rows(start, end, []))


# --- empty table ----------------------------------------------------------------


def test_empty_table_is_overwritten_once_from_first_jan_1_to_last_dec_31() -> None:
    # GIVEN an empty table and holidays published for this year and next year
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader([]), overwrite=overwrite
    )

    # THEN overwrite is called once with rows from 2026-01-01 through 2027-12-31
    assert len(overwrite.calls) == 1
    written = overwrite.calls[0]
    assert written[0].date == date(2026, 1, 1)
    assert written[-1].date == date(2027, 12, 31)


def test_empty_table_reports_every_date_as_changed() -> None:
    # GIVEN an empty table and holidays published for this year and next year
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})

    # WHEN dim_date is loaded
    result = load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader([]), overwrite=FakeOverwrite()
    )

    # THEN every built date is in changed, in date order
    assert result.changed == _all_days(date(2026, 1, 1), date(2027, 12, 31))


# --- last year ------------------------------------------------------------------


def test_rows_extend_to_next_dec_31_when_next_year_holidays_are_published() -> None:
    # GIVEN holidays for next year (2027) are already published
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader([]), overwrite=overwrite
    )

    # THEN the last written row is 2027-12-31
    assert overwrite.calls[0][-1].date == date(2027, 12, 31)


def test_rows_stop_at_this_dec_31_when_next_year_holidays_are_not_published() -> None:
    # GIVEN next year (2027) returns no holidays yet
    fetch = FakeFetch({2026: HOLIDAYS_2026})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    result = load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader([]), overwrite=overwrite
    )

    # THEN the rows end at 2026-12-31
    assert overwrite.calls[0][-1].date == date(2026, 12, 31)
    assert result.rows == 365


# --- invalid responses / arguments ----------------------------------------------


def test_zero_holidays_for_this_year_raises_and_does_not_overwrite() -> None:
    # GIVEN this year (2026) returns no holidays
    fetch = FakeFetch({2027: HOLIDAYS_2027})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    # THEN ValueError is raised and the table is not written
    with pytest.raises(ValueError):
        load_dim_date(
            first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader([]), overwrite=overwrite
        )
    assert overwrite.calls == []


def test_zero_holidays_for_an_earlier_year_raises_and_does_not_overwrite() -> None:
    # GIVEN first_year 2025 returns no holidays while 2026 does
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded from 2025
    # THEN ValueError is raised and the table is not written
    with pytest.raises(ValueError):
        load_dim_date(
            first_year=2025, today=TODAY, fetch=fetch, read=_reader([]), overwrite=overwrite
        )
    assert overwrite.calls == []


def test_first_year_after_this_year_raises_value_error() -> None:
    # GIVEN first_year 2027 while today is in 2026
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    # THEN ValueError is raised and the table is not written
    with pytest.raises(ValueError):
        load_dim_date(
            first_year=2027, today=TODAY, fetch=fetch, read=_reader([]), overwrite=overwrite
        )
    assert overwrite.calls == []


def test_fetch_error_propagates_and_does_not_overwrite() -> None:
    # GIVEN a fetch that raises
    class FetchFailed(Exception):
        pass

    def fetch(year: int) -> list[tuple[date, str]]:
        raise FetchFailed(f"holiday API down for {year}")

    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    # THEN the same exception reaches the caller and the table is not written
    with pytest.raises(FetchFailed):
        load_dim_date(
            first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader([]), overwrite=overwrite
        )
    assert overwrite.calls == []


# --- fetch order ----------------------------------------------------------------


def test_fetch_is_called_once_per_year_in_order_from_first_year_to_next_year() -> None:
    # GIVEN holidays for 2024 through 2027
    fetch = FakeFetch(
        {
            2024: [(date(2024, 1, 1), "1월1일")],
            2025: [(date(2025, 1, 1), "1월1일")],
            2026: HOLIDAYS_2026,
            2027: HOLIDAYS_2027,
        }
    )

    # WHEN dim_date is loaded from 2024
    load_dim_date(
        first_year=2024, today=TODAY, fetch=fetch, read=_reader([]), overwrite=FakeOverwrite()
    )

    # THEN each year is fetched exactly once, in ascending order
    assert fetch.calls == [2024, 2025, 2026, 2027]


# --- comparison with the current table ------------------------------------------


def test_table_equal_to_built_rows_is_not_overwritten_and_nothing_changed() -> None:
    # GIVEN a table that already holds exactly the rows that will be built
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})
    current = _expected_rows(HOLIDAYS_2026 + HOLIDAYS_2027, 2027)
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    result = load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader(current), overwrite=overwrite
    )

    # THEN the table is not written and changed is empty
    assert overwrite.calls == []
    assert result.changed == ()


def test_added_holiday_overwrites_and_reports_only_that_date() -> None:
    # GIVEN a table built before a substitute holiday on 2027-03-02 was announced
    current = _expected_rows(HOLIDAYS_2026 + HOLIDAYS_2027, 2027)
    added = (date(2027, 3, 2), "대체공휴일")
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: [*HOLIDAYS_2027, added]})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    result = load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader(current), overwrite=overwrite
    )

    # THEN the table is overwritten and only the added date is changed
    assert len(overwrite.calls) == 1
    assert result.changed == (date(2027, 3, 2),)


def test_renamed_holiday_reports_that_date_as_changed() -> None:
    # GIVEN a table where 2027-03-01 is a holiday under a different name
    current = [
        replace(row, holiday_name="3·1절") if row.date == date(2027, 3, 1) else row
        for row in _expected_rows(HOLIDAYS_2026 + HOLIDAYS_2027, 2027)
    ]
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded
    result = load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader(current), overwrite=overwrite
    )

    # THEN only the renamed date is changed and the table is overwritten
    assert result.changed == (date(2027, 3, 1),)
    assert len(overwrite.calls) == 1


def test_date_only_in_the_table_is_reported_as_changed() -> None:
    # GIVEN a table that runs to 2027-12-31, but 2027 holidays are no longer returned
    current = _expected_rows(HOLIDAYS_2026, 2027)
    fetch = FakeFetch({2026: HOLIDAYS_2026})
    overwrite = FakeOverwrite()

    # WHEN dim_date is loaded (rows now end at 2026-12-31)
    result = load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader(current), overwrite=overwrite
    )

    # THEN every 2027 date, present only in the table, is changed
    assert result.changed == _all_days(date(2027, 1, 1), date(2027, 12, 31))
    assert len(overwrite.calls) == 1


# --- result ---------------------------------------------------------------------


def test_rows_is_the_number_of_built_rows_730_for_2026_and_2027() -> None:
    # GIVEN holidays for 2026 and 2027
    fetch = FakeFetch({2026: HOLIDAYS_2026, 2027: HOLIDAYS_2027})

    # WHEN dim_date is loaded
    result = load_dim_date(
        first_year=FIRST_YEAR, today=TODAY, fetch=fetch, read=_reader([]), overwrite=FakeOverwrite()
    )

    # THEN rows counts the 365 + 365 built rows
    assert result.rows == 730
