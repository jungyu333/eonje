from datetime import date
from pathlib import Path

import pytest

from eonje.client.holidays import parse_holidays
from eonje.transform.dates import date_rows

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "holidays"

MONDAY = date(2026, 11, 16)
SUNDAY = date(2026, 11, 22)


# --- range ----------------------------------------------------------------------


def test_both_ends_are_included_one_row_per_day_in_date_order() -> None:
    # GIVEN a range from 2026-11-16 to 2026-11-22
    # WHEN the rows are built
    rows = date_rows(MONDAY, SUNDAY, [])

    # THEN there is one row per day, both ends included, in date order
    assert [row.date for row in rows] == [date(2026, 11, d) for d in range(16, 23)]


def test_same_start_and_end_yields_one_row() -> None:
    # GIVEN a range whose start equals its end
    # WHEN the rows are built
    rows = date_rows(MONDAY, MONDAY, [])

    # THEN there is exactly one row for that day
    assert [row.date for row in rows] == [MONDAY]


def test_start_after_end_raises_value_error() -> None:
    # GIVEN a start one day after the end
    # WHEN the rows are built
    # THEN ValueError is raised
    with pytest.raises(ValueError):
        date_rows(date(2026, 11, 17), MONDAY, [])


def test_leap_year_2028_has_366_rows_including_february_29() -> None:
    # GIVEN the whole of leap year 2028
    # WHEN the rows are built
    rows = date_rows(date(2028, 1, 1), date(2028, 12, 31), [])

    # THEN there are 366 rows and February 29 is among them
    assert len(rows) == 366
    assert date(2028, 2, 29) in [row.date for row in rows]


# --- calendar fields ------------------------------------------------------------


def test_year_month_day_match_the_date() -> None:
    # GIVEN a range crossing a month and a year boundary
    # WHEN the rows are built
    rows = date_rows(date(2026, 12, 30), date(2027, 1, 2), [])

    # THEN year, month and day match each row's date
    assert [(r.year, r.month, r.day) for r in rows] == [
        (2026, 12, 30),
        (2026, 12, 31),
        (2027, 1, 1),
        (2027, 1, 2),
    ]


def test_day_of_week_is_iso_monday_1_to_sunday_7() -> None:
    # GIVEN the week from Monday 2026-11-16 to Sunday 2026-11-22
    # WHEN the rows are built
    rows = date_rows(MONDAY, SUNDAY, [])

    # THEN day_of_week runs 1 (Monday) to 7 (Sunday)
    assert [row.day_of_week for row in rows] == [1, 2, 3, 4, 5, 6, 7]


def test_only_saturday_and_sunday_are_weekend() -> None:
    # GIVEN the week from Monday 2026-11-16 to Sunday 2026-11-22
    # WHEN the rows are built
    rows = date_rows(MONDAY, SUNDAY, [])

    # THEN only Saturday and Sunday have is_weekend
    assert [row.is_weekend for row in rows] == [False] * 5 + [True, True]


# --- holidays -------------------------------------------------------------------


def test_listed_day_is_holiday_with_its_name() -> None:
    # GIVEN a holiday listed on Wednesday 2026-11-18
    holidays = [(date(2026, 11, 18), "테스트공휴일")]

    # WHEN the rows are built
    rows = date_rows(MONDAY, SUNDAY, holidays)

    # THEN that day is a holiday with the name
    (row,) = [r for r in rows if r.date == date(2026, 11, 18)]
    assert row.is_holiday is True
    assert row.holiday_name == "테스트공휴일"


def test_unlisted_day_is_not_holiday_and_has_no_name() -> None:
    # GIVEN a holiday listed on Wednesday 2026-11-18 only
    holidays = [(date(2026, 11, 18), "테스트공휴일")]

    # WHEN the rows are built
    rows = date_rows(MONDAY, SUNDAY, holidays)

    # THEN Tuesday is not a holiday and has no name
    (row,) = [r for r in rows if r.date == date(2026, 11, 17)]
    assert row.is_holiday is False
    assert row.holiday_name is None


def test_weekend_not_in_holiday_list_is_not_holiday() -> None:
    # GIVEN no holidays listed
    # WHEN the rows for a week are built
    rows = date_rows(MONDAY, SUNDAY, [])

    # THEN Saturday and Sunday are weekend but not holidays
    weekend = [r for r in rows if r.is_weekend]
    assert len(weekend) == 2
    assert all(r.is_holiday is False and r.holiday_name is None for r in weekend)


def test_two_holidays_on_one_day_are_joined_in_received_order() -> None:
    # GIVEN two holidays on the same day, "임시공휴일" received before "전국동시지방선거"
    day = date(2026, 6, 3)
    holidays = [(day, "임시공휴일"), (day, "전국동시지방선거")]

    # WHEN the rows are built
    (row,) = date_rows(day, day, holidays)

    # THEN the names are joined with ", " in received order
    assert row.is_holiday is True
    assert row.holiday_name == "임시공휴일, 전국동시지방선거"


def test_two_holidays_on_one_day_keep_received_order_not_sorted_order() -> None:
    # GIVEN two holidays on the same day received in reverse alphabetical order
    day = date(2026, 6, 3)
    holidays = [(day, "전국동시지방선거"), (day, "임시공휴일")]

    # WHEN the rows are built
    (row,) = date_rows(day, day, holidays)

    # THEN the names keep the received order
    assert row.holiday_name == "전국동시지방선거, 임시공휴일"


def test_same_name_twice_on_one_day_is_written_once() -> None:
    # GIVEN the same holiday name received twice for one day
    day = date(2026, 10, 3)
    holidays = [(day, "개천절"), (day, "개천절")]

    # WHEN the rows are built
    (row,) = date_rows(day, day, holidays)

    # THEN the name appears once
    assert row.is_holiday is True
    assert row.holiday_name == "개천절"


def test_holiday_with_empty_name_is_holiday_without_name() -> None:
    # GIVEN a holiday with an empty name
    day = date(2026, 10, 3)

    # WHEN the rows are built
    (row,) = date_rows(day, day, [(day, "")])

    # THEN the day is a holiday and has no name
    assert row.is_holiday is True
    assert row.holiday_name is None


def test_holidays_outside_the_range_are_ignored() -> None:
    # GIVEN holidays before and after the range only
    holidays = [(date(2026, 11, 15), "전날"), (date(2026, 11, 23), "다음날")]

    # WHEN the rows are built
    rows = date_rows(MONDAY, SUNDAY, holidays)

    # THEN the range is unchanged and no row is a holiday
    assert [row.date for row in rows] == [date(2026, 11, d) for d in range(16, 23)]
    assert all(r.is_holiday is False and r.holiday_name is None for r in rows)


# --- real response --------------------------------------------------------------


def test_real_2026_fixture_yields_365_rows_with_22_holidays() -> None:
    # GIVEN the holidays parsed from the real 2026 getRestDeInfo response
    text = (FIXTURES / "rest_de_info_2026.json").read_text(encoding="utf-8")
    holidays = [(h.day, h.name) for h in parse_holidays(text)]

    # WHEN the rows for the whole of 2026 are built
    rows = date_rows(date(2026, 1, 1), date(2026, 12, 31), holidays)

    # THEN there are 365 rows and 22 of them are holidays
    assert len(rows) == 365
    assert sum(r.is_holiday for r in rows) == 22
