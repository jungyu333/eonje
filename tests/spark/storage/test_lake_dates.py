"""Spark tests for eonje.storage.lake.dates: overwrite_dim_date replaces core.dim_date whole in one
Iceberg commit, and read_dim_date reads it back as DateRow values in date order.

Every test creates core.dim_date with create_table_sql in the throwaway `lake` catalog of
tests/spark/conftest.py and drops it when the test ends. Rows are built with date_rows, as the job
does, from explicit dates and holiday names.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from dataclasses import astuple

import pytest
from pyspark.sql import SparkSession

from eonje.storage.lake.dates import overwrite_dim_date, read_dim_date
from eonje.storage.lake.iceberg import DIM_DATE, create_table_sql
from eonje.storage.lake.spark import CATALOG
from eonje.transform.dates import DateRow, date_rows

# October 2026: 10-03 (Sat) and 10-09 (Fri) are holidays, 10-05 a substitute holiday.
OCTOBER = date_rows(
    dt.date(2026, 10, 1),
    dt.date(2026, 10, 10),
    [
        (dt.date(2026, 10, 3), "개천절"),
        (dt.date(2026, 10, 5), "대체공휴일"),
        (dt.date(2026, 10, 9), "한글날"),
    ],
)
# A period that does not overlap OCTOBER, with Christmas on a Friday.
DECEMBER = date_rows(
    dt.date(2026, 12, 20),
    dt.date(2026, 12, 31),
    [(dt.date(2026, 12, 25), "기독탄신일")],
)


@pytest.fixture
def table(spark: SparkSession) -> Iterator[None]:
    """core.dim_date, created empty with create_table_sql and dropped when the test ends."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DIM_DATE.namespace}")
    spark.sql(create_table_sql(DIM_DATE))
    yield
    spark.sql(f"DROP TABLE IF EXISTS {DIM_DATE.full_name}")


def _commits(spark: SparkSession) -> int:
    return spark.table(f"{DIM_DATE.full_name}.snapshots").count()


def _types(rows: list[DateRow]) -> list[tuple[type, ...]]:
    return [tuple(type(value) for value in astuple(row)) for row in rows]


# --- read -------------------------------------------------------------------


@pytest.mark.usefixtures("table")
def test_reading_an_empty_table_gives_an_empty_list(spark: SparkSession) -> None:
    # GIVEN the newly created, empty dim_date table

    # WHEN it is read
    rows = read_dim_date(spark)

    # THEN there are no rows
    assert rows == []


# --- overwrite --------------------------------------------------------------


@pytest.mark.usefixtures("table")
def test_overwriting_an_empty_table_reads_back_the_same_rows(spark: SparkSession) -> None:
    # GIVEN the newly created, empty dim_date table and ten October days, three of them holidays

    # WHEN the October rows are written
    overwrite_dim_date(spark, OCTOBER)

    # THEN reading gives the same DateRows, values equal and of the same types (date, int, bool,
    # str or None), so a 1 is not read back for True nor a datetime for a date
    rows = read_dim_date(spark)
    assert rows == OCTOBER
    assert _types(rows) == _types(OCTOBER)


@pytest.mark.usefixtures("table")
def test_overwriting_with_another_period_leaves_none_of_the_previous_rows(
    spark: SparkSession,
) -> None:
    # GIVEN the table holding the October rows
    overwrite_dim_date(spark, OCTOBER)

    # WHEN it is overwritten with December rows that share no date with October
    overwrite_dim_date(spark, DECEMBER)

    # THEN only the December rows are in the table
    assert read_dim_date(spark) == DECEMBER


@pytest.mark.usefixtures("table")
def test_rows_passed_out_of_order_are_read_back_in_date_order(spark: SparkSession) -> None:
    # GIVEN the October rows in a shuffled, fixed order
    shuffled = [OCTOBER[i] for i in (7, 2, 9, 0, 5, 3, 8, 1, 6, 4)]

    # WHEN they are written
    overwrite_dim_date(spark, shuffled)

    # THEN reading gives them sorted by date
    assert read_dim_date(spark) == OCTOBER


@pytest.mark.usefixtures("table")
def test_row_without_a_holiday_name_is_read_back_with_name_none(spark: SparkSession) -> None:
    # GIVEN a plain weekday and a holiday on the next day
    plain, holiday = date_rows(
        dt.date(2026, 10, 8), dt.date(2026, 10, 9), [(dt.date(2026, 10, 9), "한글날")]
    )
    assert plain.holiday_name is None  # precondition: the row carries no name

    # WHEN both are written
    overwrite_dim_date(spark, [plain, holiday])

    # THEN the plain day's name is None (not "" or "None") and the holiday keeps its name
    [read_plain, read_holiday] = read_dim_date(spark)
    assert read_plain.holiday_name is None
    assert read_holiday.holiday_name == "한글날"


@pytest.mark.usefixtures("table")
def test_one_overwrite_is_one_iceberg_commit(spark: SparkSession) -> None:
    # GIVEN the table holding the October rows
    overwrite_dim_date(spark, OCTOBER)
    before = _commits(spark)

    # WHEN it is overwritten with the December rows
    overwrite_dim_date(spark, DECEMBER)

    # THEN the table has exactly one more snapshot
    assert _commits(spark) == before + 1


@pytest.mark.usefixtures("table")
def test_overwriting_with_no_rows_raises_and_leaves_the_table_as_it_was(
    spark: SparkSession,
) -> None:
    # GIVEN the table holding the October rows
    overwrite_dim_date(spark, OCTOBER)
    before = _commits(spark)

    # WHEN it is overwritten with an empty list
    with pytest.raises(ValueError, match="no rows"):
        overwrite_dim_date(spark, [])

    # THEN the October rows are still there and no commit was made
    assert read_dim_date(spark) == OCTOBER
    assert _commits(spark) == before
