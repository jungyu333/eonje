"""Spark tests for eonje.transform.observations: raw getChargerStatus rows -> stg.obs_status rows.

Inputs are built from the real getChargerStatus page in tests/fixtures/evcharger, shaped like the
raw Parquet the collector writes (every API field a string, plus collected_at, page_fetched_at,
page_no) with the run_key and source the reader attaches. The API -> internal column mapping is
restated here on purpose, so a mis-mapping in observations.py shows up.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from eonje.storage.lake.iceberg import OBS_STATUS
from eonje.storage.raw.paths import run_key_from_run_id
from eonje.transform.observations import OBS_COLUMNS, observations

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"
KST = timezone(timedelta(hours=9))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

TIME_COLUMNS = ("observed_at", "stat_upd_dt", "last_ts", "last_te", "now_ts")
# internal column -> API field, for the four API time fields
TIME_API = {
    "stat_upd_dt": "statUpdDt",
    "last_ts": "lastTsdt",
    "last_te": "lastTedt",
    "now_ts": "nowTsdt",
}
# internal column -> API field, for every nullable value column
VALUE_API = {"status_code": "stat", **TIME_API}

Item = Mapping[str, str | None]  # None = field absent from the page (null after mergeSchema)
Row = dict[str, Any]


@dataclass(frozen=True)
class Run:
    run_key: str
    source: str
    collected_at: datetime


def _run(run_id: str, source: str, collected_at: datetime) -> Run:
    return Run(run_key_from_run_id(run_id), source, collected_at)


COLLECTED_AT = datetime(2026, 11, 9, 9, 10, tzinfo=UTC)
DELTA = _run("scheduled__2026-11-09T09:10:00+00:00", "delta", COLLECTED_AT)
NEXT_DELTA = _run(
    "scheduled__2026-11-09T09:20:00+00:00", "delta", COLLECTED_AT + timedelta(minutes=10)
)
# A full run sharing the delta run's collected_at (same Airflow logical time)
FULL_SAME_TIME = _run("scheduled__full_2026-11-09T09:10:00+00:00", "full", COLLECTED_AT)


def _load_items() -> list[dict[str, str]]:
    body = json.loads((FIXTURES / "real_status_page.json").read_text(encoding="utf-8"))
    return list(body["items"]["item"])


# 10 real chargers, all distinct keys, all with nowTsdt "".
STATUS_ITEMS = _load_items()


def _like(base: Item, **overrides: str | None) -> dict[str, str | None]:
    item = dict(base)
    item.update(overrides)
    return item


def _item(**overrides: str | None) -> dict[str, str | None]:
    """The first real charger (ME174013 / 01) with API fields overridden."""
    return _like(STATUS_ITEMS[0], **overrides)


def _raw(spark: SparkSession, rows: Sequence[tuple[Run, int, Item]]) -> DataFrame:
    """Raw charger_status rows as (run, page_no, item): API fields as strings plus meta columns."""
    fields = list(dict.fromkeys([*STATUS_ITEMS[0], *(f for _, _, item in rows for f in item)]))
    schema = StructType(
        [StructField(f, StringType()) for f in fields]
        + [
            StructField("collected_at", TimestampType(), nullable=False),
            StructField("page_fetched_at", TimestampType(), nullable=False),
            StructField("page_no", IntegerType(), nullable=False),
            StructField("run_key", StringType(), nullable=False),
            StructField("source", StringType(), nullable=False),
        ]
    )
    data = [
        (
            *(item.get(f) for f in fields),
            run.collected_at,
            run.collected_at,
            page_no,
            run.run_key,
            run.source,
        )
        for run, page_no, item in rows
    ]
    return spark.createDataFrame(data, schema)


def _pages(spark: SparkSession, *rows: tuple[int, Item]) -> DataFrame:
    """Rows of the DELTA run as (page_no, item)."""
    return _raw(spark, [(DELTA, page_no, item) for page_no, item in rows])


def _page(spark: SparkSession, *items: Item) -> DataFrame:
    return _pages(spark, *((1, item) for item in items))


def _rows(df: DataFrame) -> list[Row]:
    """Collected rows with timestamps as UTC datetimes (collect() alone gives naive local time),
    sorted by key so comparisons ignore Spark's output order."""
    as_micros = df.select(
        *[F.unix_micros(c).alias(c) if c in TIME_COLUMNS else F.col(c) for c in df.columns]
    )
    rows = []
    for collected in as_micros.collect():
        row = collected.asDict()
        for c in TIME_COLUMNS:
            if c in row and row[c] is not None:
                row[c] = EPOCH + timedelta(microseconds=row[c])
        rows.append(row)
    return sorted(rows, key=lambda r: (r["stat_id"], r["charger_id"], r["run_key"]))


def _only(df: DataFrame) -> Row:
    rows = _rows(df)
    assert len(rows) == 1, rows
    return rows[0]


def _keys(df: DataFrame) -> list[tuple[str, str]]:
    return [(r["stat_id"], r["charger_id"]) for r in _rows(df)]


# --- output schema ----------------------------------------------------------


def test_output_columns_are_obs_columns_in_the_obs_status_table_order(
    spark: SparkSession,
) -> None:
    # GIVEN a real getChargerStatus page
    raw = _page(spark, *STATUS_ITEMS)

    # WHEN it is turned into observation rows
    out = observations(raw)

    # THEN the columns are OBS_COLUMNS, which match the stg.obs_status definition in name and order
    assert out.columns == list(OBS_COLUMNS)
    assert list(OBS_COLUMNS) == [c.name for c in OBS_STATUS.columns]


def test_empty_raw_gives_no_rows_and_the_full_schema(spark: SparkSession) -> None:
    # GIVEN a raw run with no rows
    raw = _raw(spark, [])

    # WHEN it is turned into observation rows
    out = observations(raw)

    # THEN there are no rows, but the columns are still all there
    assert (out.columns, out.count()) == (list(OBS_COLUMNS), 0)


# --- mapping ----------------------------------------------------------------


def test_api_fields_map_to_internal_names_and_run_key_and_source_pass_through(
    spark: SparkSession,
) -> None:
    # GIVEN a real page of distinct chargers in a delta run
    raw = _page(spark, *STATUS_ITEMS)

    # WHEN it is turned into observation rows
    rows = _rows(observations(raw))

    # THEN statId, chgerId and stat become stat_id, charger_id and status_code, and every row
    # carries the run's run_key and source unchanged
    columns = ("stat_id", "charger_id", "status_code", "run_key", "source")
    got = [tuple(r[c] for c in columns) for r in rows]
    expected = sorted(
        (i["statId"], i["chgerId"], i["stat"], DELTA.run_key, DELTA.source) for i in STATUS_ITEMS
    )
    assert got == expected


@pytest.mark.parametrize("code", ["7", "99", "X"])
def test_unknown_status_code_is_kept_as_is(spark: SparkSession, code: str) -> None:
    # GIVEN a real row whose stat is a code the code tables do not know
    item = _item(stat=code)

    # WHEN it is turned into an observation row
    row = _only(observations(_page(spark, item)))

    # THEN the row is kept with the code unchanged
    assert row["status_code"] == code


# --- observed_at / observed_date --------------------------------------------


def test_observed_at_is_the_run_collected_at(spark: SparkSession) -> None:
    # GIVEN a real row from a run collected at 2026-11-09 09:10 UTC
    raw = _page(spark, _item())

    # WHEN it is turned into an observation row
    row = _only(observations(raw))

    # THEN observed_at is that instant
    assert row["observed_at"] == COLLECTED_AT


OBSERVED_DATES = [
    pytest.param(datetime(2026, 11, 9, 14, 59, tzinfo=UTC), date(2026, 11, 9), id="utc_14_59"),
    pytest.param(datetime(2026, 11, 9, 15, 0, tzinfo=UTC), date(2026, 11, 10), id="utc_15_00"),
    pytest.param(datetime(2026, 11, 9, 15, 10, tzinfo=UTC), date(2026, 11, 10), id="utc_15_10"),
]


@pytest.mark.parametrize(("collected_at", "expected"), OBSERVED_DATES)
def test_observed_date_is_the_kst_date_of_collected_at(
    spark: SparkSession, collected_at: datetime, expected: date
) -> None:
    # GIVEN a real row from a run collected at the given UTC instant
    run = Run(DELTA.run_key, DELTA.source, collected_at)
    raw = _raw(spark, [(run, 1, _item())])

    # WHEN it is turned into an observation row
    row = _only(observations(raw))

    # THEN observed_date is the KST calendar date (UTC 15:00 onward is the next day)
    assert row["observed_date"] == expected


# --- API time fields --------------------------------------------------------


@pytest.mark.parametrize("column", list(TIME_API))
def test_api_time_field_is_read_as_kst(spark: SparkSession, column: str) -> None:
    # GIVEN a real row whose time field is 2026-10-26 00:30:00 (no zone; the API means KST)
    item = _item(**{TIME_API[column]: "20261026003000"})

    # WHEN it is turned into an observation row
    row = _only(observations(_page(spark, item)))

    # THEN it is that KST instant (2026-10-25 15:30 UTC), not the same wall time in UTC
    assert row[column] == datetime(2026, 10, 26, 0, 30, tzinfo=KST)


def test_real_row_time_fields_are_read_as_kst(spark: SparkSession) -> None:
    # GIVEN the first real row (statUpdDt 20261025180410, lastTsdt 20261024115222,
    # lastTedt 20261024123529, nowTsdt "")
    item = _item()

    # WHEN it is turned into an observation row
    row = _only(observations(_page(spark, item)))

    # THEN each time is the KST instant, and the empty nowTsdt is null
    assert {c: row[c] for c in TIME_API} == {
        "stat_upd_dt": datetime(2026, 10, 25, 18, 4, 10, tzinfo=KST),
        "last_ts": datetime(2026, 10, 24, 11, 52, 22, tzinfo=KST),
        "last_te": datetime(2026, 10, 24, 12, 35, 29, tzinfo=KST),
        "now_ts": None,
    }


UNREADABLE_TIMES = [
    pytest.param("0", id="zero"),
    pytest.param("abc", id="text"),
    pytest.param("20261325180410", id="month_13"),
]


@pytest.mark.parametrize("text", UNREADABLE_TIMES)
@pytest.mark.parametrize("column", list(TIME_API))
def test_unreadable_time_becomes_null_and_keeps_the_row(
    spark: SparkSession, column: str, text: str
) -> None:
    # GIVEN a real row whose time field cannot be read as yyyyMMddHHmmss
    item = _item(**{TIME_API[column]: text})

    # WHEN it is turned into an observation row
    row = _only(observations(_page(spark, item)))

    # THEN the row is kept (no error) and that column is null
    assert row[column] is None


# --- blank values -> NULL ---------------------------------------------------

TAB = "\t"
IDEOGRAPHIC_SPACE = "　"  # full-width space; F.trim strips neither it nor a tab

BLANKS = [
    pytest.param("", id="empty"),
    pytest.param(" ", id="one_space"),
    pytest.param("   ", id="spaces"),
    pytest.param(TAB, id="tab"),
    pytest.param(IDEOGRAPHIC_SPACE, id="ideographic_space"),
    pytest.param(f"{TAB} {IDEOGRAPHIC_SPACE}{TAB}", id="mixed_whitespace"),
    pytest.param(None, id="missing"),
]


@pytest.mark.parametrize("blank", BLANKS)
@pytest.mark.parametrize("column", list(VALUE_API))
def test_blank_or_whitespace_only_value_becomes_null_and_keeps_the_row(
    spark: SparkSession, column: str, blank: str | None
) -> None:
    # GIVEN a real row whose value field is blank
    item = _item(**{VALUE_API[column]: blank})

    # WHEN it is turned into an observation row
    row = _only(observations(_page(spark, item)))

    # THEN the row is kept and that column is null (not an empty string)
    assert row[column] is None


# --- whitespace around keys and values --------------------------------------

EDGE_PADDINGS = [
    pytest.param(" ", " ", id="spaces"),
    pytest.param(TAB, "", id="leading_tab"),
    pytest.param("", TAB, id="trailing_tab"),
    pytest.param(IDEOGRAPHIC_SPACE, "", id="leading_ideographic_space"),
    pytest.param("", IDEOGRAPHIC_SPACE, id="trailing_ideographic_space"),
    pytest.param(
        f"{TAB}{IDEOGRAPHIC_SPACE} ", f" {IDEOGRAPHIC_SPACE}{TAB}", id="mixed_on_both_sides"
    ),
]


def _padded(fields: Sequence[str], before: str, after: str) -> dict[str, str | None]:
    """The first real charger with `before` / `after` wrapped around the given API fields."""
    base = _item()
    return _item(**{f: f"{before}{base[f]}{after}" for f in fields})


@pytest.mark.parametrize(("before", "after"), EDGE_PADDINGS)
def test_whitespace_around_the_key_is_trimmed(spark: SparkSession, before: str, after: str) -> None:
    # GIVEN a real row whose statId and chgerId have whitespace around them
    item = _padded(["statId", "chgerId"], before, after)

    # WHEN it is turned into an observation row
    keys = _keys(observations(_page(spark, item)))

    # THEN the key is stored without the whitespace
    assert keys == [("ME174013", "01")]


@pytest.mark.parametrize(("before", "after"), EDGE_PADDINGS)
def test_padded_and_clean_key_in_one_run_are_the_same_charger(
    spark: SparkSession, before: str, after: str
) -> None:
    # GIVEN one run holding a real row and the same charger with whitespace around its key
    clean = _item()
    padded = _padded(["statId", "chgerId"], before, after)

    # WHEN they are turned into observation rows
    keys = _keys(observations(_page(spark, clean, padded)))

    # THEN there is one row, under the key without the whitespace
    assert keys == [("ME174013", "01")]


@pytest.mark.parametrize(("before", "after"), EDGE_PADDINGS)
def test_whitespace_around_the_status_code_is_trimmed(
    spark: SparkSession, before: str, after: str
) -> None:
    # GIVEN a real row whose stat "2" has whitespace around it
    item = _padded(["stat"], before, after)

    # WHEN it is turned into an observation row
    row = _only(observations(_page(spark, item)))

    # THEN the code is stored without the whitespace
    assert row["status_code"] == "2"


@pytest.mark.parametrize(("before", "after"), EDGE_PADDINGS)
def test_whitespace_around_a_time_is_trimmed_before_it_is_read(
    spark: SparkSession, before: str, after: str
) -> None:
    # GIVEN a real row whose time fields have whitespace around them
    item = _padded(["statUpdDt", "lastTsdt", "lastTedt"], before, after)

    # WHEN it is turned into an observation row
    row = _only(observations(_page(spark, item)))

    # THEN the times are read as if the whitespace were not there
    assert (row["stat_upd_dt"], row["last_ts"], row["last_te"]) == (
        datetime(2026, 10, 25, 18, 4, 10, tzinfo=KST),
        datetime(2026, 10, 24, 11, 52, 22, tzinfo=KST),
        datetime(2026, 10, 24, 12, 35, 29, tzinfo=KST),
    )


# --- blank keys -------------------------------------------------------------

BLANK_KEY_VALUES: list[tuple[str, str | None]] = [
    ("empty", ""),
    ("spaces", "   "),
    ("tab", TAB),
    ("ideographic_space", IDEOGRAPHIC_SPACE),
    ("missing", None),
]
BLANK_KEYS = [
    pytest.param(field, value, id=f"{field}-{name}")
    for field in ("statId", "chgerId")
    for name, value in BLANK_KEY_VALUES
]


@pytest.mark.parametrize(("field", "value"), BLANK_KEYS)
def test_row_with_a_blank_key_is_dropped(
    spark: SparkSession, field: str, value: str | None
) -> None:
    # GIVEN a page with one valid charger and one whose statId or chgerId is blank
    good = _item(statId="S1", chgerId="01")
    bad = _item(**{"statId": "S2", "chgerId": "02", field: value})

    # WHEN it is turned into observation rows
    keys = _keys(observations(_page(spark, good, bad)))

    # THEN only the valid charger remains
    assert keys == [("S1", "01")]


# --- one row per charger per run --------------------------------------------

INPUT_ORDERS = [
    pytest.param(False, id="as_listed"),
    pytest.param(True, id="reversed"),
]


@pytest.mark.parametrize("reverse", INPUT_ORDERS)
def test_charger_twice_in_a_run_keeps_the_later_stat_upd_dt_even_on_an_earlier_page(
    spark: SparkSession, reverse: bool
) -> None:
    # GIVEN one charger on page 1 with the later statUpdDt and again on page 2 with an earlier one
    rows = [
        (1, _item(stat="3", statUpdDt="20261025181000")),
        (2, _item(stat="2", statUpdDt="20261025180410")),
    ]
    rows = rows[::-1] if reverse else rows

    # WHEN it is turned into observation rows
    row = _only(observations(_pages(spark, *rows)))

    # THEN the page 1 row (later statUpdDt) is kept
    assert (row["status_code"], row["stat_upd_dt"]) == (
        "3",
        datetime(2026, 10, 25, 18, 10, tzinfo=KST),
    )


@pytest.mark.parametrize("reverse", INPUT_ORDERS)
def test_charger_twice_in_a_run_with_equal_stat_upd_dt_keeps_the_later_page(
    spark: SparkSession, reverse: bool
) -> None:
    # GIVEN one charger on page 1 and page 2 with the same statUpdDt but different stat
    rows = [(1, _item(stat="2")), (2, _item(stat="3"))]
    rows = rows[::-1] if reverse else rows

    # WHEN it is turned into observation rows
    row = _only(observations(_pages(spark, *rows)))

    # THEN the page 2 row is kept
    assert row["status_code"] == "3"


TIES = [
    pytest.param({"stat": "2"}, {"stat": "3"}, id="stat"),
    pytest.param({"stat": "2"}, {"stat": ""}, id="stat_vs_blank"),
    pytest.param({"lastTsdt": "20261024115222"}, {"lastTsdt": "20261025090000"}, id="last_ts"),
    pytest.param({"lastTedt": "20261024123529"}, {"lastTedt": ""}, id="last_te_vs_blank"),
    pytest.param({"nowTsdt": ""}, {"nowTsdt": "20261025180000"}, id="now_ts"),
    pytest.param(
        {"stat": "2", "nowTsdt": "20261025180000"},
        {"stat": "3", "nowTsdt": ""},
        id="several_fields",
    ),
]


@pytest.mark.parametrize(("first", "second"), TIES)
def test_charger_twice_with_equal_stat_upd_dt_and_page_keeps_the_same_row_in_any_input_order(
    spark: SparkSession, first: dict[str, str], second: dict[str, str]
) -> None:
    # GIVEN one charger twice on the same page with the same statUpdDt, differing in other fields
    rows = [(1, _item(**first)), (1, _item(**second))]

    # WHEN the rows are turned into observation rows in input order and in reverse order
    forward = _rows(observations(_pages(spark, *rows)))
    backward = _rows(observations(_pages(spark, *rows[::-1])))

    # THEN each gives one row, and it is the same row
    assert len(forward) == 1, forward
    assert forward == backward


UNREADABLE_STAT_UPD_DT = [
    pytest.param("", id="empty"),
    pytest.param(None, id="missing"),
    pytest.param("0", id="zero"),
    pytest.param("abc", id="text"),
    pytest.param("20261325180410", id="month_13"),
]


@pytest.mark.parametrize("unreadable", UNREADABLE_STAT_UPD_DT)
def test_charger_twice_keeps_the_row_with_a_readable_stat_upd_dt(
    spark: SparkSession, unreadable: str | None
) -> None:
    # GIVEN one charger on page 1 with a readable statUpdDt and on page 2 (the later page) with
    # one that cannot be read
    rows = [
        (1, _item(stat="2", statUpdDt="20261025180410")),
        (2, _item(stat="3", statUpdDt=unreadable)),
    ]

    # WHEN it is turned into observation rows
    row = _only(observations(_pages(spark, *rows)))

    # THEN the row with the readable statUpdDt is kept
    assert (row["status_code"], row["stat_upd_dt"]) == (
        "2",
        datetime(2026, 10, 25, 18, 4, 10, tzinfo=KST),
    )


TWO_RUNS = [
    pytest.param(DELTA, NEXT_DELTA, id="two_delta_runs"),
    pytest.param(DELTA, FULL_SAME_TIME, id="delta_and_full_at_the_same_collected_at"),
]


@pytest.mark.parametrize(("first", "second"), TWO_RUNS)
def test_same_report_in_two_runs_is_kept_as_two_observations(
    spark: SparkSession, first: Run, second: Run
) -> None:
    # GIVEN the same real row, unchanged, in two different runs
    item = _item()
    raw = _raw(spark, [(first, 1, item), (second, 1, item)])

    # WHEN it is turned into observation rows
    rows = _rows(observations(raw))

    # THEN each run keeps its own observation, with its own run_key, source and observed_at
    got = sorted((r["run_key"], r["source"], r["observed_at"]) for r in rows)
    expected = sorted((run.run_key, run.source, run.collected_at) for run in (first, second))
    assert got == expected


# --- determinism ------------------------------------------------------------


def test_building_twice_from_the_same_raw_gives_the_same_rows(spark: SparkSession) -> None:
    # GIVEN a real page plus in-run duplicates on the same page and on a later page
    rows = [(1, item) for item in STATUS_ITEMS] + [
        (1, _like(STATUS_ITEMS[0], stat="3")),
        (2, _like(STATUS_ITEMS[9], stat="5")),
    ]
    raw = _pages(spark, *rows)

    # WHEN it is turned into observation rows twice
    first = _rows(observations(raw))
    second = _rows(observations(raw))

    # THEN the results are identical
    assert first == second
