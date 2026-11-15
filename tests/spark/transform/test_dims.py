"""Spark tests for eonje.transform.dims: raw getChargerInfo rows -> dim_station / dim_charger rows,
and scd2_changes: the dim table plus a new snapshot -> the rows load_dims merges.

Inputs are built from the real getChargerInfo page in tests/fixtures/evcharger, shaped like the
raw Parquet the collector writes (every API field a string, plus collected_at, page_fetched_at,
page_no). The API -> internal column mapping is restated here on purpose, so a mis-mapping in
dims.py shows up.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from eonje.storage.lake.iceberg import DIM_CHARGER, DIM_STATION, IcebergTable
from eonje.storage.raw.paths import run_key_from_run_id
from eonje.transform.dims import chargers, scd2_changes, stations

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"
COLLECTED_AT = datetime(2026, 11, 9, 18, 11, tzinfo=UTC)
NEXT_DAY = COLLECTED_AT + timedelta(days=1)

# SCD2 columns that load_dims adds; row_hash is produced by the transform itself.
LOAD_ADDED = ("valid_from", "valid_to", "is_current", "source_run_key")

Item = Mapping[str, str | None]  # None = field absent from the page (null after mergeSchema)
Row = dict[str, Any]

STATION_API = {
    "stat_id": "statId",
    "stat_nm": "statNm",
    "addr": "addr",
    "addr_detail": "addrDetail",
    "location": "location",
    "lat": "lat",
    "lng": "lng",
    "use_time": "useTime",
    "busi_id": "busiId",
    "bnm": "bnm",
    "busi_nm": "busiNm",
    "busi_call": "busiCall",
    "zcode": "zcode",
    "zscode": "zscode",
    "kind": "kind",
    "kind_detail": "kindDetail",
    "parking_free": "parkingFree",
    "note": "note",
    "limit_yn": "limitYn",
    "limit_detail": "limitDetail",
    "traffic_yn": "trafficYn",
}

CHARGER_API = {
    "stat_id": "statId",
    "charger_id": "chgerId",
    "chger_type": "chgerType",
    "output_kw": "output",
    "method": "method",
    "power_type": "powerType",
    "maker": "maker",
    "install_year": "year",
    "floor_type": "floorType",
    "floor_num": "floorNum",
    "del_yn": "delYn",
    "del_detail": "delDetail",
}


@dataclass(frozen=True)
class Dim:
    build: Callable[[DataFrame], DataFrame]
    table: IcebergTable
    key: tuple[str, ...]
    api: Mapping[str, str]  # internal column -> API field

    @property
    def columns(self) -> list[str]:
        """Expected output columns: the Iceberg definition without the load-added SCD2 columns."""
        return [c.name for c in self.table.columns if c.name not in LOAD_ADDED]

    @property
    def attrs(self) -> list[str]:
        """Attribute columns: neither key nor row_hash."""
        return [c for c in self.columns if c not in self.key and c != "row_hash"]


STATION = Dim(stations, DIM_STATION, ("stat_id",), STATION_API)
CHARGER = Dim(chargers, DIM_CHARGER, ("stat_id", "charger_id"), CHARGER_API)
BOTH = [pytest.param(STATION, id="station"), pytest.param(CHARGER, id="charger")]


def _load_items() -> list[dict[str, str]]:
    body = json.loads((FIXTURES / "real_info_page.json").read_text(encoding="utf-8"))
    return list(body["items"]["item"])


# 10 real chargers, 8 stations: ME178009 has chargers 01 and 02, ME181150 has 12 and 13.
INFO_ITEMS = _load_items()


def _like(base: Item, **overrides: str | None) -> dict[str, str | None]:
    item = dict(base)
    item.update(overrides)
    return item


def _item(**overrides: str | None) -> dict[str, str | None]:
    """The first real charger (ME174013 / 01, a one-charger station) with API fields overridden."""
    return _like(INFO_ITEMS[0], **overrides)


def _raw(
    spark: SparkSession,
    rows: Sequence[tuple[int, Item]],
    *,
    collected_at: datetime = COLLECTED_AT,
) -> DataFrame:
    """Raw charger_info rows as (page_no, item): API fields as strings plus the meta columns."""
    fields = list(dict.fromkeys([*INFO_ITEMS[0], *(f for _, item in rows for f in item)]))
    schema = StructType(
        [StructField(f, StringType()) for f in fields]
        + [
            StructField("collected_at", TimestampType(), nullable=False),
            StructField("page_fetched_at", TimestampType(), nullable=False),
            StructField("page_no", IntegerType(), nullable=False),
        ]
    )
    data = [
        (*(item.get(f) for f in fields), collected_at, collected_at, page_no)
        for page_no, item in rows
    ]
    return spark.createDataFrame(data, schema)


def _page(spark: SparkSession, *items: Item) -> DataFrame:
    return _raw(spark, [(1, item) for item in items])


def _rows(df: DataFrame) -> list[Row]:
    """Collected rows as dicts in a fixed order, so comparisons ignore Spark's output order."""
    rows = [r.asDict() for r in df.collect()]
    return sorted(rows, key=lambda r: (r["stat_id"], r.get("charger_id") or "", r["row_hash"]))


def _only(df: DataFrame) -> Row:
    rows = _rows(df)
    assert len(rows) == 1, rows
    return rows[0]


def _keys(df: DataFrame, dim: Dim) -> list[tuple[str, ...]]:
    return sorted(tuple(r[k] for k in dim.key) for r in _rows(df))


def _hash(
    spark: SparkSession,
    dim: Dim,
    rows: Sequence[tuple[int, Item]],
    *,
    collected_at: datetime = COLLECTED_AT,
) -> str:
    return str(_only(dim.build(_raw(spark, rows, collected_at=collected_at)))["row_hash"])


def _padded(dim: Dim, columns: Sequence[str], before: str, after: str) -> dict[str, str | None]:
    """The first real charger with `before` / `after` wrapped around the fields of `columns`."""
    base = _item()
    return _item(**{dim.api[c]: f"{before}{base[dim.api[c]]}{after}" for c in columns})


# --- output schema ----------------------------------------------------------


@pytest.mark.parametrize("dim", BOTH)
def test_output_columns_are_the_dim_table_columns_without_load_added_scd2_columns(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a real getChargerInfo page
    raw = _page(spark, *INFO_ITEMS)

    # WHEN it is turned into dimension rows
    out = dim.build(raw)

    # THEN the columns match the Iceberg definition minus valid_from/valid_to/is_current/
    # source_run_key, in name and order
    assert out.columns == [c.name for c in dim.table.columns if c.name not in LOAD_ADDED]


@pytest.mark.parametrize("dim", BOTH)
def test_output_column_types_match_the_dim_table(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a real getChargerInfo page
    raw = _page(spark, *INFO_ITEMS)

    # WHEN it is turned into dimension rows
    out = dim.build(raw)

    # THEN every column has the Iceberg type (numbers are no longer strings)
    expected = [(c.name, c.type.lower()) for c in dim.table.columns if c.name in dim.columns]
    assert out.dtypes == expected


@pytest.mark.parametrize("dim", BOTH)
def test_empty_raw_gives_no_rows_and_the_full_schema(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a raw run with no rows
    raw = _raw(spark, [])

    # WHEN it is turned into dimension rows
    out = dim.build(raw)

    # THEN there are no rows, but the columns are still all there
    assert (out.columns, out.count()) == (dim.columns, 0)


@pytest.mark.parametrize("dim", BOTH)
def test_unknown_api_field_is_ignored_not_an_error(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a raw row carrying a field the API did not have before
    raw = _page(spark, _item(newField="surprise"))

    # WHEN it is turned into dimension rows
    out = dim.build(raw)

    # THEN the row is kept and the unknown field does not leak into the output
    assert out.columns == dim.columns
    assert out.count() == 1


@pytest.mark.parametrize("dim", BOTH)
def test_raw_without_the_stat_id_column_raises(spark: SparkSession, dim: Dim) -> None:
    # GIVEN raw rows missing the required statId column
    raw = _page(spark, _item()).drop("statId")

    # WHEN it is turned into dimension rows
    # THEN the contract violation is raised, not silently produced as empty output
    with pytest.raises(AnalysisException):
        dim.build(raw).collect()


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
@pytest.mark.parametrize("dim", BOTH)
def test_blank_whitespace_or_missing_attribute_becomes_null(
    spark: SparkSession, dim: Dim, blank: str | None
) -> None:
    # GIVEN a real row whose every attribute field is blank
    item = _item(**{dim.api[c]: blank for c in dim.attrs})

    # WHEN it is turned into a dimension row
    row = _only(dim.build(_page(spark, item)))

    # THEN the row is kept and every attribute is null (not an empty string)
    assert {c: row[c] for c in dim.attrs} == dict.fromkeys(dim.attrs)


@pytest.mark.parametrize("dim", BOTH)
def test_row_hash_is_set_when_every_attribute_is_null(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a real row whose every attribute field is empty
    item = _item(**{dim.api[c]: "" for c in dim.attrs})

    # WHEN it is turned into a dimension row
    row = _only(dim.build(_page(spark, item)))

    # THEN row_hash still has a value (the Iceberg column is NOT NULL)
    assert row["row_hash"] is not None


# --- tabs and ideographic spaces around a value -----------------------------

EDGE_PADDINGS = [
    pytest.param(TAB, "", id="leading_tab"),
    pytest.param("", TAB, id="trailing_tab"),
    pytest.param(IDEOGRAPHIC_SPACE, "", id="leading_ideographic_space"),
    pytest.param("", IDEOGRAPHIC_SPACE, id="trailing_ideographic_space"),
    pytest.param(
        f"{TAB}{IDEOGRAPHIC_SPACE} ", f" {IDEOGRAPHIC_SPACE}{TAB}", id="mixed_on_both_sides"
    ),
]
SPACE_KINDS = [
    pytest.param(TAB, id="tab"),
    pytest.param(IDEOGRAPHIC_SPACE, id="ideographic_space"),
]
TEXT_ATTRIBUTES = [
    pytest.param(STATION, "stat_nm", id="station-stat_nm"),
    pytest.param(CHARGER, "maker", id="charger-maker"),
]
# The real values of the first fixture charger, as stored.
NUMBER_ATTRIBUTES = [
    pytest.param(STATION, {"lat": 37.476296, "lng": 126.9583876}, id="station-lat_lng"),
    pytest.param(CHARGER, {"output_kw": 50, "install_year": 2017}, id="charger-output_year"),
]


@pytest.mark.parametrize(("before", "after"), EDGE_PADDINGS)
@pytest.mark.parametrize(("dim", "column"), TEXT_ATTRIBUTES)
def test_tab_or_ideographic_space_around_a_text_attribute_is_trimmed(
    spark: SparkSession, dim: Dim, column: str, before: str, after: str
) -> None:
    # GIVEN a real row whose text field has a tab or ideographic space before or after the value
    value = _item()[dim.api[column]]
    item = _padded(dim, [column], before, after)

    # WHEN it is turned into a dimension row
    row = _only(dim.build(_page(spark, item)))

    # THEN the stored value is the real value without the surrounding whitespace
    assert row[column] == value


@pytest.mark.parametrize("space", SPACE_KINDS)
@pytest.mark.parametrize(("dim", "column"), TEXT_ATTRIBUTES)
def test_tab_or_ideographic_space_inside_a_text_attribute_is_kept(
    spark: SparkSession, dim: Dim, column: str, space: str
) -> None:
    # GIVEN a real row whose text field has the same whitespace between two words and around them
    value = _item()[dim.api[column]]
    inner = f"{value}{space}{value}"
    item = _item(**{dim.api[column]: f"{space}{inner}{space}"})

    # WHEN it is turned into a dimension row
    row = _only(dim.build(_page(spark, item)))

    # THEN only the surrounding whitespace is removed; the one between the words stays
    assert row[column] == inner


@pytest.mark.parametrize("space", SPACE_KINDS)
@pytest.mark.parametrize(("dim", "expected"), NUMBER_ATTRIBUTES)
def test_tab_or_ideographic_space_around_a_number_is_trimmed_before_the_type_cast(
    spark: SparkSession, dim: Dim, expected: dict[str, float], space: str
) -> None:
    # GIVEN a real row whose number fields have a tab or ideographic space on both sides
    # (a cast skips a tab but not U+3000: "　50" cast to int is null)
    item = _padded(dim, list(expected), space, space)

    # WHEN it is turned into a dimension row
    row = _only(dim.build(_page(spark, item)))

    # THEN the numbers are parsed as if the whitespace were not there
    assert {c: row[c] for c in expected} == expected


# --- numbers ----------------------------------------------------------------


def test_charger_output_and_year_become_integers(spark: SparkSession) -> None:
    # GIVEN a real charger row with output "50" and year "2017"
    item = _item()
    assert (item["output"], item["year"]) == ("50", "2017")

    # WHEN it is turned into a charger row
    row = _only(chargers(_page(spark, item)))

    # THEN output_kw and install_year are integers
    assert (row["output_kw"], row["install_year"]) == (50, 2017)


def test_station_coordinates_become_doubles(spark: SparkSession) -> None:
    # GIVEN a real station row with lat "37.476296" and lng "126.9583876"
    item = _item()
    assert (item["lat"], item["lng"]) == ("37.476296", "126.9583876")

    # WHEN it is turned into a station row
    row = _only(stations(_page(spark, item)))

    # THEN lat and lng are doubles
    assert (row["lat"], row["lng"]) == (37.476296, 126.9583876)


NON_NUMERIC = [
    pytest.param(CHARGER, "output_kw", "fast", id="charger-output"),
    pytest.param(CHARGER, "install_year", "2017년", id="charger-year"),
    pytest.param(STATION, "lat", "N/A", id="station-lat"),
    pytest.param(STATION, "lng", "127.0.1", id="station-lng"),
]


@pytest.mark.parametrize(("dim", "column", "text"), NON_NUMERIC)
def test_non_numeric_text_in_a_number_column_becomes_null_and_keeps_the_row(
    spark: SparkSession, dim: Dim, column: str, text: str
) -> None:
    # GIVEN a real row whose number field holds text that is not a number
    item = _item(**{dim.api[column]: text})

    # WHEN it is turned into a dimension row
    row = _only(dim.build(_page(spark, item)))

    # THEN the row is kept (no error) and that column is null
    assert row[column] is None


# --- keys -------------------------------------------------------------------

BLANK_KEY_VALUES: list[tuple[str, str | None]] = [
    ("empty", ""),
    ("spaces", "   "),
    ("tab", TAB),
    ("ideographic_space", IDEOGRAPHIC_SPACE),
    ("missing", None),
]
BLANK_CHARGER_KEYS = [
    pytest.param(field, value, id=f"{field}-{name}")
    for field in ("statId", "chgerId")
    for name, value in BLANK_KEY_VALUES
]
BLANK_STATION_KEYS = [pytest.param(value, id=name) for name, value in BLANK_KEY_VALUES]


@pytest.mark.parametrize(("field", "value"), BLANK_CHARGER_KEYS)
def test_charger_row_with_a_blank_key_is_dropped(
    spark: SparkSession, field: str, value: str | None
) -> None:
    # GIVEN a page with one valid charger and one whose key field is blank
    good = _item(statId="S1", chgerId="01")
    bad = _item(**{"statId": "S2", "chgerId": "02", field: value})

    # WHEN it is turned into charger rows
    keys = _keys(chargers(_page(spark, good, bad)), CHARGER)

    # THEN only the valid charger remains
    assert keys == [("S1", "01")]


@pytest.mark.parametrize("value", BLANK_STATION_KEYS)
def test_station_row_with_a_blank_stat_id_is_dropped(
    spark: SparkSession, value: str | None
) -> None:
    # GIVEN a page with one valid station and one row whose statId is blank
    good = _item(statId="S1", chgerId="01")
    bad = _item(statId=value, chgerId="02")

    # WHEN it is turned into station rows
    keys = _keys(stations(_page(spark, good, bad)), STATION)

    # THEN only the valid station remains
    assert keys == [("S1",)]


@pytest.mark.parametrize("space", SPACE_KINDS)
@pytest.mark.parametrize("dim", BOTH)
def test_key_with_tab_or_ideographic_space_around_it_is_the_same_key_as_without(
    spark: SparkSession, dim: Dim, space: str
) -> None:
    # GIVEN a real row, and the same row again with whitespace around every key field
    clean = _item()
    padded = _padded(dim, dim.key, space, space)

    # WHEN they are turned into dimension rows
    keys = _keys(dim.build(_page(spark, clean, padded)), dim)

    # THEN there is one row, under the key without the whitespace
    assert keys == [tuple(clean[dim.api[k]] for k in dim.key)]


# --- stations: one row per station ------------------------------------------


def test_stations_have_one_row_per_station_on_a_real_page(spark: SparkSession) -> None:
    # GIVEN a real page where some stations have more than one charger
    stat_ids = [item["statId"] for item in INFO_ITEMS]
    assert len(set(stat_ids)) < len(stat_ids)

    # WHEN it is turned into station rows
    keys = _keys(stations(_page(spark, *INFO_ITEMS)), STATION)

    # THEN each station appears exactly once
    assert keys == sorted((s,) for s in set(stat_ids))


def test_station_attributes_come_from_the_row_with_the_smallest_charger_id(
    spark: SparkSession,
) -> None:
    # GIVEN the real two-charger station ME178009, whose rows disagree on station attributes,
    # with charger 02 first in input and on the later page
    first, second = INFO_ITEMS[3], INFO_ITEMS[4]
    assert [(i["statId"], i["chgerId"]) for i in (first, second)] == [
        ("ME178009", "01"),
        ("ME178009", "02"),
    ]
    rows = [
        (2, _like(second, statNm="from 02", lat="37.2")),
        (1, _like(first, statNm="from 01", lat="37.1")),
    ]

    # WHEN it is turned into station rows
    row = _only(stations(_raw(spark, rows)))

    # THEN the station takes charger 01's attributes
    assert (row["stat_id"], row["stat_nm"], row["lat"]) == ("ME178009", "from 01", 37.1)


# A chgerId of only U+3000 is left out: it sorts after "02" as text, so it could not catch a
# blank charger id winning the tie.
@pytest.mark.parametrize("blank", [pytest.param("", id="empty"), pytest.param(TAB, id="tab")])
def test_station_attributes_do_not_come_from_a_row_with_a_blank_charger_id(
    spark: SparkSession, blank: str
) -> None:
    # GIVEN a station with a real charger 02 and a row whose chgerId is blank
    # (an empty or tab-only string sorts before "02" as text, but it is no charger id)
    rows = [
        (1, _item(statId="S1", chgerId=blank, statNm="from blank")),
        (1, _item(statId="S1", chgerId="02", statNm="from 02")),
    ]

    # WHEN it is turned into station rows
    row = _only(stations(_raw(spark, rows)))

    # THEN the station takes charger 02's attributes
    assert row["stat_nm"] == "from 02"


# Each pair sorts the wrong way round as raw text: "　01" > "02" and "\t02" < "01".
PADDED_CHARGER_IDS = [
    pytest.param(f"{IDEOGRAPHIC_SPACE}01", "02", id="ideographic_space_before_the_smaller_id"),
    pytest.param("01", f"{TAB}02", id="tab_before_the_larger_id"),
]


@pytest.mark.parametrize(("smaller", "larger"), PADDED_CHARGER_IDS)
def test_station_attributes_come_from_the_smallest_charger_id_without_surrounding_whitespace(
    spark: SparkSession, smaller: str, larger: str
) -> None:
    # GIVEN a station with chargers 01 and 02, one of whose ids has whitespace in front
    rows = [
        (1, _item(statId="S1", chgerId=larger, statNm="from 02")),
        (1, _item(statId="S1", chgerId=smaller, statNm="from 01")),
    ]

    # WHEN it is turned into station rows
    row = _only(stations(_raw(spark, rows)))

    # THEN the station takes charger 01's attributes
    assert row["stat_nm"] == "from 01"


# --- chargers: one row per charger ------------------------------------------


def test_chargers_have_one_row_per_charger_on_a_real_page(spark: SparkSession) -> None:
    # GIVEN a real page of distinct chargers
    expected = sorted((item["statId"], item["chgerId"]) for item in INFO_ITEMS)

    # WHEN it is turned into charger rows
    keys = _keys(chargers(_page(spark, *INFO_ITEMS)), CHARGER)

    # THEN every charger appears exactly once
    assert keys == expected


@pytest.mark.parametrize(
    "later_first", [True, False], ids=["later_page_first_in_input", "later_page_last_in_input"]
)
def test_charger_seen_on_two_pages_keeps_the_later_page_row(
    spark: SparkSession, later_first: bool
) -> None:
    # GIVEN one charger returned on page 1 and again, changed, on page 2 of the same run
    earlier = (1, _item(output="50", maker="earlier"))
    later = (2, _item(output="100", maker="later"))
    rows = [later, earlier] if later_first else [earlier, later]

    # WHEN it is turned into charger rows
    row = _only(chargers(_raw(spark, rows)))

    # THEN the page 2 row is kept, regardless of input order
    assert (row["output_kw"], row["maker"]) == (100, "later")


# --- row_hash ---------------------------------------------------------------


@pytest.mark.parametrize("dim", BOTH)
def test_row_hash_is_the_same_across_runs_when_attributes_are_unchanged(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN the same charger in two daily runs, on a different page, with only status and
    # heartbeat fields changed
    today = [(1, _item())]
    tomorrow = [
        (
            3,
            _item(
                stat="3",
                statUpdDt="20261026090000",
                lastTsdt="20261026080000",
                lastTedt="",
                nowTsdt="20261026085500",
            ),
        )
    ]

    # WHEN each run is turned into a dimension row
    first = _hash(spark, dim, today)
    second = _hash(spark, dim, tomorrow, collected_at=NEXT_DAY)

    # THEN the hash is the same, so no new SCD2 version would be opened
    assert first == second


CHANGED_VALUE = {"lat": "35.1796", "lng": "129.0756", "output": "7", "year": "2020"}
ONE_ATTRIBUTE = [
    pytest.param(dim, column, id=f"{name}-{column}")
    for name, dim in (("station", STATION), ("charger", CHARGER))
    for column in dim.attrs
]


@pytest.mark.parametrize(("dim", "column"), ONE_ATTRIBUTE)
def test_row_hash_changes_when_any_one_attribute_changes(
    spark: SparkSession, dim: Dim, column: str
) -> None:
    # GIVEN a real row and a copy with exactly one attribute changed
    api = dim.api[column]
    before = [(1, _item())]
    after = [(1, _item(**{api: CHANGED_VALUE.get(api, "changed")}))]

    # WHEN each is turned into a dimension row
    old = _hash(spark, dim, before)
    new = _hash(spark, dim, after)

    # THEN the hashes differ
    assert old != new


NEIGHBOURS = [
    pytest.param(STATION, "addr", "addrDetail", id="station-addr-addr_detail"),
    pytest.param(CHARGER, "floorType", "floorNum", id="charger-floor_type-floor_num"),
]


@pytest.mark.parametrize(("dim", "first", "second"), NEIGHBOURS)
def test_row_hash_tells_which_of_two_neighbouring_columns_is_null(
    spark: SparkSession, dim: Dim, first: str, second: str
) -> None:
    # GIVEN two rows holding the same text, once in `first` and once in `second`, the other
    # field empty (stored as null). A hash that dropped nulls would see the same text for both.
    left = [(1, _item(**{first: "X", second: ""}))]
    right = [(1, _item(**{first: "", second: "X"}))]

    # WHEN each is turned into a dimension row
    left_hash = _hash(spark, dim, left)
    right_hash = _hash(spark, dim, right)

    # THEN the hashes differ: a null keeps its place in the hash
    assert left_hash != right_hash


@pytest.mark.parametrize(
    ("dim", "field"),
    [
        pytest.param(STATION, "note", id="station-note"),
        pytest.param(CHARGER, "delDetail", id="charger-del_detail"),
    ],
)
def test_row_hash_is_the_same_for_empty_whitespace_and_missing_values(
    spark: SparkSession, dim: Dim, field: str
) -> None:
    # GIVEN rows whose only difference is an empty, a whitespace-only (spaces, tab, ideographic
    # space), or a missing field
    variants: list[str | None] = ["", "   ", TAB, IDEOGRAPHIC_SPACE, None]

    # WHEN each is turned into a dimension row
    hashes = {_hash(spark, dim, [(1, _item(**{field: v}))]) for v in variants}

    # THEN all of them are stored as null, so they hash the same
    assert len(hashes) == 1


@pytest.mark.parametrize("space", SPACE_KINDS)
@pytest.mark.parametrize("dim", BOTH)
def test_row_hash_is_the_same_when_attributes_differ_only_by_surrounding_whitespace(
    spark: SparkSession, dim: Dim, space: str
) -> None:
    # GIVEN a real row today, and the same row tomorrow with whitespace around every attribute
    today = [(1, _item())]
    tomorrow = [(1, _padded(dim, dim.attrs, space, space))]

    # WHEN each run is turned into a dimension row
    first = _hash(spark, dim, today)
    second = _hash(spark, dim, tomorrow, collected_at=NEXT_DAY)

    # THEN the hash is the same, so no new SCD2 version would be opened
    assert first == second


SAME_STORED_VALUE = [
    pytest.param(CHARGER, "output_kw", "050", "50", id="charger-output-leading_zero"),
    pytest.param(STATION, "lat", "37.4762960", "37.476296", id="station-lat-trailing_zero"),
    pytest.param(CHARGER, "install_year", "unknown", "", id="charger-year-non_numeric_vs_empty"),
]


@pytest.mark.parametrize(("dim", "column", "text", "other"), SAME_STORED_VALUE)
def test_row_hash_is_the_same_when_raw_text_differs_but_the_stored_row_does_not(
    spark: SparkSession, dim: Dim, column: str, text: str, other: str
) -> None:
    # GIVEN two rows whose number field differs only in text form
    api = dim.api[column]
    first = _only(dim.build(_page(spark, _item(**{api: text}))))

    # WHEN each is turned into a dimension row
    second = _only(dim.build(_page(spark, _item(**{api: other}))))

    # THEN the stored attributes are identical, so the hashes are too
    # (otherwise load_dims would open a new version identical to the current one)
    assert {k: v for k, v in first.items() if k != "row_hash"} == {
        k: v for k, v in second.items() if k != "row_hash"
    }
    assert first["row_hash"] == second["row_hash"]


# --- determinism ------------------------------------------------------------


@pytest.mark.parametrize("dim", BOTH)
def test_output_does_not_depend_on_input_row_order(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a real page plus duplicates: one key repeated on the same page with other attributes,
    # another repeated on a later page
    rows = [(1, item) for item in INFO_ITEMS] + [
        (1, _like(INFO_ITEMS[0], statNm="same page variant", maker="same page variant")),
        (2, _like(INFO_ITEMS[9], statNm="later page variant", maker="later page variant")),
    ]

    # WHEN the rows are turned into dimension rows in input order and in reverse order
    forward = _rows(dim.build(_raw(spark, rows)))
    backward = _rows(dim.build(_raw(spark, rows[::-1])))

    # THEN both give the same rows and hashes
    assert forward == backward


@pytest.mark.parametrize("dim", BOTH)
def test_rows_repeated_within_a_run_give_the_same_output_as_once(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a real page, and the same page read twice within one run
    raw = _page(spark, *INFO_ITEMS)

    # WHEN both are turned into dimension rows
    once = _rows(dim.build(raw))
    twice = _rows(dim.build(raw.unionAll(raw)))

    # THEN the repeats collapse to the same rows
    assert twice == once


@pytest.mark.parametrize("dim", BOTH)
def test_building_twice_from_the_same_raw_gives_the_same_rows(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a real page
    raw = _page(spark, *INFO_ITEMS)

    # WHEN it is turned into dimension rows twice
    first = _rows(dim.build(raw))
    second = _rows(dim.build(raw))

    # THEN the results are identical
    assert first == second


# --- scd2_changes: dim table + new snapshot -> rows to MERGE ----------------

KST = timezone(timedelta(hours=9))
DAY_1, DAY_2, DAY_3 = COLLECTED_AT, NEXT_DAY, NEXT_DAY + timedelta(days=1)
RUN_1, RUN_2, RUN_3 = (
    run_key_from_run_id(f"scheduled__{day.isoformat()}") for day in (DAY_1, DAY_2, DAY_3)
)
# valid_to of a current version: 9999-12-31 in the session time zone (Asia/Seoul, as in prod)
OPEN_END = datetime(9999, 12, 31, tzinfo=KST)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
SCD2_TIMES = ("valid_from", "valid_to")

# Real one-charger stations, so each item is one key in both dimensions.
A, B, C, D = INFO_ITEMS[0], INFO_ITEMS[1], INFO_ITEMS[2], INFO_ITEMS[5]
# A with a station and a charger attribute changed, so it is a new version in either dimension.
A_CHANGED = _like(A, statNm="renamed station", maker="replaced maker")
A_CHANGED_AGAIN = _like(A, statNm="renamed again", maker="replaced again")

Load = tuple[datetime, str, Sequence[Item]]  # one full run: (collected_at, run_key, items)


def _empty_table(spark: SparkSession, dim: Dim) -> DataFrame:
    """The dim table with its Iceberg columns and no rows, as before the first load."""
    ddl = ", ".join(f"`{c.name}` {c.type}" for c in dim.table.columns)
    return spark.createDataFrame([], ddl)


def _snapshot(spark: SparkSession, dim: Dim, items: Sequence[Item]) -> DataFrame:
    return dim.build(_page(spark, *items))


def _changes(
    dim: Dim, current: DataFrame, incoming: DataFrame, at: datetime, run_key: str
) -> DataFrame:
    return scd2_changes(current, incoming, dim.key, collected_at=at, run_key=run_key)


def _version_id(dim: Dim, row: Any) -> tuple[Any, ...]:
    return (*(row[k] for k in dim.key), row["valid_from"])


def _merged(spark: SparkSession, dim: Dim, table: DataFrame, changes: DataFrame) -> DataFrame:
    """The table after load_dims' MERGE on (key, valid_from): a change row replaces the version
    it matches or is inserted. Rebuilt from collected rows, like reading the table back."""
    rows = {_version_id(dim, r): r for r in table.collect()}
    source = changes.collect()
    # MERGE fails when two source rows match one target row
    assert len({_version_id(dim, r) for r in source}) == len(source), source
    rows.update({_version_id(dim, r): r for r in source})
    data = [tuple(r[c] for c in table.columns) for r in rows.values()]
    return spark.createDataFrame(data, table.schema)


def _table(spark: SparkSession, dim: Dim, *loads: Load) -> DataFrame:
    """The dim table after load_dims applied each full run in order, starting empty."""
    table = _empty_table(spark, dim)
    for at, run_key, items in loads:
        changes = _changes(dim, table, _snapshot(spark, dim, items), at, run_key)
        table = _merged(spark, dim, table, changes)
    return table


def _order(row: Row) -> tuple[Any, ...]:
    return (row["stat_id"], row.get("charger_id") or "", row["valid_from"], row["is_current"])


def _versions(df: DataFrame) -> list[Row]:
    """Collected rows with valid_from / valid_to as UTC datetimes (collect() alone gives naive
    local time), sorted by key, then valid_from, closed before current."""
    as_micros = df.select(
        *[F.unix_micros(c).alias(c) if c in SCD2_TIMES else F.col(c) for c in df.columns]
    )
    rows = []
    for collected in as_micros.collect():
        row = collected.asDict()
        row.update({c: EPOCH + timedelta(microseconds=row[c]) for c in SCD2_TIMES})
        rows.append(row)
    return sorted(rows, key=_order)


def _key(dim: Dim, row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row[k] for k in dim.key)


def _item_key(dim: Dim, item: Item) -> tuple[Any, ...]:
    return tuple(item[dim.api[k]] for k in dim.key)


def _current_of(dim: Dim, table: DataFrame, item: Item) -> Row:
    """The current version of the item's key in the table."""
    rows = [r for r in _versions(table) if r["is_current"] and _key(dim, r) == _item_key(dim, item)]
    assert len(rows) == 1, rows
    return rows[0]


def _incoming_of(dim: Dim, incoming: DataFrame, item: Item) -> Row:
    """The snapshot row of the item's key."""
    rows = [r for r in _rows(incoming) if _key(dim, r) == _item_key(dim, item)]
    assert len(rows) == 1, rows
    return rows[0]


def _closed(version: Row, at: datetime) -> Row:
    """A current version as the closing row should carry it: only valid_to and is_current change."""
    return {**version, "valid_to": at, "is_current": False}


def _opened(incoming: Row, at: datetime, run_key: str) -> Row:
    """A snapshot row as the new current version should carry it."""
    return {
        **incoming,
        "valid_from": at,
        "valid_to": OPEN_END,
        "is_current": True,
        "source_run_key": run_key,
    }


# --- scd2_changes: nothing changed ------------------------------------------


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_snapshot_with_only_status_and_heartbeat_changes_gives_no_rows(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN the table loaded from a real page, and the next day's run of the same chargers with
    # only status and heartbeat fields changed (statUpdDt moved, lastTedt and nowTsdt empty)
    table = _table(spark, dim, (DAY_1, RUN_1, INFO_ITEMS))
    heartbeat = [
        _like(item, stat="3", statUpdDt="20261110181000", lastTedt="", nowTsdt="")
        for item in INFO_ITEMS
    ]

    # WHEN the next day's snapshot is compared with the table
    changes = _changes(dim, table, _snapshot(spark, dim, heartbeat), DAY_2, RUN_2)

    # THEN there is nothing to merge
    assert changes.count() == 0


# --- scd2_changes: changed, new and gone keys -------------------------------


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_changed_key_closes_its_current_version_changing_only_valid_to_and_is_current(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a table holding A and B, and a snapshot in which A's attributes changed
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B]))
    before = _current_of(dim, table, A)
    incoming = _snapshot(spark, dim, [A_CHANGED, B])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, incoming, DAY_2, RUN_2))

    # THEN A's current version comes back with its attributes, row_hash, valid_from and
    # source_run_key as they were, closed at the new run's collected_at
    assert [r for r in result if not r["is_current"]] == [_closed(before, DAY_2)]


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_changed_key_opens_a_new_version_with_the_snapshot_attributes(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a table holding A and B, and a snapshot in which A's attributes changed
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B]))
    incoming = _snapshot(spark, dim, [A_CHANGED, B])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, incoming, DAY_2, RUN_2))

    # THEN A gets a current version carrying the snapshot row, valid from the new run's
    # collected_at to the open end, made by the new run
    expected = _opened(_incoming_of(dim, incoming, A_CHANGED), DAY_2, RUN_2)
    assert [r for r in result if r["is_current"]] == [expected]


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_new_key_opens_a_version_and_closes_nothing(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a table holding A, and a snapshot with A unchanged and B added
    table = _table(spark, dim, (DAY_1, RUN_1, [A]))
    incoming = _snapshot(spark, dim, [A, B])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, incoming, DAY_2, RUN_2))

    # THEN the only row is B's first version
    assert result == [_opened(_incoming_of(dim, incoming, B), DAY_2, RUN_2)]


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_gone_key_closes_its_current_version_unchanged_and_opens_nothing(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a table holding A and B, and a snapshot with A unchanged and B missing
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B]))
    before = _current_of(dim, table, B)

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, _snapshot(spark, dim, [A]), DAY_2, RUN_2))

    # THEN the only row is B's current version, closed with every other column as it was
    assert result == [_closed(before, DAY_2)]


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_gives_rows_only_for_changed_new_and_gone_keys(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a table holding A, B and C, and a snapshot where A changed, B is gone, C is unchanged
    # and D is new
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B, C]))
    incoming = _snapshot(spark, dim, [A_CHANGED, C, D])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, incoming, DAY_2, RUN_2))

    # THEN A is closed and reopened, B closed, D opened, and C does not appear
    expected = [
        (*_item_key(dim, A), False),
        (*_item_key(dim, A), True),
        (*_item_key(dim, B), False),
        (*_item_key(dim, D), True),
    ]
    assert sorted((*_key(dim, r), r["is_current"]) for r in result) == sorted(expected)


MOVED_CHARGERS = [
    pytest.param({"chgerId": "02"}, id="same_station_other_charger_id"),
    pytest.param({"statId": "ME999999"}, id="same_charger_id_other_station"),
]


@pytest.mark.parametrize("other_key", MOVED_CHARGERS)
def test_scd2_charger_sharing_only_part_of_the_key_is_another_charger(
    spark: SparkSession, other_key: dict[str, str]
) -> None:
    # GIVEN a table holding charger A, and a snapshot whose only charger has A's attributes
    # (so the same row_hash) but differs from A in one key column
    table = _table(spark, CHARGER, (DAY_1, RUN_1, [A]))
    before = _current_of(CHARGER, table, A)
    other = _like(A, **other_key)
    incoming = _snapshot(spark, CHARGER, [other])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(CHARGER, table, incoming, DAY_2, RUN_2))

    # THEN A is closed and the other charger opened, instead of matching A as unchanged
    expected = [
        _closed(before, DAY_2),
        _opened(_incoming_of(CHARGER, incoming, other), DAY_2, RUN_2),
    ]
    assert result == sorted(expected, key=_order)


@pytest.mark.parametrize(("dim", "column"), TEXT_ATTRIBUTES)
def test_scd2_attribute_that_becomes_blank_opens_a_version_holding_null(
    spark: SparkSession, dim: Dim, column: str
) -> None:
    # GIVEN a table holding A, and a snapshot where one of A's text fields is now empty
    table = _table(spark, dim, (DAY_1, RUN_1, [A]))
    blanked = _like(A, **{dim.api[column]: ""})

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, _snapshot(spark, dim, [blanked]), DAY_2, RUN_2))

    # THEN the old version keeps its value and the new current version holds null
    assert [(r["is_current"], r[column]) for r in result] == [
        (False, A[dim.api[column]]),
        (True, None),
    ]


# --- scd2_changes: closed versions are history ------------------------------


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_closed_version_whose_hash_differs_from_the_snapshot_is_not_closed_again(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A loaded on day 1 and changed on day 2, so the table holds A's closed day-1 version
    # (row_hash of A) and its current day-2 version (row_hash of A_CHANGED)
    table = _table(spark, dim, (DAY_1, RUN_1, [A]), (DAY_2, RUN_2, [A_CHANGED]))

    # WHEN day 3 brings the day-2 attributes again
    changes = _changes(dim, table, _snapshot(spark, dim, [A_CHANGED]), DAY_3, RUN_3)

    # THEN there is nothing to merge: the day-1 version is left as it is
    assert changes.count() == 0


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_change_after_a_closed_version_touches_only_the_current_version(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A loaded on day 1 and changed on day 2 (day-1 version closed, day-2 version current),
    # and a day-3 snapshot changing A once more
    table = _table(spark, dim, (DAY_1, RUN_1, [A]), (DAY_2, RUN_2, [A_CHANGED]))
    current = _current_of(dim, table, A)
    incoming = _snapshot(spark, dim, [A_CHANGED_AGAIN])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, incoming, DAY_3, RUN_3))

    # THEN the day-2 version is closed and a day-3 version opened; the day-1 version is not
    # in the result, so the MERGE leaves it alone
    assert result == [
        _closed(current, DAY_3),
        _opened(_incoming_of(dim, incoming, A_CHANGED_AGAIN), DAY_3, RUN_3),
    ]


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_reverting_to_closed_attributes_opens_a_new_version_and_leaves_the_old_one(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A loaded on day 1 and changed on day 2, and a day-3 snapshot bringing back A's day-1
    # attributes (the same row_hash as the closed day-1 version)
    table = _table(spark, dim, (DAY_1, RUN_1, [A]), (DAY_2, RUN_2, [A_CHANGED]))
    current = _current_of(dim, table, A)
    incoming = _snapshot(spark, dim, [A])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, incoming, DAY_3, RUN_3))

    # THEN the day-2 version is closed and a new day-3 version opened; the day-1 version is
    # not reopened
    assert result == [
        _closed(current, DAY_3),
        _opened(_incoming_of(dim, incoming, A), DAY_3, RUN_3),
    ]


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_key_that_comes_back_after_being_closed_opens_a_new_version_only(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN B loaded on day 1 and gone on day 2 (its only version closed), and a day-3 snapshot
    # where B is back unchanged
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B]), (DAY_2, RUN_2, [A]))
    incoming = _snapshot(spark, dim, [A, B])

    # WHEN the snapshot is compared with the table
    result = _versions(_changes(dim, table, incoming, DAY_3, RUN_3))

    # THEN the only row is a new day-3 version of B; its closed day-1 version is not touched
    assert result == [_opened(_incoming_of(dim, incoming, B), DAY_3, RUN_3)]


# --- scd2_changes: idempotency ----------------------------------------------

RERUNS = [
    pytest.param((), (DAY_1, RUN_1, INFO_ITEMS), id="first_load_of_a_real_page"),
    pytest.param(
        ((DAY_1, RUN_1, [A, B, C]),), (DAY_2, RUN_2, [A_CHANGED, C, D]), id="changed_new_and_gone"
    ),
    pytest.param(
        ((DAY_1, RUN_1, [A]), (DAY_2, RUN_2, [A_CHANGED])),
        (DAY_3, RUN_3, [A]),
        id="revert_over_a_closed_version",
    ),
]


@pytest.mark.parametrize(("history", "load"), RERUNS)
@pytest.mark.parametrize("dim", BOTH)
def test_scd2_same_run_again_after_merging_its_result_gives_no_rows(
    spark: SparkSession, dim: Dim, history: tuple[Load, ...], load: Load
) -> None:
    # GIVEN a table into which a run's result was merged
    at, run_key, items = load
    table = _table(spark, dim, *history, load)

    # WHEN the same run is compared again (load_dims rerun before its watermark moved)
    changes = _changes(dim, table, _snapshot(spark, dim, items), at, run_key)

    # THEN there is nothing to merge
    assert changes.count() == 0


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_gives_the_same_rows_when_called_twice_on_the_same_input(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a table holding A, B and C, and a snapshot with changed, new and gone keys
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B, C]))
    incoming = _snapshot(spark, dim, [A_CHANGED, C, D])

    # WHEN the changes are computed twice
    first = _versions(_changes(dim, table, incoming, DAY_2, RUN_2))
    second = _versions(_changes(dim, table, incoming, DAY_2, RUN_2))

    # THEN both give the same rows
    assert first == second


# --- scd2_changes: empty inputs ---------------------------------------------


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_first_load_into_an_empty_table_opens_every_snapshot_row(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN an empty table and a real page
    incoming = _snapshot(spark, dim, INFO_ITEMS)

    # WHEN the page is compared with the table
    result = _versions(_changes(dim, _empty_table(spark, dim), incoming, DAY_1, RUN_1))

    # THEN every snapshot row becomes a current version made by this run
    expected = [_opened(r, DAY_1, RUN_1) for r in _rows(incoming)]
    assert result == sorted(expected, key=_order)


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_empty_table_and_empty_snapshot_give_no_rows_with_the_table_columns(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN an empty table and a run with no rows
    table = _empty_table(spark, dim)

    # WHEN the empty snapshot is compared with the table
    changes = _changes(dim, table, _snapshot(spark, dim, []), DAY_1, RUN_1)

    # THEN there are no rows, but the columns are still the table's
    assert (changes.columns, changes.count()) == (table.columns, 0)


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_empty_snapshot_closes_every_current_version(spark: SparkSession, dim: Dim) -> None:
    # GIVEN a table loaded from a real page, and a run with no rows
    # (load_dims must not apply such a run; this pins what scd2_changes itself does with it)
    table = _table(spark, dim, (DAY_1, RUN_1, INFO_ITEMS))

    # WHEN the empty snapshot is compared with the table
    result = _versions(_changes(dim, table, _snapshot(spark, dim, []), DAY_2, RUN_2))

    # THEN every current version is closed and nothing is opened
    assert result == [_closed(r, DAY_2) for r in _versions(table)]


# --- scd2_changes: output schema --------------------------------------------

COLUMN_ORDERS = [pytest.param(False, id="table_order"), pytest.param(True, id="reversed_order")]


@pytest.mark.parametrize("reverse", COLUMN_ORDERS)
@pytest.mark.parametrize("dim", BOTH)
def test_scd2_result_columns_are_the_current_columns_in_the_same_order(
    spark: SparkSession, dim: Dim, reverse: bool
) -> None:
    # GIVEN a table holding A, B and C, read with its columns in table order or reversed, and a
    # snapshot that closes and opens versions
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B, C]))
    current = table.select(*reversed(table.columns)) if reverse else table

    # WHEN the snapshot is compared with the table
    changes = _changes(dim, current, _snapshot(spark, dim, [A_CHANGED, C, D]), DAY_2, RUN_2)

    # THEN the result has current's columns, in current's order
    assert changes.columns == current.columns


@pytest.mark.parametrize("dim", BOTH)
def test_scd2_result_column_types_are_the_current_column_types(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN a table holding A, B and C, and a snapshot that closes and opens versions
    table = _table(spark, dim, (DAY_1, RUN_1, [A, B, C]))

    # WHEN the snapshot is compared with the table
    changes = _changes(dim, table, _snapshot(spark, dim, [A_CHANGED, C, D]), DAY_2, RUN_2)

    # THEN every column has the table's type
    assert changes.dtypes == table.dtypes
