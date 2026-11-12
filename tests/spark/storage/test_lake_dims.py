"""Spark tests for eonje.storage.lake.dims: merge_changes applies scd2_changes' rows to a real
Iceberg dim table, and current_rows counts the table's current versions.

Every test creates core.dim_station or core.dim_charger with create_table_sql in the throwaway
`lake` catalog of tests/spark/conftest.py and drops it when the test ends. A load is what load_dims
does for one full run: read the table, compare it with the run's snapshot (scd2_changes), merge the
result. Snapshots are built from the real getChargerInfo page in tests/fixtures/evcharger.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from eonje.storage.lake import dims as lake_dims
from eonje.storage.lake.iceberg import DIM_CHARGER, DIM_STATION, IcebergTable, create_table_sql
from eonje.storage.lake.spark import CATALOG
from eonje.storage.raw.paths import run_key_from_run_id
from eonje.transform.dims import chargers, scd2_changes, stations

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"

KST = timezone(timedelta(hours=9))
DAY_1 = datetime(2026, 11, 9, 18, 11, tzinfo=UTC)
DAY_2, DAY_3 = DAY_1 + timedelta(days=1), DAY_1 + timedelta(days=2)
RUN_1, RUN_2, RUN_3 = (
    run_key_from_run_id(f"scheduled__{day.isoformat()}") for day in (DAY_1, DAY_2, DAY_3)
)
# valid_to of a current version: 9999-12-31 in the session time zone (Asia/Seoul, as in prod)
OPEN_END = datetime(9999, 12, 31, tzinfo=KST)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
SCD2_TIMES = ("valid_from", "valid_to")
KEY_API = {"stat_id": "statId", "charger_id": "chgerId"}

Item = Mapping[str, str]
Row = dict[str, Any]
Load = tuple[datetime, str, Sequence[Item]]  # one full run: (collected_at, run_key, items)


@dataclass(frozen=True)
class Dim:
    build: Callable[[DataFrame], DataFrame]
    table: IcebergTable
    key: tuple[str, ...]


# Keys restated from design.md: dim_station by stat_id, dim_charger by stat_id + charger_id.
STATION = Dim(stations, DIM_STATION, ("stat_id",))
CHARGER = Dim(chargers, DIM_CHARGER, ("stat_id", "charger_id"))


def _load_items() -> list[dict[str, str]]:
    body = json.loads((FIXTURES / "real_info_page.json").read_text(encoding="utf-8"))
    return list(body["items"]["item"])


# 10 real chargers at 8 stations: ME178009 has chargers 01 and 02, ME181150 has 12 and 13.
INFO_ITEMS = _load_items()
# Real one-charger stations, so each item is one key in both dimensions.
A, B, C, D = INFO_ITEMS[0], INFO_ITEMS[1], INFO_ITEMS[2], INFO_ITEMS[5]
# A with a station and a charger attribute changed, so it is a new version in either dimension.
A_CHANGED = {**A, "statNm": "renamed station", "maker": "replaced maker"}
A_CHANGED_AGAIN = {**A, "statNm": "renamed again", "maker": "replaced again"}
# The real page without A and B.
REST = INFO_ITEMS[2:]


@pytest.fixture(params=[pytest.param(STATION, id="station"), pytest.param(CHARGER, id="charger")])
def dim(spark: SparkSession, request: pytest.FixtureRequest) -> Iterator[Dim]:
    """The dim table, created empty with create_table_sql and dropped when the test ends."""
    created: Dim = request.param
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{created.table.namespace}")
    spark.sql(create_table_sql(created.table))
    yield created
    spark.sql(f"DROP TABLE IF EXISTS {created.table.full_name}")


def _page(spark: SparkSession, items: Sequence[Item], collected_at: datetime) -> DataFrame:
    """A raw charger_info page as the collector writes it: API fields as strings plus meta."""
    fields = list(dict.fromkeys(f for item in (INFO_ITEMS[0], *items) for f in item))
    schema = StructType(
        [StructField(f, StringType()) for f in fields]
        + [
            StructField("collected_at", TimestampType(), nullable=False),
            StructField("page_fetched_at", TimestampType(), nullable=False),
            StructField("page_no", IntegerType(), nullable=False),
        ]
    )
    data = [(*(item.get(f) for f in fields), collected_at, collected_at, 1) for item in items]
    return spark.createDataFrame(data, schema)


def _snapshot(
    spark: SparkSession, dim: Dim, items: Sequence[Item], collected_at: datetime = DAY_1
) -> DataFrame:
    return dim.build(_page(spark, items, collected_at))


def _changes(spark: SparkSession, dim: Dim, load: Load) -> DataFrame:
    """What load_dims merges for a run: the table as it is now compared with the run's snapshot."""
    at, run_key, items = load
    current = spark.table(dim.table.full_name)
    incoming = _snapshot(spark, dim, items, at)
    return scd2_changes(current, incoming, dim.key, collected_at=at, run_key=run_key)


def _load(spark: SparkSession, dim: Dim, *loads: Load) -> None:
    """load_dims applying each full run in order."""
    for load in loads:
        lake_dims.merge_changes(spark, dim.table, dim.key, _changes(spark, dim, load))


def _order(row: Row) -> tuple[Any, ...]:
    return (row["stat_id"], row.get("charger_id") or "", row["valid_from"])


def _versions(spark: SparkSession, dim: Dim) -> list[Row]:
    """The table read back with valid_from / valid_to as UTC datetimes (collect() alone gives
    naive local time), sorted by key, then valid_from."""
    table = spark.table(dim.table.full_name)
    as_micros = table.select(
        *[F.unix_micros(c).alias(c) if c in SCD2_TIMES else F.col(c) for c in table.columns]
    )
    rows = []
    for collected in as_micros.collect():
        row = collected.asDict()
        row.update({c: EPOCH + timedelta(microseconds=row[c]) for c in SCD2_TIMES})
        rows.append(row)
    return sorted(rows, key=_order)


def _key(dim: Dim, row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row[k] for k in dim.key)


def _item_key(dim: Dim, item: Item) -> tuple[str, ...]:
    return tuple(item[KEY_API[k]] for k in dim.key)


def _versions_of(spark: SparkSession, dim: Dim, item: Item) -> list[Row]:
    """Every version of the item's key in the table, oldest first."""
    return [r for r in _versions(spark, dim) if _key(dim, r) == _item_key(dim, item)]


def _current_keys(spark: SparkSession, dim: Dim) -> list[tuple[Any, ...]]:
    """The key of every current version in the table, one entry per row."""
    return sorted(_key(dim, r) for r in _versions(spark, dim) if r["is_current"])


def _snapshot_row(spark: SparkSession, dim: Dim, item: Item) -> Row:
    """The dimension row the transform builds from one item."""
    rows = [r.asDict() for r in _snapshot(spark, dim, [item]).collect()]
    assert len(rows) == 1, rows
    return rows[0]


def _closed(version: Row, at: datetime) -> Row:
    """A version after it was closed: only valid_to and is_current change."""
    return {**version, "valid_to": at, "is_current": False}


def _opened(snapshot_row: Row, at: datetime, run_key: str) -> Row:
    """A snapshot row as the current version a run opens for it."""
    return {
        **snapshot_row,
        "valid_from": at,
        "valid_to": OPEN_END,
        "is_current": True,
        "source_run_key": run_key,
    }


def _commits(spark: SparkSession, dim: Dim) -> int:
    return spark.table(f"{dim.table.full_name}.snapshots").count()


# --- first load -------------------------------------------------------------


def test_first_snapshot_into_an_empty_table_inserts_every_row_as_a_current_version(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN the newly created, empty dim table and a real getChargerInfo page

    # WHEN the page is loaded
    _load(spark, dim, (DAY_1, RUN_1, INFO_ITEMS))

    # THEN the table holds exactly the snapshot rows, each current from the run's collected_at
    # to the open end and made by this run
    snapshot = [r.asDict() for r in _snapshot(spark, dim, INFO_ITEMS).collect()]
    expected = [_opened(r, DAY_1, RUN_1) for r in snapshot]
    assert _versions(spark, dim) == sorted(expected, key=_order)


# --- changed attributes -----------------------------------------------------


def test_changed_attributes_close_the_old_version_at_the_new_runs_collected_at(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A and B loaded on day 1
    _load(spark, dim, (DAY_1, RUN_1, [A, B]))
    [before] = _versions_of(spark, dim, A)

    # WHEN day 2 brings A with changed attributes
    _load(spark, dim, (DAY_2, RUN_2, [A_CHANGED, B]))

    # THEN A's day-1 version is still in the table with its attributes, row_hash, valid_from and
    # source_run_key as they were, closed at day 2's collected_at
    closed = [r for r in _versions_of(spark, dim, A) if not r["is_current"]]
    assert closed == [_closed(before, DAY_2)]


def test_changed_attributes_open_one_new_current_version_with_the_snapshot_attributes(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A and B loaded on day 1
    _load(spark, dim, (DAY_1, RUN_1, [A, B]))

    # WHEN day 2 brings A with changed attributes
    _load(spark, dim, (DAY_2, RUN_2, [A_CHANGED, B]))

    # THEN A has exactly one current version: day 2's snapshot row, valid from day 2's
    # collected_at to the open end, made by day 2's run
    current = [r for r in _versions_of(spark, dim, A) if r["is_current"]]
    assert current == [_opened(_snapshot_row(spark, dim, A_CHANGED), DAY_2, RUN_2)]


def test_unchanged_key_keeps_its_version_untouched_when_another_key_changes(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A and B loaded on day 1
    _load(spark, dim, (DAY_1, RUN_1, [A, B]))
    before = _versions_of(spark, dim, B)

    # WHEN day 2 changes A and brings B as it was
    _load(spark, dim, (DAY_2, RUN_2, [A_CHANGED, B]))

    # THEN B still has only its day-1 version, every column as it was
    assert _versions_of(spark, dim, B) == before


def test_second_change_closes_only_the_current_version_and_leaves_older_versions_as_they_were(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A loaded on day 1 and changed on day 2: its day-1 version closed at day 2, its day-2
    # version current
    _load(spark, dim, (DAY_1, RUN_1, [A]), (DAY_2, RUN_2, [A_CHANGED]))
    day_1, day_2 = _versions_of(spark, dim, A)

    # WHEN day 3 changes A again
    _load(spark, dim, (DAY_3, RUN_3, [A_CHANGED_AGAIN]))

    # THEN the day-1 version keeps valid_to = day 2, the day-2 version is closed at day 3, and
    # a day-3 version is current
    assert _versions_of(spark, dim, A) == [
        day_1,
        _closed(day_2, DAY_3),
        _opened(_snapshot_row(spark, dim, A_CHANGED_AGAIN), DAY_3, RUN_3),
    ]


CHURN: tuple[Load, ...] = (
    (DAY_1, RUN_1, INFO_ITEMS),
    (DAY_2, RUN_2, [A_CHANGED, *REST]),  # A changed, B gone
    (DAY_3, RUN_3, [A_CHANGED_AGAIN, B, *REST]),  # A changed again, B back
)


def test_every_key_of_the_latest_snapshot_has_exactly_one_current_version_after_each_load(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN three days of real full runs in which A changes twice and B leaves and comes back
    observed = []
    expected = []

    # WHEN the runs are loaded one after another
    for load in CHURN:
        _load(spark, dim, load)
        observed.append(_current_keys(spark, dim))
        expected.append(sorted({_item_key(dim, item) for item in load[2]}))

    # THEN after every load the current versions are exactly one per key of that run's snapshot
    assert observed == expected


# --- gone and back ----------------------------------------------------------


def test_gone_key_is_only_closed(spark: SparkSession, dim: Dim) -> None:
    # GIVEN A and B loaded on day 1
    _load(spark, dim, (DAY_1, RUN_1, [A, B]))
    [before] = _versions_of(spark, dim, B)

    # WHEN day 2's snapshot no longer has B
    _load(spark, dim, (DAY_2, RUN_2, [A]))

    # THEN B's only version is closed at day 2 with every other column as it was, and no new
    # version is opened for it
    assert _versions_of(spark, dim, B) == [_closed(before, DAY_2)]


def test_key_that_comes_back_opens_a_new_version_and_leaves_the_closed_one(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN B loaded on day 1 and gone on day 2, so its only version is closed
    _load(spark, dim, (DAY_1, RUN_1, [A, B]), (DAY_2, RUN_2, [A]))
    closed = _versions_of(spark, dim, B)

    # WHEN day 3 brings B back unchanged
    _load(spark, dim, (DAY_3, RUN_3, [A, B]))

    # THEN B keeps its closed day-1 version and gets a new current version made by day 3
    assert _versions_of(spark, dim, B) == [
        *closed,
        _opened(_snapshot_row(spark, dim, B), DAY_3, RUN_3),
    ]


# --- idempotency ------------------------------------------------------------

RERUNS = [
    pytest.param((), (DAY_1, RUN_1, INFO_ITEMS), id="first_load_of_a_real_page"),
    pytest.param(
        ((DAY_1, RUN_1, [A, B, C]),), (DAY_2, RUN_2, [A_CHANGED, C, D]), id="changed_new_and_gone"
    ),
    pytest.param(
        ((DAY_1, RUN_1, [A, B]), (DAY_2, RUN_2, [A])), (DAY_3, RUN_3, [A, B]), id="back_after_gone"
    ),
]


@pytest.mark.parametrize(("history", "load"), RERUNS)
def test_loading_the_same_run_twice_leaves_the_table_as_after_the_first_load(
    spark: SparkSession, dim: Dim, history: tuple[Load, ...], load: Load
) -> None:
    # GIVEN a table after its history and one more run
    _load(spark, dim, *history, load)
    after_first = _versions(spark, dim)

    # WHEN the same run is loaded again (load_dims rerun before its watermark moved)
    _load(spark, dim, load)

    # THEN the table is unchanged
    assert _versions(spark, dim) == after_first


def test_next_run_with_only_status_and_heartbeat_changes_leaves_the_table_unchanged(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN the real page loaded on day 1, and day 2's page of the same chargers with only status
    # and heartbeat fields changed (statUpdDt moved, lastTedt and nowTsdt empty)
    _load(spark, dim, (DAY_1, RUN_1, INFO_ITEMS))
    before = _versions(spark, dim)
    heartbeat = [
        {**item, "stat": "3", "statUpdDt": "20261110181000", "lastTedt": "", "nowTsdt": ""}
        for item in INFO_ITEMS
    ]

    # WHEN day 2's page is loaded
    _load(spark, dim, (DAY_2, RUN_2, heartbeat))

    # THEN no version is opened or closed: valid_from and source_run_key still point at day 1
    assert _versions(spark, dim) == before


def test_merging_the_same_change_rows_twice_leaves_the_table_as_after_one_merge(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A, B and C loaded on day 1, and the change rows of a day-2 run that changes A, drops B
    # and adds D, computed once and kept (checkpointed, so they do not follow the table)
    _load(spark, dim, (DAY_1, RUN_1, [A, B, C]))
    changes = _changes(spark, dim, (DAY_2, RUN_2, [A_CHANGED, C, D])).localCheckpoint()
    lake_dims.merge_changes(spark, dim.table, dim.key, changes)
    after_one = _versions(spark, dim)

    # WHEN the same change rows are merged again
    lake_dims.merge_changes(spark, dim.table, dim.key, changes)

    # THEN the table is unchanged: the opened versions match themselves on (key, valid_from)
    # instead of being inserted a second time
    assert _versions(spark, dim) == after_one


# --- commit -----------------------------------------------------------------


def test_a_load_that_closes_and_opens_versions_is_one_iceberg_commit(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN A, B and C loaded on day 1
    _load(spark, dim, (DAY_1, RUN_1, [A, B, C]))
    before = _commits(spark, dim)

    # WHEN day 2 changes A, drops B and adds D (rows both updated and inserted)
    _load(spark, dim, (DAY_2, RUN_2, [A_CHANGED, C, D]))

    # THEN the table has exactly one more snapshot
    assert _commits(spark, dim) == before + 1


# --- current_rows -----------------------------------------------------------


def test_current_rows_of_an_empty_table_is_zero(spark: SparkSession, dim: Dim) -> None:
    # GIVEN the newly created, empty dim table

    # WHEN its current versions are counted
    count = lake_dims.current_rows(spark, dim.table)

    # THEN there are none
    assert count == 0


def test_current_rows_after_the_first_load_is_one_per_key_on_the_page(
    spark: SparkSession, dim: Dim
) -> None:
    # GIVEN the real page (10 chargers at 8 stations) loaded into the empty table
    _load(spark, dim, (DAY_1, RUN_1, INFO_ITEMS))

    # WHEN the current versions are counted
    count = lake_dims.current_rows(spark, dim.table)

    # THEN the count is the number of keys on the page
    assert count == len({_item_key(dim, item) for item in INFO_ITEMS})


def test_current_rows_does_not_count_closed_versions(spark: SparkSession, dim: Dim) -> None:
    # GIVEN the real page loaded on day 1, then day 2's page in which A changed (A now has a
    # closed and a current version) and B is gone (B has only a closed version)
    day_2 = [A_CHANGED, *REST]
    _load(spark, dim, (DAY_1, RUN_1, INFO_ITEMS), (DAY_2, RUN_2, day_2))

    # WHEN the current versions are counted
    count = lake_dims.current_rows(spark, dim.table)

    # THEN only day 2's keys count, once each
    assert count == len({_item_key(dim, item) for item in day_2})
