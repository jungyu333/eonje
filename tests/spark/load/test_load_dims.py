"""Spark tests for eonje.load.dims.load_dims: completed charger_info runs after the watermark are
applied oldest first to core.dim_station and core.dim_charger; runs that skip_reason rejects are
reported, never read, and leave the tables and the watermark alone; the watermark advances to a run
only after both dimensions hold it.

Both dim tables are created empty with create_table_sql in the throwaway `lake` catalog of
tests/spark/conftest.py and dropped when the test ends. read_raw is a fake (RawRuns) that serves
each run's page built from the real getChargerInfo items in tests/fixtures/evcharger and records
the runs it read; advance records the watermarks it was given.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from eonje.client.paging import page_count
from eonje.load.dims import RunResult, load_dims
from eonje.storage.lake.dims import MergeCounts
from eonje.storage.lake.iceberg import DIM_CHARGER, DIM_STATION, IcebergTable, create_table_sql
from eonje.storage.lake.spark import CATALOG
from eonje.storage.postgres.watermark import Watermark
from eonje.storage.raw.paths import Dataset, partition_date, run_key_from_run_id
from eonje.storage.raw.run import MANIFEST_VERSION, RunManifest
from eonje.timeutil import KST

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"

DAY_1 = datetime(2026, 11, 9, 18, 11, tzinfo=UTC)
DAY_2 = DAY_1 + timedelta(days=1)
RUN_1, RUN_2 = (run_key_from_run_id(f"scheduled__{day.isoformat()}") for day in (DAY_1, DAY_2))
# A manual run with day 2's collected_at: "manual__" sorts before "scheduled__", so before RUN_2.
MANUAL_2 = run_key_from_run_id(f"manual__{DAY_2.isoformat()}")

PAGE_SIZE = 9999
RATIO = 0.9  # the Settings default for master_min_row_ratio
# valid_to of a current version: 9999-12-31 in the session time zone (Asia/Seoul, as in prod)
OPEN_END = datetime(9999, 12, 31, tzinfo=KST)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
SCD2_TIMES = ("valid_from", "valid_to")

Item = Mapping[str, str]
Row = dict[str, Any]
Current = tuple[tuple[Any, ...], datetime, str]  # (key, valid_from, source_run_key)
Version = tuple[datetime, datetime, bool, str]  # (valid_from, valid_to, is_current, run_key)


@dataclass(frozen=True)
class Dim:
    table: IcebergTable
    key: tuple[str, ...]
    api_key: tuple[str, ...]


# Keys restated from design.md: dim_station by stat_id, dim_charger by stat_id + charger_id.
STATION = Dim(DIM_STATION, ("stat_id",), ("statId",))
CHARGER = Dim(DIM_CHARGER, ("stat_id", "charger_id"), ("statId", "chgerId"))
DIMS = (STATION, CHARGER)


def _load_items() -> list[dict[str, str]]:
    body = json.loads((FIXTURES / "real_info_page.json").read_text(encoding="utf-8"))
    return list(body["items"]["item"])


# 10 real chargers at 8 stations: ME178009 has chargers 01 and 02, ME181150 has 12 and 13.
INFO_ITEMS = _load_items()
# A real one-charger station, and A with a station and a charger attribute changed, so it is a
# new version in either dimension.
A = INFO_ITEMS[0]
A_CHANGED = {**A, "statNm": "renamed station", "maker": "replaced maker"}
# Day 2's real page: A changed, every other charger as on day 1.
DAY_2_ITEMS = [A_CHANGED, *INFO_ITEMS[1:]]


class RawRuns:
    """Completed charger_info runs, and read_raw over them.

    run() and truncated() register a run's items and return the manifest the collector writes for
    it. Calling the object is read_raw: the run's items as one raw page; the run_key is recorded in
    `reads`.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._items: dict[str, Sequence[Item]] = {}
        self.reads: list[str] = []

    def run(self, at: datetime, run_key: str, items: Sequence[Item]) -> RunManifest:
        """A complete run that received every item (0 items: an empty run)."""
        return self._manifest(at, run_key, items, total_count=len(items), page_size=PAGE_SIZE)

    def truncated(self, at: datetime, run_key: str, items: Sequence[Item]) -> RunManifest:
        """A run cut by the budget after page 1 of 2: items are page 1, page 2 was never fetched."""
        return self._manifest(at, run_key, items, total_count=2 * len(items), page_size=len(items))

    def _manifest(
        self,
        at: datetime,
        run_key: str,
        items: Sequence[Item],
        *,
        total_count: int,
        page_size: int,
    ) -> RunManifest:
        self._items[run_key] = items
        return RunManifest(
            version=MANIFEST_VERSION,
            dataset=str(Dataset.CHARGER_INFO),
            operation="getChargerInfo",
            dt=f"{partition_date(at):%Y-%m-%d}",
            run_key=run_key,
            collected_at=at.astimezone(UTC).isoformat(),
            total_count=total_count,
            row_count=len(items),
            page_size=page_size,
            expected_pages=page_count(total_count, page_size),
            pages=[1],
        )

    def __call__(self, manifest: RunManifest) -> DataFrame:
        self.reads.append(manifest.run_key)
        return _page(self._spark, self._items[manifest.run_key], manifest.collected)


@pytest.fixture(autouse=True)
def _dim_tables(spark: SparkSession) -> Iterator[None]:
    """Both dim tables, created empty with create_table_sql and dropped when the test ends."""
    for dim in DIMS:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{dim.table.namespace}")
        spark.sql(create_table_sql(dim.table))
    yield
    for dim in DIMS:
        spark.sql(f"DROP TABLE IF EXISTS {dim.table.full_name}")


@pytest.fixture
def raw(spark: SparkSession) -> RawRuns:
    return RawRuns(spark)


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


def _ignore(_: Watermark) -> None:
    """advance for steps that do not look at the watermark."""


def _seed(spark: SparkSession, raw: RawRuns, *runs: RunManifest) -> None:
    """Apply runs to the empty tables with load_dims, then forget that they were read."""
    load_dims(spark, list(runs), watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO)
    raw.reads.clear()


def _key(dim: Dim, row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row[k] for k in dim.key)


def _item_key(dim: Dim, item: Item) -> tuple[str, ...]:
    return tuple(item[k] for k in dim.api_key)


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
    return sorted(rows, key=lambda r: (*_key(dim, r), r["valid_from"]))


def _tables(spark: SparkSession) -> dict[str, list[Row]]:
    """Every version in both dim tables, every column, by table name."""
    return {dim.table.name: _versions(spark, dim) for dim in DIMS}


def _current_versions(spark: SparkSession) -> dict[str, list[Current]]:
    """Each table's current versions as (key, valid_from, source_run_key), by table name."""
    return {
        dim.table.name: sorted(
            (_key(dim, r), r["valid_from"], r["source_run_key"])
            for r in _versions(spark, dim)
            if r["is_current"]
        )
        for dim in DIMS
    }


def _versions_made_by(
    items: Sequence[Item], at: datetime, run_key: str
) -> dict[str, list[Current]]:
    """What _current_versions reads when a run at `at` made every current version: one per key."""
    return {
        dim.table.name: sorted({(_item_key(dim, item), at, run_key) for item in items})
        for dim in DIMS
    }


def _history(spark: SparkSession, item: Item) -> dict[str, list[Version]]:
    """Every version of the item's key in each table, oldest first, by table name."""
    return {
        dim.table.name: [
            (r["valid_from"], r["valid_to"], r["is_current"], r["source_run_key"])
            for r in _versions(spark, dim)
            if _key(dim, r) == _item_key(dim, item)
        ]
        for dim in DIMS
    }


def _key_count(dim: Dim, items: Sequence[Item]) -> int:
    return len({_item_key(dim, item) for item in items})


# --- no runs ----------------------------------------------------------------


def test_no_runs_give_no_results_and_do_not_advance_the_watermark(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN no completed runs
    advances: list[Watermark] = []

    # WHEN load_dims runs
    results = load_dims(
        spark, [], watermark=None, read_raw=raw, advance=advances.append, min_row_ratio=RATIO
    )

    # THEN there are no results and the watermark is not advanced
    assert results == []
    assert advances == []


# --- one run ----------------------------------------------------------------


def test_first_run_without_a_watermark_is_applied_to_both_dimensions(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables, no watermark, and one complete run of the real page
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS)

    # WHEN load_dims runs
    load_dims(spark, [run], watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO)

    # THEN both dimensions have one current version per key on the page, valid from the run's
    # collected_at and made by the run
    assert _current_versions(spark) == _versions_made_by(INFO_ITEMS, DAY_1, RUN_1)


def test_first_run_without_a_watermark_advances_once_to_its_collected_at_and_run_key(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables, no watermark, and one complete run of the real page
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS)
    advances: list[Watermark] = []

    # WHEN load_dims runs
    load_dims(
        spark, [run], watermark=None, read_raw=raw, advance=advances.append, min_row_ratio=RATIO
    )

    # THEN the watermark is advanced exactly once, to the run's (collected_at, run_key)
    assert advances == [Watermark(DAY_1, RUN_1)]


def test_applied_run_result_has_no_skip_reason_and_the_versions_each_dimension_opened(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables and one complete run of the real page (10 chargers at 8 stations)
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS)

    # WHEN load_dims runs
    results = load_dims(
        spark, [run], watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO
    )

    # THEN the run's result has no skip reason, and each dimension's counts are its own: nothing
    # closed, one version opened per station and per charger
    assert results == [
        RunResult(
            RUN_1,
            None,
            stations=MergeCounts(closed=0, opened=_key_count(STATION, INFO_ITEMS)),
            chargers=MergeCounts(closed=0, opened=_key_count(CHARGER, INFO_ITEMS)),
        )
    ]


# --- watermark --------------------------------------------------------------


@pytest.mark.parametrize(
    ("at", "run_key"),
    [
        pytest.param(DAY_2, RUN_2, id="the_watermark_run_itself"),
        pytest.param(DAY_1, RUN_1, id="collected_before_the_watermark"),
        pytest.param(DAY_2, MANUAL_2, id="same_collected_at_and_an_earlier_run_key"),
    ],
)
def test_run_at_or_before_the_watermark_is_passed_over_without_reading_or_advancing(
    spark: SparkSession, raw: RawRuns, at: datetime, run_key: str
) -> None:
    # GIVEN the watermark at day 2's run and a complete run at or before it
    run = raw.run(at, run_key, INFO_ITEMS)
    advances: list[Watermark] = []

    # WHEN load_dims runs
    results = load_dims(
        spark,
        [run],
        watermark=Watermark(DAY_2, RUN_2),
        read_raw=raw,
        advance=advances.append,
        min_row_ratio=RATIO,
    )

    # THEN the run has no result, its pages are not read, and the watermark is not advanced
    assert results == []
    assert raw.reads == []
    assert advances == []


# --- two runs ---------------------------------------------------------------


def test_two_runs_are_applied_oldest_first(spark: SparkSession, raw: RawRuns) -> None:
    # GIVEN empty dim tables and two complete runs: the real page on day 1, and on day 2 the same
    # page with A changed
    runs = [raw.run(DAY_1, RUN_1, INFO_ITEMS), raw.run(DAY_2, RUN_2, DAY_2_ITEMS)]

    # WHEN load_dims runs over both
    load_dims(spark, runs, watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO)

    # THEN in both dimensions A's day-1 version is closed at day 2 and its day-2 version is current
    assert _history(spark, A) == {
        dim.table.name: [(DAY_1, DAY_2, False, RUN_1), (DAY_2, OPEN_END, True, RUN_2)]
        for dim in DIMS
    }


def test_two_runs_advance_the_watermark_once_each_oldest_first(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables and two complete runs on day 1 and day 2
    runs = [raw.run(DAY_1, RUN_1, INFO_ITEMS), raw.run(DAY_2, RUN_2, DAY_2_ITEMS)]
    advances: list[Watermark] = []

    # WHEN load_dims runs over both
    load_dims(
        spark, runs, watermark=None, read_raw=raw, advance=advances.append, min_row_ratio=RATIO
    )

    # THEN the watermark is advanced to day 1's run, then to day 2's run
    assert advances == [Watermark(DAY_1, RUN_1), Watermark(DAY_2, RUN_2)]


# --- skipped runs -----------------------------------------------------------

MakeRun = Callable[[RawRuns], RunManifest]

UNLOADABLE = [
    pytest.param(lambda raw: raw.run(DAY_2, RUN_2, []), "empty run (0 rows)", id="empty"),
    pytest.param(
        lambda raw: raw.truncated(DAY_2, RUN_2, INFO_ITEMS[:5]),
        "truncated run (1 of 2 pages)",
        id="truncated",
    ),
]


@pytest.mark.parametrize(("make_run", "reason"), UNLOADABLE)
def test_empty_or_truncated_run_is_reported_with_its_reason_without_reading_or_advancing(
    spark: SparkSession, raw: RawRuns, make_run: MakeRun, reason: str
) -> None:
    # GIVEN the real page applied on day 1 with the watermark at it, and a day-2 run that is
    # empty or truncated
    _seed(spark, raw, raw.run(DAY_1, RUN_1, INFO_ITEMS))
    run = make_run(raw)
    advances: list[Watermark] = []

    # WHEN load_dims runs
    results = load_dims(
        spark,
        [run],
        watermark=Watermark(DAY_1, RUN_1),
        read_raw=raw,
        advance=advances.append,
        min_row_ratio=RATIO,
    )

    # THEN the run's result carries the skip reason and no counts, its pages are not read, and
    # the watermark is not advanced
    assert results == [RunResult(RUN_2, reason, stations=None, chargers=None)]
    assert raw.reads == []
    assert advances == []


@pytest.mark.parametrize(("make_run", "reason"), UNLOADABLE)
def test_empty_or_truncated_run_leaves_both_dimensions_as_they_were(
    spark: SparkSession, raw: RawRuns, make_run: MakeRun, reason: str
) -> None:
    # GIVEN the real page applied on day 1 with the watermark at it, and a day-2 run that is
    # empty or truncated (applying it would close the versions of every charger it lacks)
    _seed(spark, raw, raw.run(DAY_1, RUN_1, INFO_ITEMS))
    before = _tables(spark)
    run = make_run(raw)

    # WHEN load_dims runs
    load_dims(
        spark,
        [run],
        watermark=Watermark(DAY_1, RUN_1),
        read_raw=raw,
        advance=_ignore,
        min_row_ratio=RATIO,
    )

    # THEN every version in both tables is as it was
    assert _tables(spark) == before


def test_run_with_fewer_rows_than_the_ratio_of_current_chargers_is_skipped(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN the real page (10 chargers at 8 stations) applied on day 1 with the watermark at it,
    # and a complete day-2 run of 8 chargers: below 0.9 x 10 chargers, though not below 0.9 x 8
    # stations
    _seed(spark, raw, raw.run(DAY_1, RUN_1, INFO_ITEMS))
    run = raw.run(DAY_2, RUN_2, INFO_ITEMS[:8])
    advances: list[Watermark] = []

    # WHEN load_dims runs with the ratio at 0.9
    results = load_dims(
        spark,
        [run],
        watermark=Watermark(DAY_1, RUN_1),
        read_raw=raw,
        advance=advances.append,
        min_row_ratio=0.9,
    )

    # THEN the run is skipped as a drop against the 10 current chargers, its pages are not read,
    # and the watermark is not advanced
    assert results == [
        RunResult(
            RUN_2,
            "row count dropped: 8 rows against 10 current chargers (min ratio 0.9)",
            stations=None,
            chargers=None,
        )
    ]
    assert raw.reads == []
    assert advances == []


def test_first_load_into_empty_dimensions_does_not_compare_the_row_count(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables and a complete run of only 2 chargers, with the ratio at its
    # maximum (1.0)
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS[:2])

    # WHEN load_dims runs
    load_dims(spark, [run], watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=1.0)

    # THEN the run is applied to both dimensions
    assert _current_versions(spark) == _versions_made_by(INFO_ITEMS[:2], DAY_1, RUN_1)


def test_only_the_complete_run_after_a_skipped_run_is_applied(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables, a truncated run on day 1 and a complete run of the real page on day 2
    runs = [raw.truncated(DAY_1, RUN_1, INFO_ITEMS[:5]), raw.run(DAY_2, RUN_2, INFO_ITEMS)]

    # WHEN load_dims runs over both
    load_dims(spark, runs, watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO)

    # THEN only day 2's pages are read, and every current version in both dimensions is day 2's
    assert raw.reads == [RUN_2]
    assert _current_versions(spark) == _versions_made_by(INFO_ITEMS, DAY_2, RUN_2)


def test_watermark_advances_only_to_the_complete_run_after_a_skipped_run(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables, a truncated run on day 1 and a complete run of the real page on day 2
    runs = [raw.truncated(DAY_1, RUN_1, INFO_ITEMS[:5]), raw.run(DAY_2, RUN_2, INFO_ITEMS)]
    advances: list[Watermark] = []

    # WHEN load_dims runs over both
    load_dims(
        spark, runs, watermark=None, read_raw=raw, advance=advances.append, min_row_ratio=RATIO
    )

    # THEN the watermark is advanced once, to day 2's run
    assert advances == [Watermark(DAY_2, RUN_2)]


def test_applied_run_stays_applied_when_the_run_after_it_is_skipped(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables, a complete run of the real page on day 1 and a truncated run on day 2
    runs = [raw.run(DAY_1, RUN_1, INFO_ITEMS), raw.truncated(DAY_2, RUN_2, INFO_ITEMS[:5])]
    advances: list[Watermark] = []

    # WHEN load_dims runs over both
    load_dims(
        spark, runs, watermark=None, read_raw=raw, advance=advances.append, min_row_ratio=RATIO
    )

    # THEN both dimensions hold day 1's run and the watermark is at it
    assert _current_versions(spark) == _versions_made_by(INFO_ITEMS, DAY_1, RUN_1)
    assert advances == [Watermark(DAY_1, RUN_1)]


def test_last_result_is_the_skip_when_a_skipped_run_follows_an_applied_run(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables, a complete run of the real page on day 1 and a truncated run on day 2
    runs = [raw.run(DAY_1, RUN_1, INFO_ITEMS), raw.truncated(DAY_2, RUN_2, INFO_ITEMS[:5])]

    # WHEN load_dims runs over both
    results = load_dims(
        spark, runs, watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO
    )

    # THEN there is one result per run in order: day 1 applied, day 2 skipped as truncated
    assert [(r.run_key, r.skipped) for r in results] == [
        (RUN_1, None),
        (RUN_2, "truncated run (1 of 2 pages)"),
    ]


# --- rerun ------------------------------------------------------------------


def test_rerunning_an_applied_run_without_a_watermark_counts_no_changes(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN the real page applied on day 1 but no watermark written (died before advance)
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS)
    _seed(spark, raw, run)

    # WHEN the same run is loaded again without a watermark
    results = load_dims(
        spark, [run], watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO
    )

    # THEN it is applied with nothing closed or opened in either dimension
    assert results == [
        RunResult(
            RUN_1,
            None,
            stations=MergeCounts(closed=0, opened=0),
            chargers=MergeCounts(closed=0, opened=0),
        )
    ]


def test_rerunning_an_applied_run_without_a_watermark_leaves_both_dimensions_as_they_were(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN the real page applied on day 1 but no watermark written (died before advance)
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS)
    _seed(spark, raw, run)
    before = _tables(spark)

    # WHEN the same run is loaded again without a watermark
    load_dims(spark, [run], watermark=None, read_raw=raw, advance=_ignore, min_row_ratio=RATIO)

    # THEN every version in both tables is as it was
    assert _tables(spark) == before


# --- write order ------------------------------------------------------------


def test_both_dimensions_hold_the_run_when_the_watermark_advances(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN empty dim tables, one complete run of the real page, and an advance that reads both
    # tables at the moment it is called
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS)
    seen: list[dict[str, list[Current]]] = []

    def advance(_: Watermark) -> None:
        seen.append(_current_versions(spark))

    # WHEN load_dims runs
    load_dims(spark, [run], watermark=None, read_raw=raw, advance=advance, min_row_ratio=RATIO)

    # THEN when the watermark advanced, both dimensions already held the run's versions
    assert seen == [_versions_made_by(INFO_ITEMS, DAY_1, RUN_1)]


def test_failed_raw_read_does_not_advance_the_watermark(spark: SparkSession, raw: RawRuns) -> None:
    # GIVEN a complete run whose pages cannot be read
    run = raw.run(DAY_1, RUN_1, INFO_ITEMS)
    advances: list[Watermark] = []

    def read_raw(_: RunManifest) -> DataFrame:
        raise OSError("s3a read failed")

    # WHEN load_dims runs, the read error propagates
    with pytest.raises(OSError, match="s3a read failed"):
        load_dims(
            spark,
            [run],
            watermark=None,
            read_raw=read_raw,
            advance=advances.append,
            min_row_ratio=RATIO,
        )

    # THEN the watermark is not advanced
    assert advances == []
