"""Spark tests for eonje.storage.lake.observations: read_run_folders reads completed raw runs
written by the real RawRunWriter into a local folder and tags every row with its run_key and source,
read_raw_runs refuses manifests of mixed datasets, and merge_observations inserts observation rows
into stg.obs_status, ignoring keys that are already there.

stg.obs_status is created empty with create_table_sql in the throwaway `lake` catalog of
tests/spark/conftest.py and dropped when the test ends. Items come from the real getChargerStatus
page in tests/fixtures/evcharger. Observation rows are built with transform.observations, as the
load flow does.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fsspec.implementations.local import LocalFileSystem
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from eonje.client.types import Page
from eonje.storage.lake.iceberg import OBS_STATUS, create_table_sql
from eonje.storage.lake.observations import merge_observations, read_raw_runs, read_run_folders
from eonje.storage.lake.spark import CATALOG
from eonje.storage.raw.paths import (
    Dataset,
    RawRun,
    manifest_path,
    partition_date,
    run_key_from_run_id,
    run_prefix,
)
from eonje.storage.raw.run import MANIFEST_VERSION, RawRunWriter, RunManifest
from eonje.transform.observations import observations

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
TIME_COLUMNS = ("observed_at", "stat_upd_dt", "last_ts", "last_te", "now_ts")
# Key of stg.obs_status, restated from design.md
OBS_KEY = ("stat_id", "charger_id", "run_key")
PAGE_SIZE = 5
# auto_mkdir: RawRunWriter writes pages with pipe_file and never creates folders itself
FS = LocalFileSystem(auto_mkdir=True)

Item = Mapping[str, str]
Row = dict[str, Any]


@dataclass(frozen=True)
class Run:
    run_key: str
    source: str
    collected_at: datetime


def _run(run_id: str, collected_at: datetime, source: str = "delta") -> Run:
    return Run(run_key_from_run_id(run_id), source, collected_at)


AT_1 = datetime(2026, 11, 9, 9, 10, tzinfo=UTC)
AT_2 = AT_1 + timedelta(minutes=10)
RUN_1 = _run("scheduled__2026-11-09T09:10:00+00:00", AT_1)
RUN_2 = _run("scheduled__2026-11-09T09:20:00+00:00", AT_2)
# 2026-11-09 23:59 KST and 2026-11-10 00:10 KST: two observed_date partitions
LATE_DAY_1 = _run("scheduled__2026-11-09T14:59:00+00:00", datetime(2026, 11, 9, 14, 59, tzinfo=UTC))
EARLY_DAY_2 = _run(
    "scheduled__2026-11-09T15:10:00+00:00", datetime(2026, 11, 9, 15, 10, tzinfo=UTC)
)


def _load_items() -> list[dict[str, str]]:
    body = json.loads((FIXTURES / "real_status_page.json").read_text(encoding="utf-8"))
    return list(body["items"]["item"])


# 10 real chargers, all distinct keys.
STATUS_ITEMS = _load_items()


def _charger(item: Mapping[str, Any]) -> tuple[str, str]:
    return (item["statId"], item["chgerId"])


# --- raw files --------------------------------------------------------------


@pytest.fixture
def root(tmp_path: Path) -> str:
    """The local folder standing in for the raw bucket."""
    return (tmp_path / "raw").as_posix()


def _write_run(
    root: str,
    run: Run,
    pages: Sequence[Sequence[Mapping[str, str]]],
    dataset: Dataset = Dataset.CHARGER_STATUS,
) -> str:
    """Write a completed run (pages, then _manifest.json) with RawRunWriter; return its folder
    as Spark reads it."""
    raw_run = RawRun(dataset, partition_date(run.collected_at), run.run_key)
    writer = RawRunWriter(FS, root, raw_run, run.collected_at, page_size=PAGE_SIZE)
    writer.start()
    total = sum(len(items) for items in pages)
    for page_no, items in enumerate(pages, start=1):
        writer.write(
            Page(
                operation="getChargerStatus",
                page_no=page_no,
                total_count=total,
                items=[dict(item) for item in items],
                fetched_at=run.collected_at,
            )
        )
    writer.finish()
    return f"file://{run_prefix(root, raw_run)}"


# --- in-memory raw and observation rows -------------------------------------


def _raw(spark: SparkSession, rows: Sequence[tuple[Run, int, Item]]) -> DataFrame:
    """Raw charger_status rows as (run, page_no, item), with the run_key and source the reader
    attaches."""
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


def _obs(spark: SparkSession, *runs: tuple[Run, Sequence[Item]]) -> DataFrame:
    """Observation rows of each run's items (all on page 1)."""
    return observations(_raw(spark, [(run, 1, item) for run, items in runs for item in items]))


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
    return sorted(rows, key=lambda r: tuple(r[k] for k in OBS_KEY))


@pytest.fixture
def table(spark: SparkSession) -> Iterator[str]:
    """stg.obs_status, created empty with create_table_sql and dropped when the test ends."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{OBS_STATUS.namespace}")
    spark.sql(create_table_sql(OBS_STATUS))
    yield OBS_STATUS.full_name
    spark.sql(f"DROP TABLE IF EXISTS {OBS_STATUS.full_name}")


def _table_rows(spark: SparkSession, table: str) -> list[Row]:
    return _rows(spark.table(table))


def _commits(spark: SparkSession, table: str) -> int:
    return spark.table(f"{table}.snapshots").count()


def _merge(spark: SparkSession, rows: DataFrame) -> int:
    return merge_observations(spark, rows, OBS_KEY)


# --- read_run_folders -------------------------------------------------------


def test_rows_of_several_run_folders_carry_the_run_key_of_the_run_they_came_from(
    spark: SparkSession, root: str
) -> None:
    # GIVEN two completed runs with scheduled__ run keys whose chargers overlap (items 3 and 4
    # are in both)
    first = _write_run(root, RUN_1, [STATUS_ITEMS[:5]])
    second = _write_run(root, RUN_2, [STATUS_ITEMS[3:8]])

    # WHEN both folders are read at once
    raw = read_run_folders(spark, [first, second], source="delta")

    # THEN every row is tagged with the run_key of the folder it was read from
    got = sorted((r["statId"], r["chgerId"], r["run_key"]) for r in raw.collect())
    expected = sorted(
        [(*_charger(i), RUN_1.run_key) for i in STATUS_ITEMS[:5]]
        + [(*_charger(i), RUN_2.run_key) for i in STATUS_ITEMS[3:8]]
    )
    assert got == expected


@pytest.mark.parametrize("source", ["delta", "full"])
def test_every_row_carries_the_source_that_was_passed(
    spark: SparkSession, root: str, source: str
) -> None:
    # GIVEN a completed run
    folder = _write_run(root, RUN_1, [STATUS_ITEMS])

    # WHEN it is read with a source
    raw = read_run_folders(spark, [folder], source=source)

    # THEN every row carries that source
    assert [r["source"] for r in raw.collect()] == [source] * len(STATUS_ITEMS)


def test_pages_with_different_columns_are_read_together_with_missing_columns_as_null(
    spark: SparkSession, root: str
) -> None:
    # GIVEN a run whose page 1 is real items as received, and whose page 2 has no lastTedt field
    # and an extra field the API added
    page_2 = [
        {**{k: v for k, v in item.items() if k != "lastTedt"}, "extraField": "x"}
        for item in STATUS_ITEMS[5:]
    ]
    folder = _write_run(root, RUN_1, [STATUS_ITEMS[:5], page_2])

    # WHEN the run is read
    raw = read_run_folders(spark, [folder], source="delta")

    # THEN every row is there; page 1 rows have no extraField value and page 2 rows no lastTedt
    got = {_charger(r): (r["lastTedt"], r["extraField"]) for r in raw.collect()}
    expected = {_charger(i): (i["lastTedt"], None) for i in STATUS_ITEMS[:5]} | {
        _charger(i): (None, "x") for i in page_2
    }
    assert got == expected


def test_manifest_file_is_not_read_as_a_row(spark: SparkSession, root: str) -> None:
    # GIVEN a completed run of two pages, so its folder also holds _manifest.json
    folder = _write_run(root, RUN_1, [STATUS_ITEMS[:5], STATUS_ITEMS[5:]])
    raw_run = RawRun(Dataset.CHARGER_STATUS, partition_date(RUN_1.collected_at), RUN_1.run_key)
    assert FS.exists(manifest_path(root, raw_run))

    # WHEN the run folder is read
    raw = read_run_folders(spark, [folder], source="delta")

    # THEN only the page rows come back
    assert raw.count() == len(STATUS_ITEMS)


# --- read_raw_runs ----------------------------------------------------------


def _manifest(dataset: Dataset, run: Run) -> RunManifest:
    return RunManifest(
        version=MANIFEST_VERSION,
        dataset=str(dataset),
        operation="getChargerStatus",
        dt=f"{partition_date(run.collected_at):%Y-%m-%d}",
        run_key=run.run_key,
        collected_at=run.collected_at.isoformat(),
        total_count=1,
        row_count=1,
        page_size=PAGE_SIZE,
        expected_pages=1,
        pages=[1],
    )


def test_manifests_of_mixed_datasets_are_rejected(spark: SparkSession) -> None:
    # GIVEN a charger_status run and a charger_info run
    manifests = [
        _manifest(Dataset.CHARGER_STATUS, RUN_1),
        _manifest(Dataset.CHARGER_INFO, RUN_2),
    ]

    # WHEN / THEN reading them together raises ValueError
    with pytest.raises(ValueError, match="one dataset"):
        read_raw_runs(spark, "eonje-raw", manifests)


# --- merge_observations -----------------------------------------------------


def test_merge_into_an_empty_table_inserts_every_row_and_returns_the_row_count(
    spark: SparkSession, table: str
) -> None:
    # GIVEN the empty table and observation rows of a real page
    rows = _obs(spark, (RUN_1, STATUS_ITEMS))

    # WHEN they are merged
    count = _merge(spark, rows)

    # THEN the table holds exactly those rows, and the count is the number of rows passed
    assert (_table_rows(spark, table), count) == (_rows(rows), len(STATUS_ITEMS))


def test_merging_the_same_rows_again_keeps_the_table_row_count(
    spark: SparkSession, table: str
) -> None:
    # GIVEN observation rows of a real page already merged
    rows = _obs(spark, (RUN_1, STATUS_ITEMS))
    _merge(spark, rows)

    # WHEN the same rows are merged again
    _merge(spark, rows)

    # THEN the table still has one row per observation
    assert spark.table(table).count() == len(STATUS_ITEMS)


def test_partly_new_rows_add_only_the_new_rows(spark: SparkSession, table: str) -> None:
    # GIVEN the first four chargers of a run already merged
    _merge(spark, _obs(spark, (RUN_1, STATUS_ITEMS[:4])))

    # WHEN all ten chargers of the same run are merged
    everything = _obs(spark, (RUN_1, STATUS_ITEMS))
    _merge(spark, everything)

    # THEN the table holds each of the ten observations once
    assert _table_rows(spark, table) == _rows(everything)


def test_existing_row_with_the_same_key_is_not_changed_by_different_values(
    spark: SparkSession, table: str
) -> None:
    # GIVEN a charger's observation in a run already merged
    item = STATUS_ITEMS[0]
    _merge(spark, _obs(spark, (RUN_1, [item])))
    before = _table_rows(spark, table)

    # WHEN a row with the same key (charger, run) but another status and statUpdDt is merged
    changed = {**item, "stat": "7", "statUpdDt": "20261109182000"}
    _merge(spark, _obs(spark, (RUN_1, [changed])))

    # THEN the table still holds only the first row, every column as it was
    assert _table_rows(spark, table) == before


def test_rows_of_several_observed_dates_go_into_each_date(spark: SparkSession, table: str) -> None:
    # GIVEN rows of a run at 23:59 KST on 2026-11-09 and a run at 00:10 KST on 2026-11-10
    rows = _obs(spark, (LATE_DAY_1, STATUS_ITEMS[:3]), (EARLY_DAY_2, STATUS_ITEMS[3:7]))

    # WHEN they are merged in one call
    _merge(spark, rows)

    # THEN each run's rows are in the table under its own KST date
    got = sorted((r["run_key"], r["observed_date"]) for r in _table_rows(spark, table))
    expected = sorted(
        [(LATE_DAY_1.run_key, date(2026, 11, 9))] * 3
        + [(EARLY_DAY_2.run_key, date(2026, 11, 10))] * 4
    )
    assert got == expected


def test_merging_no_rows_returns_zero_and_leaves_the_table_as_it_was(
    spark: SparkSession, table: str
) -> None:
    # GIVEN a table holding a real page's observations, and no rows to merge
    _merge(spark, _obs(spark, (RUN_1, STATUS_ITEMS)))
    before = (_table_rows(spark, table), _commits(spark, table))
    empty = observations(_raw(spark, []))

    # WHEN the empty rows are merged
    count = _merge(spark, empty)

    # THEN nothing is counted, and the table has the same rows and no new commit
    assert (count, (_table_rows(spark, table), _commits(spark, table))) == (0, before)


# --- end to end -------------------------------------------------------------


def test_real_run_files_read_transformed_and_merged_land_in_the_table(
    spark: SparkSession, root: str, table: str
) -> None:
    # GIVEN two completed runs written by RawRunWriter: run 1 has two pages (page 2 repeats
    # charger 4, paging drift), run 2 overlaps run 1's chargers
    first = _write_run(root, RUN_1, [STATUS_ITEMS[:5], STATUS_ITEMS[4:8]])
    second = _write_run(root, RUN_2, [STATUS_ITEMS[6:]])

    # WHEN the folders are read, turned into observations and merged
    raw = read_run_folders(spark, [first, second], source="delta")
    count = _merge(spark, observations(raw))

    # THEN the table holds one row per charger per run, with that run's key and source, and the
    # count is the number of observations
    got = sorted(
        (r["stat_id"], r["charger_id"], r["run_key"], r["source"])
        for r in _table_rows(spark, table)
    )
    expected = sorted(
        [(*_charger(i), RUN_1.run_key, "delta") for i in STATUS_ITEMS[:8]]
        + [(*_charger(i), RUN_2.run_key, "delta") for i in STATUS_ITEMS[6:]]
    )
    assert (got, count) == (expected, len(expected))
