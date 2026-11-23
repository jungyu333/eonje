"""Spark tests for eonje.load.observations.load_observations: settled charger_status runs after the
watermark are loaded into stg.obs_status in KST date batches, oldest first; each batch is merged
before the watermark advances to its last run; runs without rows are never read but are passed by
the watermark; truncated runs are loaded; a rerun without a watermark changes nothing.

stg.obs_status is created empty with create_table_sql in the throwaway `lake` catalog of
tests/spark/conftest.py and dropped when the test ends. read_raw is a fake (RawRuns) that serves
each run's items from the real getChargerStatus page in tests/fixtures/evcharger, tagged with
run_key and source as the real reader does, and records the run_keys of every call; advance records
the watermarks it was given.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from eonje.client.paging import page_count
from eonje.load.observations import BatchResult, load_observations
from eonje.storage.lake.iceberg import OBS_STATUS, create_table_sql
from eonje.storage.lake.spark import CATALOG
from eonje.storage.postgres.watermark import Watermark
from eonje.storage.raw.paths import Dataset, partition_date, run_key_from_run_id
from eonje.storage.raw.run import MANIFEST_VERSION, RunManifest

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"

DATASET = str(Dataset.CHARGER_STATUS)
PAGE_SIZE = 9999
SETTLE = timedelta(minutes=30)

# Two runs on 2026-11-09 KST (18:10, 18:20) and one on 2026-11-10 KST (18:10).
AT_1A = datetime(2026, 11, 9, 9, 10, tzinfo=UTC)
AT_1B = AT_1A + timedelta(minutes=10)
AT_2 = AT_1A + timedelta(days=1)
RUN_1A, RUN_1B, RUN_2 = (
    run_key_from_run_id(f"scheduled__{at.isoformat()}") for at in (AT_1A, AT_1B, AT_2)
)
DT_1, DT_2 = (f"{partition_date(at):%Y-%m-%d}" for at in (AT_1A, AT_2))
# Long after every run above: all of them are settled.
NOW = AT_2 + timedelta(days=1)

Item = Mapping[str, str]
ObsKey = tuple[str, str, str]  # (stat_id, charger_id, run_key)


def _load_items() -> list[dict[str, str]]:
    body = json.loads((FIXTURES / "real_status_page.json").read_text(encoding="utf-8"))
    return list(body["items"]["item"])


# 10 real chargers, all distinct keys.
STATUS_ITEMS = _load_items()


class RawRuns:
    """Completed charger_status runs, and read_raw over them.

    run() and truncated() register a run's items and return the manifest the collector writes for
    it. Calling the object is read_raw: the items of every given run as raw rows tagged with
    run_key and source; the run_keys of each call are recorded in `reads`.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._runs: dict[str, tuple[datetime, Sequence[Item]]] = {}
        self.reads: list[list[str]] = []

    def run(self, at: datetime, run_key: str, items: Sequence[Item]) -> RunManifest:
        """A complete run that received every item (0 items: a run without rows)."""
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
        self._runs[run_key] = (at, items)
        return RunManifest(
            version=MANIFEST_VERSION,
            dataset=DATASET,
            operation="getChargerStatus",
            dt=f"{partition_date(at):%Y-%m-%d}",
            run_key=run_key,
            collected_at=at.astimezone(UTC).isoformat(),
            total_count=total_count,
            row_count=len(items),
            page_size=page_size,
            expected_pages=page_count(total_count, page_size),
            pages=[1],
        )

    def __call__(self, manifests: Sequence[RunManifest]) -> DataFrame:
        self.reads.append([m.run_key for m in manifests])
        return _raw(self._spark, [(m.run_key, *self._runs[m.run_key]) for m in manifests])


def _raw(spark: SparkSession, runs: Sequence[tuple[str, datetime, Sequence[Item]]]) -> DataFrame:
    """Raw charger_status rows as the reader returns them: API fields as strings, meta columns,
    run_key and source."""
    seen = (f for _, _, items in runs for item in items for f in item)
    fields = list(dict.fromkeys([*STATUS_ITEMS[0], *seen]))
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
        (*(item.get(f) for f in fields), at, at, 1, run_key, "delta")
        for run_key, at, items in runs
        for item in items
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(autouse=True)
def _obs_table(spark: SparkSession) -> Iterator[None]:
    """stg.obs_status, created empty with create_table_sql and dropped when the test ends."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{OBS_STATUS.namespace}")
    spark.sql(create_table_sql(OBS_STATUS))
    yield
    spark.sql(f"DROP TABLE IF EXISTS {OBS_STATUS.full_name}")


@pytest.fixture
def raw(spark: SparkSession) -> RawRuns:
    return RawRuns(spark)


def _ignore(_: Watermark) -> None:
    """advance for steps that do not look at the watermark."""


def _load(
    spark: SparkSession,
    raw: RawRuns,
    runs: Sequence[RunManifest],
    *,
    watermark: Watermark | None = None,
    now: datetime = NOW,
    advances: list[Watermark] | None = None,
) -> list[BatchResult]:
    return load_observations(
        spark,
        runs,
        watermark=watermark,
        now=now,
        settle=SETTLE,
        read_raw=raw,
        advance=_ignore if advances is None else advances.append,
    )


def _stored(spark: SparkSession) -> set[ObsKey]:
    """The key of every row in stg.obs_status."""
    rows = spark.table(OBS_STATUS.full_name).select("stat_id", "charger_id", "run_key").collect()
    return {(r["stat_id"], r["charger_id"], r["run_key"]) for r in rows}


def _stored_run_keys(spark: SparkSession) -> set[str]:
    return {run_key for _, _, run_key in _stored(spark)}


def _keys(run_key: str, items: Sequence[Item]) -> set[ObsKey]:
    """The observation keys a run of these items makes."""
    return {(item["statId"], item["chgerId"], run_key) for item in items}


def _three_runs(raw: RawRuns) -> list[RunManifest]:
    """Two runs on day 1 and one on day 2, each with every real charger."""
    return [
        raw.run(AT_1A, RUN_1A, STATUS_ITEMS),
        raw.run(AT_1B, RUN_1B, STATUS_ITEMS),
        raw.run(AT_2, RUN_2, STATUS_ITEMS),
    ]


# --- no watermark -----------------------------------------------------------


def test_without_a_watermark_every_settled_run_is_stored(spark: SparkSession, raw: RawRuns) -> None:
    # GIVEN an empty table, no watermark, and three settled runs over two KST dates
    runs = _three_runs(raw)

    # WHEN load_observations runs
    _load(spark, raw, runs)

    # THEN the table holds one row per charger per run
    assert _stored(spark) == (
        _keys(RUN_1A, STATUS_ITEMS) | _keys(RUN_1B, STATUS_ITEMS) | _keys(RUN_2, STATUS_ITEMS)
    )


def test_without_a_watermark_there_is_one_result_per_date_in_date_order(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN an empty table, no watermark, and three settled runs over two KST dates
    runs = _three_runs(raw)

    # WHEN load_observations runs
    results = _load(spark, raw, runs)

    # THEN there is one result per date, oldest first, with its run count and row count
    n = len(STATUS_ITEMS)
    assert results == [
        BatchResult(DATASET, DT_1, runs=2, rows=2 * n),
        BatchResult(DATASET, DT_2, runs=1, rows=n),
    ]


# --- watermark advance ------------------------------------------------------


def test_watermark_advances_once_per_batch_to_its_last_run_in_date_order(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN three settled runs over two KST dates
    runs = _three_runs(raw)
    advances: list[Watermark] = []

    # WHEN load_observations runs
    _load(spark, raw, runs, advances=advances)

    # THEN the watermark goes to day 1's last run, then to day 2's run
    assert advances == [Watermark(AT_1B, RUN_1B), Watermark(AT_2, RUN_2)]


def test_batch_rows_are_in_the_table_when_the_watermark_advances(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN three settled runs over two KST dates, and an advance that reads the table's run_keys
    # at the moment it is called
    runs = _three_runs(raw)
    seen: list[set[str]] = []

    def advance(_: Watermark) -> None:
        seen.append(_stored_run_keys(spark))

    # WHEN load_observations runs
    load_observations(
        spark, runs, watermark=None, now=NOW, settle=SETTLE, read_raw=raw, advance=advance
    )

    # THEN each advance already saw its own batch's runs in the table
    assert seen == [{RUN_1A, RUN_1B}, {RUN_1A, RUN_1B, RUN_2}]


@pytest.mark.parametrize(
    "watermark",
    [
        pytest.param(Watermark(AT_1A, RUN_1A), id="at_the_first_run"),
        pytest.param(Watermark(AT_1A + timedelta(minutes=5), RUN_1A), id="between_runs"),
    ],
)
def test_runs_at_or_before_the_watermark_are_not_passed_to_read_raw(
    spark: SparkSession, raw: RawRuns, watermark: Watermark
) -> None:
    # GIVEN three settled runs and a watermark at or after day 1's first run
    runs = _three_runs(raw)

    # WHEN load_observations runs
    _load(spark, raw, runs, watermark=watermark)

    # THEN read_raw is given only the runs after the watermark, one call per date
    assert raw.reads == [[RUN_1B], [RUN_2]]


# --- unsettled runs ---------------------------------------------------------


def test_unsettled_run_is_not_stored(spark: SparkSession, raw: RawRuns) -> None:
    # GIVEN two day-1 runs, the second collected less than SETTLE before now
    runs = [raw.run(AT_1A, RUN_1A, STATUS_ITEMS), raw.run(AT_1B, RUN_1B, STATUS_ITEMS)]
    now = AT_1B + SETTLE - timedelta(minutes=1)

    # WHEN load_observations runs
    _load(spark, raw, runs, now=now)

    # THEN only the settled run is read and stored
    assert raw.reads == [[RUN_1A]]
    assert _stored_run_keys(spark) == {RUN_1A}


def test_watermark_does_not_pass_an_unsettled_run(spark: SparkSession, raw: RawRuns) -> None:
    # GIVEN two day-1 runs, the second collected less than SETTLE before now
    runs = [raw.run(AT_1A, RUN_1A, STATUS_ITEMS), raw.run(AT_1B, RUN_1B, STATUS_ITEMS)]
    now = AT_1B + SETTLE - timedelta(minutes=1)
    advances: list[Watermark] = []

    # WHEN load_observations runs
    _load(spark, raw, runs, now=now, advances=advances)

    # THEN the watermark stops at the settled run
    assert advances == [Watermark(AT_1A, RUN_1A)]


# --- runs without rows ------------------------------------------------------


def test_run_without_rows_is_not_read_but_the_watermark_passes_it(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN a day-1 run of every charger followed by a day-1 run without rows
    runs = [raw.run(AT_1A, RUN_1A, STATUS_ITEMS), raw.run(AT_1B, RUN_1B, [])]
    advances: list[Watermark] = []

    # WHEN load_observations runs
    _load(spark, raw, runs, advances=advances)

    # THEN read_raw gets only the run with rows, and the watermark goes to the empty run
    assert raw.reads == [[RUN_1A]]
    assert advances == [Watermark(AT_1B, RUN_1B)]


def test_batch_of_only_empty_runs_is_not_read_and_counts_zero_rows(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN two day-1 runs, both without rows
    runs = [raw.run(AT_1A, RUN_1A, []), raw.run(AT_1B, RUN_1B, [])]
    advances: list[Watermark] = []

    # WHEN load_observations runs
    results = _load(spark, raw, runs, advances=advances)

    # THEN read_raw is never called, the batch has 0 rows, and the watermark passes both runs
    assert raw.reads == []
    assert results == [BatchResult(DATASET, DT_1, runs=2, rows=0)]
    assert advances == [Watermark(AT_1B, RUN_1B)]


# --- truncated runs ---------------------------------------------------------


def test_truncated_run_is_stored(spark: SparkSession, raw: RawRuns) -> None:
    # GIVEN a day-1 run cut after page 1 of 2, holding 5 real chargers
    run = raw.truncated(AT_1A, RUN_1A, STATUS_ITEMS[:5])

    # WHEN load_observations runs
    results = _load(spark, raw, [run])

    # THEN its rows are stored and counted
    assert _stored(spark) == _keys(RUN_1A, STATUS_ITEMS[:5])
    assert results == [BatchResult(DATASET, DT_1, runs=1, rows=5)]


# --- rerun ------------------------------------------------------------------


def test_rerunning_the_same_runs_without_a_watermark_keeps_the_row_count(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN three runs already loaded but no watermark written (died before advance)
    runs = _three_runs(raw)
    _load(spark, raw, runs)
    before = spark.table(OBS_STATUS.full_name).count()

    # WHEN the same runs are loaded again without a watermark
    _load(spark, raw, runs)

    # THEN the table has as many rows as before
    assert spark.table(OBS_STATUS.full_name).count() == before


# --- nothing to do ----------------------------------------------------------

MakeRuns = Callable[[RawRuns], list[RunManifest]]


@pytest.mark.parametrize(
    ("make_runs", "watermark", "now"),
    [
        pytest.param(lambda raw: [], None, NOW, id="no_runs"),
        pytest.param(_three_runs, Watermark(AT_2, RUN_2), NOW, id="all_at_or_before_watermark"),
        pytest.param(_three_runs, None, AT_1A, id="none_settled"),
    ],
)
def test_nothing_to_process_gives_no_results_and_calls_neither_read_raw_nor_advance(
    spark: SparkSession,
    raw: RawRuns,
    make_runs: MakeRuns,
    watermark: Watermark | None,
    now: datetime,
) -> None:
    # GIVEN no run that is both after the watermark and settled
    runs = make_runs(raw)
    advances: list[Watermark] = []

    # WHEN load_observations runs
    results = _load(spark, raw, runs, watermark=watermark, now=now, advances=advances)

    # THEN there are no results, and neither read_raw nor advance is called
    assert results == []
    assert raw.reads == []
    assert advances == []


# --- failure ----------------------------------------------------------------


def test_failed_read_of_the_second_batch_leaves_the_watermark_at_the_first_batch(
    spark: SparkSession, raw: RawRuns
) -> None:
    # GIVEN three settled runs over two KST dates, and a read_raw that fails on its second call
    runs = _three_runs(raw)
    advances: list[Watermark] = []
    calls: list[int] = []

    def read_raw(manifests: Sequence[RunManifest]) -> DataFrame:
        calls.append(1)
        if len(calls) == 2:
            raise OSError("s3a read failed")
        return raw(manifests)

    # WHEN load_observations runs, the read error propagates
    with pytest.raises(OSError, match="s3a read failed"):
        load_observations(
            spark,
            runs,
            watermark=None,
            now=NOW,
            settle=SETTLE,
            read_raw=read_raw,
            advance=advances.append,
        )

    # THEN the watermark was advanced only to the first batch's last run
    assert advances == [Watermark(AT_1B, RUN_1B)]
