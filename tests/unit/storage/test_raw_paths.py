import dataclasses
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from eonje.storage.raw_paths import (
    MAX_PAGE_NO,
    Dataset,
    RawRun,
    manifest_path,
    page_path,
    partition_date,
    run_prefix,
)

BUCKET = "eonje-raw"


@pytest.fixture
def status_run() -> RawRun:
    return RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key="20261026T101000")


# --- run_prefix -----------------------------------------------------------------------


def test_run_prefix_follows_dataset_dt_run_partition_layout(status_run: RawRun) -> None:
    # GIVEN a charger_status run on 2026-10-26
    # WHEN the run prefix is built
    prefix = run_prefix(BUCKET, status_run)
    # THEN it matches docs/design.md section 6: <bucket>/<dataset>/dt=YYYY-MM-DD/run=<ts>
    assert prefix == "eonje-raw/charger_status/dt=2026-10-26/run=20261026T101000"


def test_run_prefix_uses_the_dataset_value_and_zero_pads_the_date() -> None:
    # GIVEN a charger_info run in early January
    run = RawRun(dataset=Dataset.CHARGER_INFO, dt=date(2026, 1, 2), run_key="r1")
    # WHEN the run prefix is built
    prefix = run_prefix(BUCKET, run)
    # THEN the dataset segment is the lowercase value and the date is zero-padded
    assert prefix == "eonje-raw/charger_info/dt=2026-01-02/run=r1"


def test_run_prefix_has_no_scheme(status_run: RawRun) -> None:
    # GIVEN a bucket name without scheme (the caller adds s3:// or s3a://)
    # WHEN the run prefix is built
    prefix = run_prefix(BUCKET, status_run)
    # THEN no scheme is prepended
    assert "://" not in prefix
    assert prefix.startswith(f"{BUCKET}/")


def test_run_prefix_is_identical_for_a_rerun_of_the_same_run() -> None:
    # GIVEN two equal runs built independently (rerun of the same run_id)
    a = RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key="k")
    b = RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key="k")
    # WHEN prefixes are built
    # THEN they are identical, so a rerun overwrites the same run= directory
    assert run_prefix(BUCKET, a) == run_prefix(BUCKET, b)


def test_runs_with_different_keys_on_the_same_day_get_distinct_prefixes() -> None:
    # GIVEN two runs on the same day with different keys
    a = RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key="k1")
    b = RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key="k2")
    # WHEN prefixes are built
    # THEN they differ, so runs never overwrite each other
    assert run_prefix(BUCKET, a) != run_prefix(BUCKET, b)


# --- page_path ------------------------------------------------------------------------


def test_page_path_zero_pads_the_page_number_to_three_digits(status_run: RawRun) -> None:
    # GIVEN page 1
    # WHEN the page path is built
    path = page_path(BUCKET, status_run, 1)
    # THEN the file is page=001.parquet under the run prefix
    assert path == "eonje-raw/charger_status/dt=2026-10-26/run=20261026T101000/page=001.parquet"


def test_page_path_accepts_the_maximum_page_number(status_run: RawRun) -> None:
    # GIVEN the largest allowed page number
    # WHEN the page path is built
    path = page_path(BUCKET, status_run, MAX_PAGE_NO)
    # THEN it is formatted without truncation
    assert path.endswith(f"/page={MAX_PAGE_NO}.parquet")


@pytest.mark.parametrize("page_no", [0, -1, MAX_PAGE_NO + 1])
def test_page_path_rejects_out_of_range_page_numbers(status_run: RawRun, page_no: int) -> None:
    # GIVEN a page number outside 1..MAX_PAGE_NO
    # WHEN the page path is built
    # THEN it raises instead of producing a path that breaks lexical ordering
    with pytest.raises(ValueError, match="page_no out of range"):
        page_path(BUCKET, status_run, page_no)


def test_page_paths_sort_lexically_in_page_order(status_run: RawRun) -> None:
    # GIVEN page numbers in a scrambled order spanning 1, 2 and 3 digits
    pages = [MAX_PAGE_NO, 10, 1, 100, 2, 53]
    # WHEN page paths are built and sorted as strings (as object listings are)
    paths = [page_path(BUCKET, status_run, n) for n in pages]
    # THEN string order equals numeric order
    assert sorted(paths) == [page_path(BUCKET, status_run, n) for n in sorted(pages)]


def test_page_path_is_identical_when_built_twice(status_run: RawRun) -> None:
    # GIVEN the same run and page
    # WHEN the page path is built twice
    # THEN both results are identical
    assert page_path(BUCKET, status_run, 7) == page_path(BUCKET, status_run, 7)


# --- manifest_path --------------------------------------------------------------------


def test_manifest_path_sits_directly_in_the_run_directory(status_run: RawRun) -> None:
    # GIVEN a run
    # WHEN the manifest path is built
    path = manifest_path(BUCKET, status_run)
    # THEN it is _manifest.json directly under the run prefix
    assert path == f"{run_prefix(BUCKET, status_run)}/_manifest.json"


def test_manifest_path_is_not_matched_by_a_parquet_page_glob(status_run: RawRun) -> None:
    # GIVEN a run
    # WHEN the manifest path is built
    name = manifest_path(BUCKET, status_run).rsplit("/", 1)[-1]
    # THEN a reader globbing page=*.parquet does not pick it up
    assert not name.startswith("page=")
    assert not name.endswith(".parquet")


# --- RawRun ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "run_key",
    ["20261026T101000", "manual__x", "a.b-c_d", "0", "Run-1.2"],
)
def test_raw_run_accepts_path_safe_run_keys(run_key: str) -> None:
    # GIVEN a run key of [0-9A-Za-z_.-] starting with an alphanumeric
    # WHEN a RawRun is created
    run = RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key=run_key)
    # THEN it is kept unchanged
    assert run.run_key == run_key


@pytest.mark.parametrize(
    "run_key",
    [
        "",
        "..",
        "../escape",
        ".hidden",
        "_leading",
        "-leading",
        "a/b",
        "a\\b",
        "a b",
        "run=1",
        "scheduled__2026-10-26T01:10:00+00:00",
        "k\n",
        "\uc5b8\uc81c",
    ],
)
def test_raw_run_rejects_run_keys_that_are_not_path_safe(run_key: str) -> None:
    # GIVEN a run key with a separator, traversal, leading punctuation, or another unsafe char
    # WHEN a RawRun is created
    # THEN it raises ValueError
    with pytest.raises(ValueError, match="run_key must be path-safe"):
        RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key=run_key)


def test_raw_run_cannot_be_mutated_after_validation(status_run: RawRun) -> None:
    # GIVEN a constructed (validated) RawRun
    # WHEN its run_key is reassigned
    # THEN it raises, so the validated key cannot be replaced
    with pytest.raises(dataclasses.FrozenInstanceError):
        status_run.run_key = "../x"  # type: ignore[misc]


def test_equal_raw_runs_are_equal_and_hash_equal() -> None:
    # GIVEN two RawRuns with the same fields
    a = RawRun(dataset=Dataset.CHARGER_INFO, dt=date(2026, 10, 26), run_key="k")
    b = RawRun(dataset=Dataset.CHARGER_INFO, dt=date(2026, 10, 26), run_key="k")
    # WHEN compared
    # THEN they are equal and collapse to one set member
    assert a == b
    assert len({a, b}) == 1


# --- partition_date -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("collected_at", "expected"),
    [
        (datetime(2026, 10, 26, 14, 59, 59, tzinfo=UTC), date(2026, 10, 26)),  # 23:59:59 KST
        (datetime(2026, 10, 26, 15, 0, 0, tzinfo=UTC), date(2026, 10, 27)),  # 00:00 KST
        (datetime(2026, 10, 26, 0, 30, 0, tzinfo=UTC), date(2026, 10, 26)),  # 09:30 KST
        (
            datetime(2026, 10, 27, 0, 10, 0, tzinfo=timezone(timedelta(hours=9))),
            date(2026, 10, 27),
        ),
    ],
)
def test_partition_date_is_the_kst_date_of_collected_at(
    collected_at: datetime, expected: date
) -> None:
    # GIVEN a timezone-aware collection time
    # WHEN its partition date is computed
    # THEN it is the calendar date in KST, regardless of the input's offset
    assert partition_date(collected_at) == expected


def test_partition_date_rejects_a_naive_time() -> None:
    # GIVEN a collection time without a timezone
    # WHEN / THEN it is rejected instead of guessing KST or UTC
    with pytest.raises(ValueError, match="timezone-aware"):
        partition_date(datetime(2026, 10, 26, 0, 10, 0))
