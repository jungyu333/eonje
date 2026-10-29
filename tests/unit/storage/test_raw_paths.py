import dataclasses
import re
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from eonje.storage.raw_paths import (
    MAX_PAGE_NO,
    Dataset,
    RawRun,
    manifest_path,
    page_path,
    partition_date,
    run_key_from_run_id,
    run_prefix,
)

BUCKET = "eonje-raw"
DIGEST_SUFFIX = r"-[0-9a-f]{8}"


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


# --- run_key_from_run_id --------------------------------------------------------------


@pytest.mark.parametrize(
    ("run_id", "expected"),
    [
        ("scheduled__2026-10-26T01:10:00+00:00", "scheduled__2026-10-26T01-10-00-00-00"),
        ("manual__2026-10-26T01:10:00.123456+00:00", "manual__2026-10-26T01-10-00.123456-00-00"),
        ("backfill__x/y z", "backfill__x-y-z"),
        ("a\\b", "a-b"),
        ("id언제", "id--"),
    ],
)
def test_run_key_from_run_id_replaces_each_unsafe_char_with_a_hyphen(
    run_id: str, expected: str
) -> None:
    # GIVEN an Airflow-style run_id containing ':', '+', '/', space, backslash or non-ASCII
    # WHEN it is converted to a run key
    key = run_key_from_run_id(run_id)
    # THEN every char outside [0-9A-Za-z_.-] becomes '-', '.', '_', '-' are kept,
    # and an 8-hex-char digest suffix marks that the run_id was changed
    assert re.fullmatch(re.escape(expected) + DIGEST_SUFFIX, key)


@pytest.mark.parametrize(
    ("run_id", "expected"),
    [
        ("-leading", "r-leading"),
        ("_x", "r_x"),
        (".x", "r.x"),
        ("..", "r.."),
        (":x", "r-x"),
        ("언제", "r--"),
    ],
)
def test_run_key_from_run_id_prefixes_r_when_the_key_would_not_start_alphanumeric(
    run_id: str, expected: str
) -> None:
    # GIVEN a run_id whose sanitized form starts with '-', '_' or '.'
    # WHEN it is converted to a run key
    key = run_key_from_run_id(run_id)
    # THEN 'r' is prepended so the key starts with an alphanumeric, plus a digest suffix
    assert re.fullmatch(re.escape(expected) + DIGEST_SUFFIX, key)


def test_run_key_from_run_id_rejects_an_empty_run_id() -> None:
    # GIVEN an empty run_id
    # WHEN / THEN it raises instead of producing an empty run= directory
    with pytest.raises(ValueError, match="run_id must not be empty"):
        run_key_from_run_id("")


@pytest.mark.parametrize("run_id", ["20261026T001000Z", "manual__x", "a.b-c_d", "0", "Run-1.2"])
def test_run_key_from_run_id_keeps_an_already_safe_run_id_unchanged(run_id: str) -> None:
    # GIVEN a run_id that is already a valid run key
    # WHEN it is converted to a run key
    key = run_key_from_run_id(run_id)
    # THEN it is returned as is
    assert key == run_id


@pytest.mark.parametrize(
    "run_id",
    [
        "scheduled__2026-10-26T01:10:00+00:00",
        "manual__2026-10-26T01:10:00.123456+00:00",
        "backfill__x/y z",
        "-leading",
        "_x",
        ".x",
        "..",
        "../escape",
        "run=1",
        "k\n",
        "a\\b",
        "언제",
        "20261026T001000Z",
    ],
)
def test_run_key_from_run_id_always_yields_a_key_accepted_by_raw_run(run_id: str) -> None:
    # GIVEN any non-empty run_id, including ones RawRun rejects directly
    # WHEN it is converted and used to build a RawRun
    run = RawRun(
        dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key=run_key_from_run_id(run_id)
    )
    # THEN construction succeeds and the run prefix has no extra path segment
    assert run_prefix(BUCKET, run).count("/") == 3


@pytest.mark.parametrize(
    "run_id",
    ["scheduled__2026-10-26T01:10:00+00:00", "-leading", "backfill__x/y z", "20261026T001000Z"],
)
def test_run_key_from_run_id_is_deterministic(run_id: str) -> None:
    # GIVEN the same run_id (a rerun of the same Airflow run)
    # WHEN it is converted twice
    # THEN both keys are identical, so the rerun overwrites the same run= directory
    assert run_key_from_run_id(run_id) == run_key_from_run_id(run_id)


@pytest.mark.parametrize(
    "run_id",
    ["scheduled__2026-10-26T01:10:00+00:00", "-leading", "_x", ".x", "backfill__x/y z"],
)
def test_run_key_from_run_id_is_idempotent(run_id: str) -> None:
    # GIVEN a run key already produced from a run_id
    key = run_key_from_run_id(run_id)
    # WHEN it is converted again
    # THEN it is unchanged
    assert run_key_from_run_id(key) == key


def test_run_key_from_run_id_keeps_ids_differing_only_in_unsafe_chars_distinct() -> None:
    # GIVEN run_ids that sanitize to the same text "a-b"
    run_ids = ["a:b", "a/b", "a-b"]
    # WHEN each is converted to a run key
    keys = [run_key_from_run_id(run_id) for run_id in run_ids]
    # THEN all keys differ, so the runs never share a run= directory
    assert len(set(keys)) == len(run_ids)


def test_run_key_from_run_id_keeps_a_prefixed_id_distinct_from_its_literal_form() -> None:
    # GIVEN "-x" (gets the 'r' prefix) and "r-x" (already safe)
    # WHEN both are converted to run keys
    prefixed = run_key_from_run_id("-x")
    literal = run_key_from_run_id("r-x")
    # THEN they differ, so the 'r' prefix cannot collide with a real run_id
    assert prefixed != literal
