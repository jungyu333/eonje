import math

import pytest

from eonje.load.policy import skip_reason
from eonje.storage.raw.run import MANIFEST_VERSION, RunManifest

PAGE_SIZE = 9999
RATIO = 0.9  # the Settings default for master_min_row_ratio


def _manifest(
    *, total_count: int, row_count: int | None = None, pages: int | None = None
) -> RunManifest:
    """A charger_info manifest. Defaults describe a complete run that received every row."""
    expected_pages = max(1, math.ceil(total_count / PAGE_SIZE))
    received = expected_pages if pages is None else pages
    return RunManifest(
        version=MANIFEST_VERSION,
        dataset="charger_info",
        operation="getChargerInfo",
        dt="2026-11-10",
        run_key="scheduled__2026-11-09T18:11:00+00:00",
        collected_at="2026-11-09T18:11:30+00:00",
        total_count=total_count,
        row_count=total_count if row_count is None else row_count,
        page_size=PAGE_SIZE,
        expected_pages=expected_pages,
        pages=list(range(1, received + 1)),
    )


# --- reasons ---


def test_complete_run_with_about_as_many_rows_as_the_dimension_is_loaded() -> None:
    # GIVEN a complete 53-page run slightly larger than the current dimension
    manifest = _manifest(total_count=529_120)

    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=529_000, min_row_ratio=RATIO)

    # THEN there is no reason to skip
    assert reason is None


def test_empty_run_is_skipped_as_empty() -> None:
    # GIVEN a run whose single page returned 0 rows, against a populated dimension
    manifest = _manifest(total_count=0)

    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=529_000, min_row_ratio=RATIO)

    # THEN it is skipped as empty, not reported as a row-count drop
    assert reason == "empty run (0 rows)"


def test_truncated_run_is_skipped_even_when_its_row_count_passes_the_ratio() -> None:
    # GIVEN a run cut at 52 of 53 pages whose rows are still above 90% of the dimension
    manifest = _manifest(total_count=529_941, row_count=52 * PAGE_SIZE, pages=52)

    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=529_000, min_row_ratio=RATIO)

    # THEN it is skipped as truncated, naming received and expected pages
    assert reason == "truncated run (52 of 53 pages)"


def test_run_whose_row_count_dropped_below_the_ratio_is_skipped() -> None:
    # GIVEN a complete run with 400,000 rows against 529,000 current chargers
    manifest = _manifest(total_count=400_000)

    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=529_000, min_row_ratio=RATIO)

    # THEN it is skipped as a drop, naming both counts and the ratio
    assert reason == (
        "row count dropped: 400000 rows against 529000 current chargers (min ratio 0.9)"
    )


# --- first load ---


def test_first_load_does_not_compare_row_counts() -> None:
    # GIVEN an empty dimension and a small complete run
    manifest = _manifest(total_count=12)

    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=0, min_row_ratio=RATIO)

    # THEN it is loaded
    assert reason is None


@pytest.mark.parametrize(
    ("manifest", "expected"),
    [
        (_manifest(total_count=0), "empty run (0 rows)"),
        (
            _manifest(total_count=529_941, row_count=PAGE_SIZE, pages=1),
            "truncated run (1 of 53 pages)",
        ),
    ],
    ids=["empty", "truncated"],
)
def test_first_load_still_skips_an_empty_or_truncated_run(
    manifest: RunManifest, expected: str
) -> None:
    # GIVEN an empty dimension and a run that is empty or truncated
    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=0, min_row_ratio=RATIO)

    # THEN only the row-count comparison is waived; the run is still skipped
    assert reason == expected


# --- ratio boundary ---


@pytest.mark.parametrize(
    ("current_rows", "ratio", "row_count"),
    [
        (1000, 0.9, 900),
        (529_000, 0.9, 476_100),
        (10, 1.0, 10),
        (100, 0.57, 57),  # float product 56.999... falls below the exact threshold
        (100, 0.55, 55),  # float product 55.000...01 overshoots the exact threshold
        (300, 0.81, 243),  # float product 243.000...03 overshoots the exact threshold
    ],
)
def test_run_with_exactly_the_ratio_of_current_rows_is_loaded(
    current_rows: int, ratio: float, row_count: int
) -> None:
    # GIVEN a complete run whose rows are exactly current_rows x ratio
    manifest = _manifest(total_count=row_count)

    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=current_rows, min_row_ratio=ratio)

    # THEN it is loaded
    assert reason is None


@pytest.mark.parametrize(
    ("current_rows", "ratio", "row_count"),
    [
        (1000, 0.9, 899),
        (529_000, 0.9, 476_099),
        (10, 1.0, 9),
        (100, 0.57, 56),
        (100, 0.55, 54),
    ],
)
def test_run_one_row_short_of_the_ratio_is_skipped(
    current_rows: int, ratio: float, row_count: int
) -> None:
    # GIVEN a complete run one row below current_rows x ratio
    manifest = _manifest(total_count=row_count)

    # WHEN deciding whether to load it
    reason = skip_reason(manifest, current_rows=current_rows, min_row_ratio=ratio)

    # THEN it is skipped as a row-count drop
    assert reason is not None
    assert reason.startswith("row count dropped:")


def test_fractional_threshold_is_met_only_by_the_next_whole_row() -> None:
    # GIVEN 1001 current chargers at ratio 0.9, a threshold of 900.9 rows
    # WHEN deciding for complete runs of 901 and 900 rows
    at_threshold = skip_reason(_manifest(total_count=901), current_rows=1001, min_row_ratio=RATIO)
    below = skip_reason(_manifest(total_count=900), current_rows=1001, min_row_ratio=RATIO)

    # THEN 901 rows are loaded and 900 rows are skipped
    assert at_threshold is None
    assert below is not None
    assert below.startswith("row count dropped:")
