"""load-dims job: per-run results -> process exit code. The flow itself is tested in tests/spark."""

from eonje.cli import exit_codes
from eonje.cli.jobs.load_dims import exit_code
from eonje.load.dims import RunResult
from eonje.storage.lake.dims import MergeCounts


def _applied(run_key: str) -> RunResult:
    counts = MergeCounts(closed=0, opened=1)
    return RunResult(run_key, skipped=None, stations=counts, chargers=counts)


def _skipped(run_key: str) -> RunResult:
    return RunResult(run_key, skipped="row count dropped", stations=None, chargers=None)


def test_no_run_seen_exits_ok() -> None:
    # GIVEN no run after the watermark
    results: list[RunResult] = []
    # WHEN the results are turned into an exit code
    code = exit_code(results)
    # THEN the job succeeds
    assert code == exit_codes.OK


def test_last_run_applied_exits_ok_even_after_a_skipped_run() -> None:
    # GIVEN an earlier skipped run followed by an applied run
    results = [_skipped("run-1"), _applied("run-2")]
    # WHEN the results are turned into an exit code
    code = exit_code(results)
    # THEN the job succeeds
    assert code == exit_codes.OK


def test_last_run_skipped_exits_skipped_even_after_an_applied_run() -> None:
    # GIVEN an earlier applied run followed by a skipped run
    results = [_applied("run-1"), _skipped("run-2")]
    # WHEN the results are turned into an exit code
    code = exit_code(results)
    # THEN the job tells Airflow it was skipped
    assert code == exit_codes.SKIPPED
