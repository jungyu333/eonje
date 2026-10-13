import pytest

from eonje.cli.app import main
from eonje.cli.registry import JOBS


def test_known_job_returns_not_implemented() -> None:
    code = main(["fetch-delta", "--run-id", "manual__2026-10-13"])
    assert code == 2


def test_unknown_job_is_rejected() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["no-such-job", "--run-id", "x"])
    assert exc.value.code == 2


def test_run_id_is_required() -> None:
    with pytest.raises(SystemExit):
        main(["fetch-delta"])


def test_every_registered_job_is_callable() -> None:
    assert all(callable(job) for job in JOBS.values())
