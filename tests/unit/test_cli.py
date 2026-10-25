import pytest

from eonje.cli import registry
from eonje.cli.app import main
from eonje.cli.registry import JOBS
from eonje.config import Settings


def test_known_job_returns_not_implemented(
    api_key_env: str, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN valid settings and a stub job that is not implemented yet
    # WHEN the job is run
    code = main(["fetch-delta", "--run-id", "manual__2026-10-13"])

    # THEN the CLI exits 2 with the job's not-implemented message
    assert code == 2
    assert "not implemented" in capsys.readouterr().err


def test_unknown_job_is_rejected() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["no-such-job", "--run-id", "x"])
    assert exc.value.code == 2


def test_run_id_is_required() -> None:
    with pytest.raises(SystemExit):
        main(["fetch-delta"])


def test_every_registered_job_is_callable() -> None:
    assert all(callable(job) for job in JOBS.values())


def test_job_receives_run_id_and_loaded_settings(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a registered job that records its arguments
    calls: list[tuple[str, Settings]] = []

    def fake_job(run_id: str, settings: Settings) -> int:
        calls.append((run_id, settings))
        return 0

    monkeypatch.setitem(registry.JOBS, "fetch-delta", fake_job)

    # WHEN the CLI dispatches it
    main(["fetch-delta", "--run-id", "manual__x"])

    # THEN the job gets the run_id and the Settings loaded from env
    assert len(calls) == 1
    run_id, settings = calls[0]
    assert run_id == "manual__x"
    assert isinstance(settings, Settings)
    assert settings.api_service_key.get_secret_value() == api_key_env


@pytest.mark.parametrize("job_code", [0, 1])
def test_job_exit_code_is_returned_unchanged(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, job_code: int
) -> None:
    # GIVEN a job that returns a specific exit code
    monkeypatch.setitem(registry.JOBS, "fetch-delta", lambda run_id, settings: job_code)

    # WHEN the CLI dispatches it
    code = main(["fetch-delta", "--run-id", "manual__x"])

    # THEN the CLI returns that code
    assert code == job_code


def test_invalid_settings_exit_2_without_running_the_job(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN no API key (settings invalid) and a job that must not run
    called = False

    def fake_job(run_id: str, settings: Settings) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setitem(registry.JOBS, "fetch-delta", fake_job)

    # WHEN the CLI is invoked
    code = main(["fetch-delta", "--run-id", "manual__x"])

    # THEN it exits 2, reports the settings error, and skips the job
    assert code == 2
    assert "invalid settings" in capsys.readouterr().err
    assert called is False


def test_same_run_id_twice_yields_the_same_exit_code(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a deterministic job
    monkeypatch.setitem(registry.JOBS, "fetch-delta", lambda run_id, settings: 0)

    # WHEN the same run is invoked twice
    first = main(["fetch-delta", "--run-id", "manual__x"])
    second = main(["fetch-delta", "--run-id", "manual__x"])

    # THEN both invocations agree
    assert first == second == 0
