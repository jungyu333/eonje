from datetime import UTC, datetime, timedelta

import pytest

from eonje.cli import registry
from eonje.cli.app import main
from eonje.cli.context import JobContext
from eonje.cli.registry import JOBS
from eonje.config import Settings

RUN_ID = "manual__x"
RUN_AFTER = "2026-10-28T01:10:00+00:00"
RUN_AFTER_DT = datetime(2026, 10, 28, 1, 10, tzinfo=UTC)


def _argv(*, run_id: str = RUN_ID, run_after: str = RUN_AFTER) -> list[str]:
    return ["fetch-delta", "--run-id", run_id, "--run-after", run_after]


def _record_contexts(monkeypatch: pytest.MonkeyPatch) -> list[JobContext]:
    calls: list[JobContext] = []

    def fake_job(ctx: JobContext) -> int:
        calls.append(ctx)
        return 0

    monkeypatch.setitem(registry.JOBS, "fetch-delta", fake_job)
    return calls


def test_known_job_returns_not_implemented(
    api_key_env: str, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN valid settings and a stub job that is not implemented yet
    # (only valid while some job is still a stub; fetch-master lands in stage 3)
    # WHEN the job is run
    argv = _argv(run_id="manual__2026-10-13")
    argv[0] = "fetch-master"
    code = main(argv)

    # THEN the CLI exits 2 with the job's not-implemented message
    assert code == 2
    assert "not implemented" in capsys.readouterr().err


def test_unknown_job_is_rejected() -> None:
    # GIVEN a job name that is not registered
    # WHEN the CLI is invoked with otherwise valid arguments
    with pytest.raises(SystemExit) as exc:
        main(["no-such-job", "--run-id", "x", "--run-after", RUN_AFTER])

    # THEN argparse rejects it with exit code 2
    assert exc.value.code == 2


def test_run_id_is_required() -> None:
    # GIVEN arguments without --run-id
    # WHEN the CLI is invoked
    with pytest.raises(SystemExit) as exc:
        main(["fetch-delta", "--run-after", RUN_AFTER])

    # THEN argparse rejects it with exit code 2
    assert exc.value.code == 2


def test_run_after_is_required(api_key_env: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # GIVEN arguments without --run-after and a job that must not run
    calls = _record_contexts(monkeypatch)

    # WHEN the CLI is invoked
    with pytest.raises(SystemExit) as exc:
        main(["fetch-delta", "--run-id", RUN_ID])

    # THEN argparse rejects it with exit code 2 and the job never runs
    assert exc.value.code == 2
    assert calls == []


def test_naive_run_after_is_rejected_with_timezone_message(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN a --run-after value without a UTC offset
    calls = _record_contexts(monkeypatch)

    # WHEN the CLI is invoked
    with pytest.raises(SystemExit) as exc:
        main(_argv(run_after="2026-10-28T01:10:00"))

    # THEN it exits 2, the error mentions the timezone, and the job never runs
    assert exc.value.code == 2
    assert "timezone" in capsys.readouterr().err
    assert calls == []


@pytest.mark.parametrize("value", ["yesterday", "", "2026-13-01T00:00:00+00:00"])
def test_non_iso_run_after_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a --run-after value that is not an ISO 8601 datetime
    calls = _record_contexts(monkeypatch)

    # WHEN the CLI is invoked
    with pytest.raises(SystemExit) as exc:
        main(_argv(run_after=value))

    # THEN argparse rejects it with exit code 2 and the job never runs
    assert exc.value.code == 2
    assert calls == []


def test_run_after_with_z_suffix_is_parsed_as_utc(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a --run-after value in Z notation
    calls = _record_contexts(monkeypatch)

    # WHEN the CLI dispatches the job
    code = main(_argv(run_after="2026-10-28T01:10:00Z"))

    # THEN the job receives that instant with a zero UTC offset
    assert code == 0
    run_after = calls[0].run_after
    assert run_after == RUN_AFTER_DT
    assert run_after.utcoffset() == timedelta(0)


def test_run_after_with_kst_offset_is_the_same_instant(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a --run-after value with a +09:00 offset for 01:10 UTC
    calls = _record_contexts(monkeypatch)

    # WHEN the CLI dispatches the job
    code = main(_argv(run_after="2026-10-28T10:10:00+09:00"))

    # THEN the job receives the same instant, with the offset preserved
    assert code == 0
    run_after = calls[0].run_after
    assert run_after == RUN_AFTER_DT
    assert run_after.utcoffset() == timedelta(hours=9)


def test_every_registered_job_is_callable() -> None:
    assert all(callable(job) for job in JOBS.values())


def test_job_receives_context_with_run_id_run_after_and_settings(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a registered job that records its context
    calls = _record_contexts(monkeypatch)

    # WHEN the CLI dispatches it
    main(_argv())

    # THEN the job gets exactly one JobContext carrying the CLI arguments and env settings
    assert len(calls) == 1
    ctx = calls[0]
    assert isinstance(ctx, JobContext)
    assert ctx.run_id == RUN_ID
    assert ctx.run_after == RUN_AFTER_DT
    assert isinstance(ctx.settings, Settings)
    assert ctx.settings.api_service_key.get_secret_value() == api_key_env


@pytest.mark.parametrize("job_code", [0, 1])
def test_job_exit_code_is_returned_unchanged(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, job_code: int
) -> None:
    # GIVEN a job that returns a specific exit code
    def fake_job(ctx: JobContext) -> int:
        return job_code

    monkeypatch.setitem(registry.JOBS, "fetch-delta", fake_job)

    # WHEN the CLI dispatches it
    code = main(_argv())

    # THEN the CLI returns that code
    assert code == job_code


def test_invalid_settings_exit_2_without_running_the_job(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN no API key (settings invalid) and a job that must not run
    calls = _record_contexts(monkeypatch)

    # WHEN the CLI is invoked
    code = main(_argv())

    # THEN it exits 2, reports the settings error, and skips the job
    assert code == 2
    assert "invalid settings" in capsys.readouterr().err
    assert calls == []


def test_same_run_twice_yields_the_same_exit_code_and_context(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a deterministic job that records its context
    calls = _record_contexts(monkeypatch)

    # WHEN the same run is invoked twice
    first = main(_argv())
    second = main(_argv())

    # THEN both invocations agree on exit code, run_id and run_after
    assert first == second == 0
    assert calls[0].run_id == calls[1].run_id
    assert calls[0].run_after == calls[1].run_after
