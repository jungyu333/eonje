import dataclasses
from datetime import UTC, datetime, timedelta, timezone

import pytest

from eonje.cli.context import JobContext
from eonje.config import Settings

RUN_ID = "manual__x"
RUN_AFTER_DT = datetime(2026, 10, 28, 1, 10, tzinfo=UTC)


def _settings() -> Settings:
    return Settings(api_service_key="test-service-key")  # type: ignore[arg-type]


def test_job_context_accepts_aware_run_after_and_non_empty_run_id() -> None:
    # GIVEN valid inputs with a non-UTC offset
    run_after = datetime(2026, 10, 28, 10, 10, tzinfo=timezone(timedelta(hours=9)))

    # WHEN a JobContext is built directly
    ctx = JobContext(run_id=RUN_ID, run_after=run_after, settings=_settings())

    # THEN the values are kept as given
    assert ctx.run_id == RUN_ID
    assert ctx.run_after == RUN_AFTER_DT


def test_job_context_rejects_naive_run_after() -> None:
    # GIVEN a naive run_after
    naive = datetime(2026, 10, 28, 1, 10)

    # WHEN / THEN building a JobContext raises ValueError
    with pytest.raises(ValueError, match="run_after"):
        JobContext(run_id=RUN_ID, run_after=naive, settings=_settings())


def test_job_context_rejects_empty_run_id() -> None:
    # GIVEN an empty run_id
    # WHEN / THEN building a JobContext raises ValueError
    with pytest.raises(ValueError, match="run_id"):
        JobContext(run_id="", run_after=RUN_AFTER_DT, settings=_settings())


@pytest.mark.parametrize("run_id", [" ", "\t", "\n", "   "])
def test_job_context_rejects_blank_run_id(run_id: str) -> None:
    # GIVEN a run_id made only of whitespace (never produced by Airflow)
    # WHEN / THEN building a JobContext raises instead of creating a meaningless run key
    with pytest.raises(ValueError, match="run_id"):
        JobContext(run_id=run_id, run_after=RUN_AFTER_DT, settings=_settings())


def test_job_context_keeps_surrounding_whitespace_of_a_real_run_id() -> None:
    # GIVEN a run_id with leading/trailing spaces around real content
    run_id = " manual__x "

    # WHEN a JobContext is built
    ctx = JobContext(run_id=run_id, run_after=RUN_AFTER_DT, settings=_settings())

    # THEN the original is kept verbatim (it is matched against Airflow's run_id)
    assert ctx.run_id == run_id


def test_job_context_cannot_be_mutated_after_construction() -> None:
    # GIVEN a valid JobContext
    ctx = JobContext(run_id=RUN_ID, run_after=RUN_AFTER_DT, settings=_settings())

    # WHEN / THEN reassigning a field raises FrozenInstanceError
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.run_id = "other"  # type: ignore[misc]
