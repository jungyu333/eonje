"""DB tests for eonje.storage.call_log: CallLogWriter and count_calls against real Postgres."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from sqlalchemy import Connection, select, text
from sqlalchemy.orm import Session, sessionmaker
from tenacity import wait_none

from eonje.client.evcharger import EvChargerClient
from eonje.client.types import CallRecord
from eonje.config import Settings
from eonje.storage.call_log import CallLogWriter, count_calls
from eonje.storage.db import session_scope
from eonje.storage.models import ApiCallLog

pytestmark = pytest.mark.db

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "evcharger"
KST = timezone(timedelta(hours=9))
T0 = datetime(2026, 10, 27, 0, 0, tzinfo=UTC)
JOB = "fetch-delta"
RUN_ID = "scheduled__2026-10-27T00:00:00+00:00"


def _record(**overrides: object) -> CallRecord:
    base = CallRecord(
        operation="getChargerStatus",
        page_no=2,
        params={"period": "10", "pageNo": "2", "numOfRows": "500", "dataType": "JSON"},
        called_at=T0,
        elapsed_ms=123,
        attempt=1,
        http_status=200,
        result_code="00",
        total_count=1234,
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


def _writer(factory: sessionmaker[Session]) -> CallLogWriter:
    return CallLogWriter(factory, job=JOB, run_id=RUN_ID)


def _rows(factory: sessionmaker[Session]) -> list[ApiCallLog]:
    with factory() as session:
        return list(session.scalars(select(ApiCallLog).order_by(ApiCallLog.id)))


def _count_rows(conn: Connection) -> int:
    return conn.execute(text("SELECT count(*) FROM api_call_log")).scalar_one()


# --- CallLogWriter ----------------------------------------------------------


def test_single_record_is_stored_with_every_column_and_params_round_trip_as_dict(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a writer and a successful call record
    record = _record()

    # WHEN the record is written
    _writer(session_factory)(record)

    # THEN exactly one row holds every column as given, params back as a dict
    (row,) = _rows(session_factory)
    assert row.called_at == T0
    assert row.job == JOB
    assert row.run_id == RUN_ID
    assert row.operation == "getChargerStatus"
    assert row.page_no == 2
    assert row.attempt == 1
    assert row.params == {"period": "10", "pageNo": "2", "numOfRows": "500", "dataType": "JSON"}
    assert row.http_status == 200
    assert row.result_code == "00"
    assert row.total_count == 1234
    assert row.elapsed_ms == 123


def test_three_retry_attempts_produce_three_rows_with_attempts_one_two_three(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a writer and three attempts at the same page
    writer = _writer(session_factory)

    # WHEN each attempt is written
    for n in (1, 2, 3):
        writer(_record(attempt=n, called_at=T0 + timedelta(seconds=n)))

    # THEN there are three rows, one per attempt
    assert [r.attempt for r in _rows(session_factory)] == [1, 2, 3]


def test_failed_call_with_null_status_code_and_total_is_stored(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a record of a call that failed before any response (transport error)
    record = _record(http_status=None, result_code=None, total_count=None)

    # WHEN it is written
    _writer(session_factory)(record)

    # THEN the row exists with the nullable columns as NULL
    (row,) = _rows(session_factory)
    assert row.http_status is None
    assert row.result_code is None
    assert row.total_count is None


def test_logged_row_survives_rollback_of_a_separate_task_session(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a call record written by the writer
    _writer(session_factory)(_record())

    # WHEN a task's own session_scope writes its own row, then fails and rolls back
    with (
        pytest.raises(RuntimeError, match="task failed"),
        session_scope(session_factory) as session,
    ):
        session.execute(
            text(
                "INSERT INTO api_call_log"
                " (called_at, job, run_id, operation, page_no, attempt, params, elapsed_ms)"
                " VALUES (:at, 'task', 'task-run', 'getChargerStatus', 1, 1, '{}'::jsonb, 0)"
            ),
            {"at": T0},
        )
        raise RuntimeError("task failed")

    # THEN the task's row is gone but the call log row written earlier remains
    assert [r.run_id for r in _rows(session_factory)] == [RUN_ID]


def test_writer_rejects_naive_called_at_and_writes_nothing(
    session_factory: sessionmaker[Session], connection: Connection
) -> None:
    # GIVEN a record whose called_at has no timezone
    record = _record(called_at=datetime(2026, 10, 27, 9, 0))

    # WHEN it is written THEN ValueError is raised
    with pytest.raises(ValueError, match="called_at"):
        _writer(session_factory)(record)

    # AND no row was stored
    assert _count_rows(connection) == 0


def test_writing_the_same_record_twice_stores_two_rows(
    session_factory: sessionmaker[Session], connection: Connection
) -> None:
    # GIVEN a writer (each HTTP attempt counts against the daily limit, so no dedupe)
    writer = _writer(session_factory)

    # WHEN the identical record is written twice
    writer(_record())
    writer(_record())

    # THEN both rows are kept
    assert _count_rows(connection) == 2


# --- count_calls -------------------------------------------------------------


def test_count_calls_on_empty_table_is_zero(session_factory: sessionmaker[Session]) -> None:
    # GIVEN no logged calls
    # WHEN counting over a day
    with session_factory() as session:
        n = count_calls(session, T0, T0 + timedelta(days=1))

    # THEN the count is zero
    assert n == 0


def test_count_calls_includes_start_and_excludes_end(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN calls exactly at start, inside, and exactly at end of a window
    start, end = T0, T0 + timedelta(hours=1)
    writer = _writer(session_factory)
    for at in (start - timedelta(microseconds=1), start, start + timedelta(minutes=30), end):
        writer(_record(called_at=at))

    # WHEN counting over [start, end)
    with session_factory() as session:
        n = count_calls(session, start, end)

    # THEN only the start and inside calls are counted
    assert n == 2


def test_count_calls_counts_failed_calls(session_factory: sessionmaker[Session]) -> None:
    # GIVEN one successful call and one 500 response
    writer = _writer(session_factory)
    writer(_record())
    writer(_record(attempt=2, http_status=500, result_code=None, total_count=None))

    # WHEN counting
    with session_factory() as session:
        n = count_calls(session, T0, T0 + timedelta(seconds=1))

    # THEN both count against the limit
    assert n == 2


def test_count_calls_gives_same_result_for_kst_and_utc_windows_of_same_instants(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN calls around a KST day boundary (2026-10-27 00:00 KST == 2026-10-26 15:00 UTC)
    writer = _writer(session_factory)
    kst_start = datetime(2026, 10, 27, 0, 0, tzinfo=KST)
    kst_end = kst_start + timedelta(days=1)
    for at in (
        kst_start - timedelta(minutes=1),
        kst_start,
        kst_start + timedelta(hours=12),
        kst_end,
    ):
        writer(_record(called_at=at.astimezone(UTC)))
    utc_start, utc_end = kst_start.astimezone(UTC), kst_end.astimezone(UTC)

    # WHEN counting with the window expressed in KST and in UTC
    with session_factory() as session:
        n_kst = count_calls(session, kst_start, kst_end)
        n_utc = count_calls(session, utc_start, utc_end)

    # THEN both give the same count
    assert n_kst == n_utc == 2


def test_count_calls_is_idempotent(session_factory: sessionmaker[Session]) -> None:
    # GIVEN one logged call
    _writer(session_factory)(_record())

    # WHEN counting twice over the same window
    with session_factory() as session:
        first = count_calls(session, T0, T0 + timedelta(hours=1))
        second = count_calls(session, T0, T0 + timedelta(hours=1))

    # THEN the results match
    assert first == second == 1


@pytest.mark.parametrize("which", ["start", "end"])
def test_count_calls_rejects_naive_bounds(
    session_factory: sessionmaker[Session], which: str
) -> None:
    # GIVEN a window with one naive bound
    naive = datetime(2026, 10, 27, 9, 0)
    aware = datetime(2026, 10, 28, 0, 0, tzinfo=UTC)
    start, end = (naive, aware) if which == "start" else (T0, naive)

    # WHEN counting THEN ValueError names the naive bound
    with session_factory() as session, pytest.raises(ValueError, match=which):
        count_calls(session, start, end)


# --- client wiring -----------------------------------------------------------


def test_client_with_call_log_writer_logs_each_attempt_of_500_then_200(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a client whose transport answers 500 then a normal page
    responses = [
        httpx.Response(500, text="server error"),
        httpx.Response(
            200, text=(FIXTURES / "getChargerStatus_page.json").read_text(encoding="utf-8")
        ),
    ]
    transport = httpx.MockTransport(lambda _req: responses.pop(0))
    settings = Settings(
        api_service_key="test-service-key",  # type: ignore[arg-type]
        api_base_url="https://api.test/B552584/EvCharger",
        page_size=500,
        api_max_attempts=3,
    )
    client = EvChargerClient(
        httpx.Client(transport=transport),
        settings,
        on_call=_writer(session_factory),
        wait=wait_none(),
    )

    # WHEN page 1 is fetched
    client.fetch_page("getChargerStatus", 1, {"period": "10"})

    # THEN two rows are logged: the 500 attempt and the successful retry, without the key
    rows = _rows(session_factory)
    assert [(r.attempt, r.http_status) for r in rows] == [(1, 500), (2, 200)]
    assert rows[0].result_code is None
    assert rows[1].result_code == "00"
    assert all("serviceKey" not in r.params for r in rows)
    assert all(r.job == JOB and r.run_id == RUN_ID for r in rows)
