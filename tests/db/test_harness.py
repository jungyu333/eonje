"""Sanity checks for the DB test harness in tests/db/conftest.py."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import Connection, text
from sqlalchemy.orm import Session, sessionmaker

pytestmark = pytest.mark.db

HEAD_REVISION = "30e8abb95390"

_INSERT_CALL_LOG = text(
    """
    INSERT INTO api_call_log
        (called_at, job, run_id, operation, page_no, attempt, params, elapsed_ms)
    VALUES
        (:called_at, 'harness', :run_id, 'getChargerStatus', 1, 1, '{}'::jsonb, 0)
    """
)
_FIXED_CALLED_AT = datetime(2026, 10, 27, 0, 0, tzinfo=UTC)


def _count_call_log(conn: Connection) -> int:
    return conn.execute(text("SELECT count(*) FROM api_call_log")).scalar_one()


def test_api_call_log_table_exists_in_temporary_schema(connection: Connection, schema: str) -> None:
    # GIVEN the session schema migrated to head by the engine fixture
    # WHEN information_schema is queried for that schema
    tables = set(
        connection.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema = :schema"),
            {"schema": schema},
        ).scalars()
    )

    # THEN api_call_log lives in the temporary schema
    assert "api_call_log" in tables


def test_search_path_points_at_temporary_schema(connection: Connection, schema: str) -> None:
    # GIVEN a connection from the harness engine
    # WHEN the current schema is asked
    current = connection.execute(text("SELECT current_schema()")).scalar_one()

    # THEN unqualified names resolve to the temporary schema, not public
    assert current == schema


def test_alembic_version_is_at_head(connection: Connection, schema: str) -> None:
    # GIVEN the session schema migrated to head
    # WHEN the alembic_version table in that schema is read
    revisions = (
        connection.execute(text(f'SELECT version_num FROM "{schema}".alembic_version'))
        .scalars()
        .all()
    )

    # THEN exactly the head revision is recorded
    assert revisions == [HEAD_REVISION]


@pytest.mark.parametrize("run_id", ["isolation-first", "isolation-second"])
def test_rows_inserted_in_one_test_are_not_visible_in_the_next(
    connection: Connection, run_id: str
) -> None:
    # GIVEN a fresh test whose predecessor (if any) inserted a row
    # THEN the table starts empty regardless of run order
    assert _count_call_log(connection) == 0

    # WHEN this test inserts a row
    connection.execute(_INSERT_CALL_LOG, {"called_at": _FIXED_CALLED_AT, "run_id": run_id})

    # THEN it is visible within this test only
    assert _count_call_log(connection) == 1


@pytest.mark.parametrize("run_id", ["session-commit-first", "session-commit-second"])
def test_session_commit_is_rolled_back_after_the_test(
    connection: Connection, session_factory: sessionmaker[Session], run_id: str
) -> None:
    # GIVEN a fresh test whose predecessor (if any) committed through a Session
    assert _count_call_log(connection) == 0

    # WHEN a Session inserts a row and commits
    with session_factory() as session:
        session.execute(_INSERT_CALL_LOG, {"called_at": _FIXED_CALLED_AT, "run_id": run_id})
        session.commit()

    # THEN the committed row is visible now, but the outer rollback removes it afterwards
    assert _count_call_log(connection) == 1
