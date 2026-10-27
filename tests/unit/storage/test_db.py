from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from eonje.config import DatabaseSettings, Settings
from eonje.storage.db import make_engine, make_session_factory, session_scope

DB_PASSWORD = "db-pw-should-not-leak"
DB_URL = f"postgresql+psycopg://eonje:{DB_PASSWORD}@postgres.test:5432/eonje"


def _settings(database_url: str = DB_URL) -> Settings:
    return Settings(
        api_service_key="test-service-key",  # type: ignore[arg-type]
        database_url=database_url,  # type: ignore[arg-type]
    )


@pytest.fixture
def captured_engine_kwargs(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def fake(url: str, **kwargs: Any) -> object:
        seen["url"] = url
        seen.update(kwargs)
        return object()

    monkeypatch.setattr("eonje.storage.db.create_engine", fake)
    return seen


@pytest.fixture
def session() -> MagicMock:
    return MagicMock(spec=Session)


@pytest.fixture
def factory(session: MagicMock) -> MagicMock:
    return MagicMock(spec=sessionmaker, return_value=session)


# make_engine


def test_engine_receives_the_unwrapped_database_url(captured_engine_kwargs: dict[str, Any]) -> None:
    # GIVEN settings with a secret database URL
    settings = _settings()

    # WHEN the engine is built
    make_engine(settings)

    # THEN create_engine gets the plain string, not the SecretStr
    assert captured_engine_kwargs["url"] == DB_URL
    assert isinstance(captured_engine_kwargs["url"], str)


def test_engine_can_be_built_from_database_settings_without_an_api_key(
    captured_engine_kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN only a database URL in the environment (no API key)
    monkeypatch.setenv("EONJE_DATABASE_URL", DB_URL)

    # WHEN the engine is built from DatabaseSettings
    make_engine(DatabaseSettings())

    # THEN it gets the URL; DB-only tools need no API key
    assert captured_engine_kwargs["url"] == DB_URL


def test_engine_uses_a_small_pre_pinged_pool(captured_engine_kwargs: dict[str, Any]) -> None:
    # GIVEN default settings
    # WHEN the engine is built
    make_engine(_settings())

    # THEN the pool is small and checks connections before use
    assert captured_engine_kwargs["pool_pre_ping"] is True
    assert captured_engine_kwargs["pool_size"] == 2
    assert captured_engine_kwargs["max_overflow"] == 2


def test_engine_is_built_without_connecting() -> None:
    # GIVEN a URL pointing at a host that does not exist in the test environment
    settings = _settings()

    # WHEN the engine is built
    engine = make_engine(settings)

    # THEN an engine for the psycopg dialect is returned with no connection attempted
    try:
        assert isinstance(engine, Engine)
        assert engine.dialect.name == "postgresql"
        assert engine.dialect.driver == "psycopg"
        assert engine.url.render_as_string(hide_password=False) == DB_URL
    finally:
        engine.dispose()


def test_engine_repr_does_not_leak_the_database_password() -> None:
    # GIVEN an engine built from a URL with a password
    engine = make_engine(_settings())

    # WHEN the engine is rendered (e.g. in a log line)
    try:
        rendered = repr(engine) + str(engine.url)
    finally:
        engine.dispose()

    # THEN the password is masked
    assert DB_PASSWORD not in rendered


# make_session_factory


def test_session_factory_binds_sessions_to_the_given_engine() -> None:
    # GIVEN an engine (never connected)
    engine = make_engine(_settings())

    # WHEN a session is created from the factory
    try:
        factory = make_session_factory(engine)
        with factory() as session:
            bound = session.get_bind()
    finally:
        engine.dispose()

    # THEN it is bound to that engine
    assert bound is engine


def test_session_factory_does_not_expire_objects_on_commit() -> None:
    # GIVEN an engine (never connected)
    engine = make_engine(_settings())

    # WHEN the factory is built
    try:
        factory = make_session_factory(engine)
    finally:
        engine.dispose()

    # THEN committed objects stay readable after the session scope ends
    assert factory.kw["expire_on_commit"] is False


# session_scope


def test_scope_yields_the_session_from_the_factory(factory: MagicMock, session: MagicMock) -> None:
    # GIVEN a factory producing a known session
    # WHEN the scope is entered
    with session_scope(factory) as got:
        yielded = got

    # THEN the factory's session is yielded, created exactly once
    assert yielded is session
    factory.assert_called_once_with()


def test_scope_commits_and_closes_on_success(factory: MagicMock, session: MagicMock) -> None:
    # GIVEN a factory producing a session
    # WHEN the body completes without error
    with session_scope(factory):
        pass

    # THEN the session is committed, not rolled back, and closed
    session.commit.assert_called_once_with()
    session.rollback.assert_not_called()
    session.close.assert_called_once_with()


def test_scope_rolls_back_closes_and_reraises_on_error(
    factory: MagicMock, session: MagicMock
) -> None:
    # GIVEN a body that raises
    class BoomError(Exception):
        pass

    # WHEN the scope exits through the exception
    with pytest.raises(BoomError), session_scope(factory):
        raise BoomError

    # THEN nothing is committed, the session is rolled back and closed, the error propagates
    session.commit.assert_not_called()
    session.rollback.assert_called_once_with()
    session.close.assert_called_once_with()


def test_scope_rolls_back_on_keyboard_interrupt(factory: MagicMock, session: MagicMock) -> None:
    # GIVEN a body interrupted by a BaseException that is not an Exception
    # WHEN the scope exits through it
    with pytest.raises(KeyboardInterrupt), session_scope(factory):
        raise KeyboardInterrupt

    # THEN the transaction is still rolled back and the session closed
    session.commit.assert_not_called()
    session.rollback.assert_called_once_with()
    session.close.assert_called_once_with()


def test_failed_commit_is_rolled_back_and_reraised(factory: MagicMock, session: MagicMock) -> None:
    # GIVEN a session whose commit fails (e.g. constraint violation at flush)
    session.commit.side_effect = RuntimeError("commit failed")

    # WHEN the body completes and the scope tries to commit
    with pytest.raises(RuntimeError, match="commit failed"), session_scope(factory):
        pass

    # THEN the write failure is raised, after rollback and close
    session.rollback.assert_called_once_with()
    session.close.assert_called_once_with()


def test_session_is_closed_even_when_rollback_fails(factory: MagicMock, session: MagicMock) -> None:
    # GIVEN a session whose rollback also fails (e.g. connection dropped)
    session.rollback.side_effect = RuntimeError("rollback failed")

    # WHEN the body raises
    with pytest.raises(RuntimeError, match="rollback failed"), session_scope(factory):
        raise ValueError("body failed")

    # THEN the session is still closed
    session.close.assert_called_once_with()


def test_each_scope_opens_and_closes_its_own_session() -> None:
    # GIVEN a factory producing a fresh session per call
    sessions = [MagicMock(spec=Session), MagicMock(spec=Session)]
    factory = MagicMock(spec=sessionmaker, side_effect=sessions)

    # WHEN the same scope is used twice in a row
    with session_scope(factory) as first:
        pass
    with session_scope(factory) as second:
        pass

    # THEN each run gets a distinct session, each committed and closed once
    assert first is sessions[0]
    assert second is sessions[1]
    for s in sessions:
        s.commit.assert_called_once_with()
        s.close.assert_called_once_with()
