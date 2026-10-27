from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from eonje.config import DatabaseSettings


def make_engine(settings: DatabaseSettings) -> Engine:
    """DB URL만 쓴다. API 키 없이 로드되는 DatabaseSettings로 충분하다.

    Settings도 DatabaseSettings의 하위 클래스라 그대로 넘길 수 있다.
    """
    return create_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        connect_args={"connect_timeout": 10},
    )


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:

    session = factory()

    try:
        yield session
        session.commit()
    except BaseException:
        session.rollback()
        raise
    finally:
        session.close()
