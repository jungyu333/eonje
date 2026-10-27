import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from eonje.config import DatabaseSettings

REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.db


@pytest.fixture(scope="session")
def schema() -> str:
    return f"test_{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="session")
def engine(schema: str) -> Iterator[Engine]:
    """개발 DB 안에 세션용 임시 스키마를 만들고 마이그레이션을 head까지 적용한다."""
    url = DatabaseSettings().database_url.get_secret_value()
    admin = create_engine(url, connect_args={"connect_timeout": 5})
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))

    eng = create_engine(
        url, connect_args={"connect_timeout": 5, "options": f"-csearch_path={schema}"}
    )
    try:
        cfg = Config(str(REPO_ROOT / "alembic.ini"))
        cfg.set_main_option("script_location", str(REPO_ROOT / "infra" / "migrations"))
        with eng.begin() as conn:
            cfg.attributes["connection"] = conn
            command.upgrade(cfg, "head")
        yield eng
    finally:
        eng.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


@pytest.fixture
def connection(engine: Engine) -> Iterator[Connection]:
    """테스트마다 바깥 트랜잭션을 열고 끝나면 롤백한다. 테스트 간 데이터가 남지 않는다."""
    with engine.connect() as conn:
        outer = conn.begin()
        yield conn
        outer.rollback()


@pytest.fixture
def session_factory(connection: Connection) -> sessionmaker[Session]:
    """세션의 commit은 savepoint 해제가 되어, 바깥 롤백으로 모두 지워진다."""
    return sessionmaker(
        bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
    )
