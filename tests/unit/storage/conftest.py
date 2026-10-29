from datetime import date
from typing import Any

import pytest
from sqlalchemy import Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Dialect
from sqlalchemy.schema import CreateIndex, CreateTable

from eonje.storage.models import Base
from eonje.storage.raw_paths import Dataset, RawRun


@pytest.fixture
def run() -> RawRun:
    return RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key="20261026T101000")


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def fake(**kwargs: Any) -> object:
        seen.update(kwargs)
        return object()

    monkeypatch.setattr("eonje.storage.fs.s3fs.S3FileSystem", fake)
    return seen


@pytest.fixture
def pg_dialect() -> Dialect:
    return postgresql.dialect()  # type: ignore[no-untyped-call]


@pytest.fixture
def api_call_log() -> Table:
    return Base.metadata.tables["api_call_log"]


@pytest.fixture
def api_call_log_ddl(api_call_log: Table, pg_dialect: Dialect) -> str:
    return str(CreateTable(api_call_log).compile(dialect=pg_dialect))


@pytest.fixture
def api_call_log_index_ddl(api_call_log: Table, pg_dialect: Dialect) -> list[str]:
    return sorted(str(CreateIndex(ix).compile(dialect=pg_dialect)) for ix in api_call_log.indexes)
