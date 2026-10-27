from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, insert, select
from sqlalchemy.orm import Session, sessionmaker

from eonje.client.types import CallRecord
from eonje.storage.db import session_scope
from eonje.storage.models import ApiCallLog
from eonje.timeutil import require_aware


class CallLogWriter:
    def __init__(self, factory: sessionmaker[Session], job: str, run_id: str) -> None:
        self._factory = factory
        self._job = job
        self._run_id = run_id

    def __call__(self, record: CallRecord) -> None:
        require_aware("called_at", record.called_at)
        with session_scope(self._factory) as session:
            session.execute(insert(ApiCallLog), [self._row(record)])

    def _row(self, r: CallRecord) -> dict[str, Any]:
        return {
            "called_at": r.called_at,
            "job": self._job,
            "run_id": self._run_id,
            "operation": r.operation,
            "page_no": r.page_no,
            "attempt": r.attempt,
            "params": dict(r.params),
            "http_status": r.http_status,
            "result_code": r.result_code,
            "total_count": r.total_count,
            "elapsed_ms": r.elapsed_ms,
        }


def count_calls(session: Session, start: datetime, end: datetime) -> int:

    require_aware("start", start)
    require_aware("end", end)
    stmt = select(func.count()).where(ApiCallLog.called_at >= start, ApiCallLog.called_at < end)

    return session.execute(stmt).scalar_one()
