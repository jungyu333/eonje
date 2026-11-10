from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from eonje.storage.postgres.models import TransformWatermark
from eonje.timeutil import require_aware


@dataclass(frozen=True, order=True)
class Watermark:
    """처리한 마지막 run. 필드 순서가 곧 비교 순서"""

    collected_at: datetime
    run_key: str


def read_watermark(session: Session, task: str) -> Watermark | None:
    row = session.execute(
        select(TransformWatermark.collected_at, TransformWatermark.run_key).where(
            TransformWatermark.task == task
        )
    ).one_or_none()

    return None if row is None else Watermark(row.collected_at, row.run_key)


def advance_watermark(session: Session, task: str, to: Watermark, *, now: datetime) -> None:
    """워터마크를 to로 올린다. 이미 그보다 뒤에 있으면 그대로 둔다 (뒤로 가지 않는다)."""
    require_aware("collected_at", to.collected_at)
    require_aware("now", now)
    stmt = insert(TransformWatermark).values(
        task=task, collected_at=to.collected_at, run_key=to.run_key, updated_at=now
    )

    session.execute(
        stmt.on_conflict_do_update(
            index_elements=[TransformWatermark.task],
            set_={
                "collected_at": stmt.excluded.collected_at,
                "run_key": stmt.excluded.run_key,
                "updated_at": stmt.excluded.updated_at,
            },
            where=tuple_(TransformWatermark.collected_at, TransformWatermark.run_key)
            < tuple_(stmt.excluded.collected_at, stmt.excluded.run_key),
        )
    )
