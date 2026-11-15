from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Integer, MetaData, SmallInteger, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class ApiCallLog(Base):
    __tablename__ = "api_call_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    called_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    job: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[str] = mapped_column(String(250))
    operation: Mapped[str] = mapped_column(String(64))
    page_no: Mapped[int] = mapped_column(Integer)
    attempt: Mapped[int] = mapped_column(SmallInteger)
    params: Mapped[dict[str, Any]] = mapped_column(JSONB)
    http_status: Mapped[int | None] = mapped_column(SmallInteger)
    result_code: Mapped[str | None] = mapped_column(String(16))
    total_count: Mapped[int | None] = mapped_column(Integer)
    elapsed_ms: Mapped[int] = mapped_column(Integer)


class TransformWatermark(Base):
    """변환 태스크별로 처리한 마지막 원본 run. 태스크가 자기 쓰기를 커밋한 뒤 올린다.

    처리 대상은 (collected_at, run_key)가 이 값보다 뒤인 완료 run이다 (design.md 7장).
    """

    __tablename__ = "transform_watermark"

    task: Mapped[str] = mapped_column(String(64), primary_key=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    run_key: Mapped[str] = mapped_column(String(250, collation="C"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
