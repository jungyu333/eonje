"""원본 Parquet 쓰기. 응답 필드는 전부 문자열로 받은 그대로, 메타데이터만 타입을 붙인다."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from fsspec import AbstractFileSystem

from eonje.client.types import Page
from eonje.storage.raw_paths import RawRun, page_path

META_FIELDS = (
    pa.field("collected_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("page_fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("page_no", pa.int32(), nullable=False),
)

_META_NAMES = frozenset(f.name for f in META_FIELDS)


def page_table(page: Page, collected_at: datetime) -> pa.Table:
    """Page → Arrow 테이블. 순수 함수. 시각은 시간대가 있어야 하고 UTC로 저장된다."""
    _require_aware("collected_at", collected_at)
    _require_aware("page.fetched_at", page.fetched_at)
    columns = _field_names(page.items)
    clash = _META_NAMES.intersection(columns)

    if clash:
        raise ValueError(f"response field collides with metadata column: {sorted(clash)}")

    schema = pa.schema([pa.field(c, pa.string()) for c in columns] + list(META_FIELDS))
    n = len(page.items)
    data: dict[str, list[Any]] = {c: [_as_str(item.get(c)) for item in page.items] for c in columns}
    data["collected_at"] = [collected_at] * n
    data["page_fetched_at"] = [page.fetched_at] * n
    data["page_no"] = [page.page_no] * n

    return pa.Table.from_pydict(data, schema=schema)


def write_page(
    fs: AbstractFileSystem, bucket: str, run: RawRun, page: Page, collected_at: datetime
) -> str:
    """버퍼에 다 쓴 뒤 한 번에 올린다. 변환·직렬화가 실패하면 객체가 생기지 않는다."""
    path = page_path(bucket, run, page.page_no)
    buf = pa.BufferOutputStream()

    pq.write_table(page_table(page, collected_at), buf, compression="zstd")

    fs.pipe_file(path, buf.getvalue().to_pybytes())
    return path


def _require_aware(name: str, value: datetime) -> None:
    """시간대 없는 시각은 거부한다. KST를 naive로 넘기면 9시간 어긋난 채 조용히 저장되므로."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware: {value!r}")


def _field_names(items: list[dict[str, Any]]) -> list[str]:
    seen: dict[str, None] = {}
    for item in items:
        for key in item:
            seen.setdefault(key, None)
    return list(seen)


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    # 문자열이 아니면 JSON에 적힌 표기 그대로: 50 → "50", true → "true", {...} → JSON
    return json.dumps(value, ensure_ascii=False)
