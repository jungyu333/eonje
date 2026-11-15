"""dim_date의 읽기·쓰기 (Iceberg). 작은 정적 테이블이라 MERGE가 아니라 전체를 덮어쓴다."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import astuple, fields
from typing import TYPE_CHECKING

from eonje.storage.lake.iceberg import DIM_DATE
from eonje.transform.dates import DateRow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_COLUMNS = tuple(f.name for f in fields(DateRow))
_ROWS_VIEW = "eonje_dim_date_rows"


def read_dim_date(spark: SparkSession) -> list[DateRow]:
    """테이블의 모든 행 (날짜순). 작은 정적 테이블이라 드라이버로 다 가져온다."""
    rows = spark.table(DIM_DATE.full_name).select(*_COLUMNS).orderBy("date").collect()
    return [DateRow(*row) for row in rows]


def overwrite_dim_date(spark: SparkSession, rows: Sequence[DateRow]) -> None:
    """테이블 전체를 rows로 바꾼다. 한 번의 커밋이라 실패하면 이전 내용이 그대로 남는다."""
    if not rows:
        raise ValueError("refusing to overwrite dim_date with no rows")

    frame = spark.createDataFrame([astuple(row) for row in rows], schema=_schema_ddl())
    frame.createOrReplaceTempView(_ROWS_VIEW)
    columns = ", ".join(f"`{name}`" for name in _COLUMNS)
    spark.sql(
        f"INSERT OVERWRITE {DIM_DATE.full_name} ({columns}) SELECT {columns} FROM {_ROWS_VIEW}"
    )


def _schema_ddl() -> str:
    """테이블 정의와 같은 타입·NOT NULL로 DataFrame 스키마를 만든다 (컬럼 순서는 DateRow)."""
    by_name = {c.name: c for c in DIM_DATE.columns}
    return ", ".join(
        f"`{name}` {by_name[name].type}" + ("" if by_name[name].nullable else " NOT NULL")
        for name in _COLUMNS
    )
