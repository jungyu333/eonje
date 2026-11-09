from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from eonje.storage.lake.spark import CATALOG

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

DEFAULT_PROPERTIES: Mapping[str, str] = {
    "format-version": "2",
    "write.merge.mode": "copy-on-write",
    "write.update.mode": "copy-on-write",
    "write.delete.mode": "copy-on-write",
}


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    comment: str | None = None


@dataclass(frozen=True)
class IcebergTable:
    namespace: str
    name: str
    columns: tuple[Column, ...]
    partitioned_by: tuple[str, ...] = ()
    properties: Mapping[str, str] = field(default_factory=lambda: dict(DEFAULT_PROPERTIES))

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        duplicated = sorted({n for n in names if names.count(n) > 1})
        if duplicated:
            raise ValueError(f"{self.name}: duplicated columns {duplicated}")
        unknown = [p for p in self.partitioned_by if p not in names]
        if unknown:
            raise ValueError(f"{self.name}: partition columns not in table {unknown}")

    @property
    def full_name(self) -> str:
        return f"{CATALOG}.{self.namespace}.{self.name}"


@dataclass(frozen=True)
class EnsureResult:
    created: tuple[str, ...]
    altered: Mapping[str, tuple[str, ...]]  # 테이블 → 추가한 컬럼
    unchanged: tuple[str, ...]


def _quote(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _column_sql(column: Column) -> str:
    sql = f"`{column.name}` {column.type}"
    if not column.nullable:
        sql += " NOT NULL"
    if column.comment:
        sql += f" COMMENT {_quote(column.comment)}"
    return sql


def create_table_sql(table: IcebergTable) -> str:
    columns = ",\n  ".join(_column_sql(c) for c in table.columns)
    sql = f"CREATE TABLE IF NOT EXISTS {table.full_name} (\n  {columns}\n)\nUSING iceberg"
    if table.partitioned_by:
        sql += "\nPARTITIONED BY (" + ", ".join(f"`{p}`" for p in table.partitioned_by) + ")"
    if table.properties:
        props = ", ".join(f"{_quote(k)}={_quote(v)}" for k, v in sorted(table.properties.items()))
        sql += f"\nTBLPROPERTIES ({props})"
    return sql


def missing_columns(table: IcebergTable, existing: Iterable[str]) -> tuple[Column, ...]:

    have = set(existing)
    missing = tuple(c for c in table.columns if c.name not in have)
    required = [c.name for c in missing if not c.nullable]
    if required:
        raise ValueError(
            f"{table.full_name}: cannot add NOT NULL columns to an existing table {required}"
        )
    return missing


def add_columns_sql(table: IcebergTable, columns: Iterable[Column]) -> str:
    body = ", ".join(_column_sql(c) for c in columns)
    return f"ALTER TABLE {table.full_name} ADD COLUMNS ({body})"


def ensure_tables(spark: SparkSession, tables: Iterable[IcebergTable]) -> EnsureResult:

    created: list[str] = []
    altered: dict[str, tuple[str, ...]] = {}
    unchanged: list[str] = []
    for table in tables:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{table.namespace}")
        if not spark.catalog.tableExists(table.full_name):
            spark.sql(create_table_sql(table))
            created.append(table.full_name)
            continue
        missing = missing_columns(table, spark.table(table.full_name).columns)
        if missing:
            spark.sql(add_columns_sql(table, missing))
            altered[table.full_name] = tuple(c.name for c in missing)
        else:
            unchanged.append(table.full_name)
    return EnsureResult(created=tuple(created), altered=altered, unchanged=tuple(unchanged))


_SCD2 = (
    Column(
        "valid_from",
        "TIMESTAMP",
        nullable=False,
        comment="이 버전이 처음 관측된 full run의 collected_at",
    ),
    Column("valid_to", "TIMESTAMP", nullable=False, comment="현재 버전은 9999-12-31"),
    Column("is_current", "BOOLEAN", nullable=False),
    Column("row_hash", "STRING", nullable=False, comment="속성 컬럼 해시. 바뀐 행 판단용"),
    Column("source_run_key", "STRING", nullable=False, comment="이 버전을 만든 원본 full run"),
)

# getChargerInfo 응답 중 충전소 단위 필드 (같은 충전소의 충전기 행마다 반복됨)
DIM_STATION = IcebergTable(
    namespace="core",
    name="dim_station",
    columns=(
        Column("stat_id", "STRING", nullable=False, comment="statId"),
        Column("stat_nm", "STRING", comment="statNm"),
        Column("addr", "STRING"),
        Column("addr_detail", "STRING", comment="addrDetail"),
        Column("location", "STRING", comment="상세 위치 설명"),
        Column("lat", "DOUBLE"),
        Column("lng", "DOUBLE"),
        Column("use_time", "STRING", comment="useTime, 자유 텍스트"),
        Column("busi_id", "STRING", comment="busiId, 운영기관 코드"),
        Column("bnm", "STRING", comment="기관명"),
        Column("busi_nm", "STRING", comment="운영기관명"),
        Column("busi_call", "STRING", comment="운영기관 연락처"),
        Column("zcode", "STRING", comment="시도 코드"),
        Column("zscode", "STRING", comment="시군구 코드"),
        Column("kind", "STRING", comment="시설 구분 코드"),
        Column("kind_detail", "STRING", comment="시설 구분 상세 코드"),
        Column("parking_free", "STRING", comment="parkingFree Y/N"),
        Column("note", "STRING"),
        Column("limit_yn", "STRING", comment="이용 제한 Y/N"),
        Column("limit_detail", "STRING"),
        Column("traffic_yn", "STRING", comment="trafficYn Y/N"),
        *_SCD2,
    ),
)

DIM_CHARGER = IcebergTable(
    namespace="core",
    name="dim_charger",
    columns=(
        Column("stat_id", "STRING", nullable=False, comment="statId"),
        Column("charger_id", "STRING", nullable=False, comment="chgerId"),
        Column("chger_type", "STRING", comment="충전기 타입 코드"),
        Column("output_kw", "INT", comment="output (kW)"),
        Column("method", "STRING", comment="단독/동시"),
        Column("power_type", "STRING", comment="powerType"),
        Column("maker", "STRING", comment="제조사"),
        Column("install_year", "INT", comment="year"),
        Column("floor_type", "STRING", comment="floorType F(지상)/B(지하)"),
        Column("floor_num", "STRING", comment="floorNum"),
        Column("del_yn", "STRING", comment="삭제 여부 Y/N"),
        Column("del_detail", "STRING"),
        *_SCD2,
    ),
)

DIM_DATE = IcebergTable(
    namespace="core",
    name="dim_date",
    columns=(
        Column("date", "DATE", nullable=False),
        Column("year", "INT", nullable=False),
        Column("month", "INT", nullable=False),
        Column("day", "INT", nullable=False),
        Column("day_of_week", "INT", nullable=False, comment="1=월 ... 7=일 (ISO)"),
        Column("is_weekend", "BOOLEAN", nullable=False),
        Column("is_holiday", "BOOLEAN", nullable=False),
        Column("holiday_name", "STRING"),
    ),
)

TABLES: tuple[IcebergTable, ...] = (DIM_STATION, DIM_CHARGER, DIM_DATE)
