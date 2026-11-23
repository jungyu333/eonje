"""마스터 원본(charger_info) → 차원 행. 순수 함수: DataFrame in → DataFrame out, I/O 없음.

API 필드 이름을 내부 이름으로 바꾸는 곳은 여기 하나다. 빈 문자열은 NULL로, 숫자는 타입으로 바꾸고,
속성 해시(row_hash)를 붙인다. SCD2 컬럼(valid_from 등)은 적재(load_dims)가 붙인다.
"""

from __future__ import annotations

from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from eonje.transform.text import blank_to_null

Field = tuple[str, str, str]  # (내부 이름, API 필드, Spark 타입)

_OPEN_END = "9999-12-31 00:00:00"

STATION_KEY = ("stat_id",)
STATION_FIELDS: tuple[Field, ...] = (
    ("stat_id", "statId", "string"),
    ("stat_nm", "statNm", "string"),
    ("addr", "addr", "string"),
    ("addr_detail", "addrDetail", "string"),
    ("location", "location", "string"),
    ("lat", "lat", "double"),
    ("lng", "lng", "double"),
    ("use_time", "useTime", "string"),
    ("busi_id", "busiId", "string"),
    ("bnm", "bnm", "string"),
    ("busi_nm", "busiNm", "string"),
    ("busi_call", "busiCall", "string"),
    ("zcode", "zcode", "string"),
    ("zscode", "zscode", "string"),
    ("kind", "kind", "string"),
    ("kind_detail", "kindDetail", "string"),
    ("parking_free", "parkingFree", "string"),
    ("note", "note", "string"),
    ("limit_yn", "limitYn", "string"),
    ("limit_detail", "limitDetail", "string"),
    ("traffic_yn", "trafficYn", "string"),
)

CHARGER_KEY = ("stat_id", "charger_id")
CHARGER_FIELDS: tuple[Field, ...] = (
    ("stat_id", "statId", "string"),
    ("charger_id", "chgerId", "string"),
    ("chger_type", "chgerType", "string"),
    ("output_kw", "output", "int"),
    ("method", "method", "string"),
    ("power_type", "powerType", "string"),
    ("maker", "maker", "string"),
    ("install_year", "year", "int"),
    ("floor_type", "floorType", "string"),
    ("floor_num", "floorNum", "string"),
    ("del_yn", "delYn", "string"),
    ("del_detail", "delDetail", "string"),
)

_SEP = "\u001f"  # 값 사이 구분자 (값에 나오지 않는 제어 문자)
_NULL = "\u0000"  # NULL과 빈 값을 해시에서 구분


def stations(raw: DataFrame) -> DataFrame:
    """충전소당 1행. 충전기 행마다 반복되는 충전소 속성 중 chgerId가 가장 작은 행의 것을 쓴다."""
    return _dimension(raw, STATION_FIELDS, STATION_KEY, tie_breaker="chgerId")


def chargers(raw: DataFrame) -> DataFrame:
    """충전기당 1행. 페이징 중 같은 충전기가 두 번 나오면 나중 페이지의 행을 쓴다."""
    return _dimension(raw, CHARGER_FIELDS, CHARGER_KEY, tie_breaker=None)


def _dimension(
    raw: DataFrame, fields: tuple[Field, ...], key: tuple[str, ...], tie_breaker: str | None
) -> DataFrame:
    attrs = [name for name, _, _ in fields if name not in key]
    # 정리(공백·빈 값)와 타입 변환을 먼저 한다. 숫자가 아닌 값은 실패하지 않고 NULL이 된다
    typed = raw.select(
        *[blank_to_null(api).try_cast(spark_type).alias(name) for name, api, spark_type in fields],
        (blank_to_null(tie_breaker) if tie_breaker else F.lit(None).cast("string")).alias("_tie"),
        F.col("page_no").alias("_page_no"),
    )
    for k in key:  # 키가 비어 있는 행은 차원 행이 될 수 없다
        typed = typed.where(F.col(k).isNotNull())

    # 해시는 저장되는 값으로 계산한다. 원문으로 계산하면 "050"과 "50"처럼 저장값이 같은데도
    # 해시가 달라져, 바뀐 게 없는 새 버전이 생긴다
    hashed = typed.withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws(_SEP, *[F.coalesce(F.col(a).cast("string"), F.lit(_NULL)) for a in attrs]),
            256,
        ),
    )
    # 키당 한 행을 고르는 기준은 결정적이어야 한다 (다시 돌려도 같은 행)
    order = Window.partitionBy(*key).orderBy(
        F.col("_tie").asc_nulls_last(), F.col("_page_no").desc(), F.col("row_hash")
    )
    picked = hashed.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1)
    return picked.select(*[name for name, _, _ in fields], "row_hash")


def scd2_changes(
    current: DataFrame,
    incoming: DataFrame,
    key: tuple[str, ...],
    *,
    collected_at: datetime,
    run_key: str,
) -> DataFrame:
    """현재 차원(current)과 새 스냅샷(incoming)을 비교해 차원 테이블에 MERGE할 행을 만든다.

    - 닫을 행: 현재 버전인데 새 스냅샷에 없거나 row_hash가 다른 행 (valid_to, is_current만 바꾼다)
    - 열 행: 새 스냅샷에 있는데 현재 버전이 없거나 row_hash가 다른 행 (새 버전)
    결과 컬럼은 current와 같다. 바뀐 게 없으면 0행이라 같은 run을 다시 넣어도 결과가 같다.
    incoming은 키당 1행이어야 한다 (stations·chargers의 결과).
    """

    at = F.lit(collected_at).cast("timestamp")
    cur = current.where(F.col("is_current")).alias("c")
    new = incoming.alias("n")
    same_key = reduce(lambda a, b: a & b, [F.col(f"c.{k}") == F.col(f"n.{k}") for k in key])
    differs = F.col("c.row_hash") != F.col("n.row_hash")

    closed = (
        cur.join(new, same_key, "left")
        .where(F.col(f"n.{key[0]}").isNull() | differs)
        .select(*[F.col(f"c.{name}") for name in current.columns])
        .withColumn("valid_to", at)
        .withColumn("is_current", F.lit(False))
    )

    opened = (
        new.join(cur, same_key, "left")
        .where(F.col(f"c.{key[0]}").isNull() | differs)
        .select(*[F.col(f"n.{name}") for name in incoming.columns])
        .withColumn("valid_from", at)
        .withColumn("valid_to", F.lit(_OPEN_END).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("source_run_key", F.lit(run_key))
    )

    return closed.unionByName(opened.select(*current.columns))
