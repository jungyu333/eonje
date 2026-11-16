"""원본(charger_status·charger_info) → 관측 행. 순수 함수: DataFrame in → DataFrame out, I/O 없음.

충전기 1대가 run 1회에서 관측된 것이 1행이다 (stg.obs_status). 입력에 여러 run이 섞여 있어도 된다.
API 시각(yyyyMMddHHmmss)은 시간대 표시가 없는 KST라, Spark 세션 시간대(Asia/Seoul)로 해석한다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from eonje.transform.text import blank_to_null

OBS_KEY = ("stat_id", "charger_id", "run_key")
OBS_COLUMNS = (
    "stat_id",
    "charger_id",
    "run_key",
    "observed_at",
    "observed_date",
    "source",
    "status_code",
    "stat_upd_dt",
    "last_ts",
    "last_te",
    "now_ts",
)

_API_TIME = "yyyyMMddHHmmss"
_TIME_FIELDS = (
    ("stat_upd_dt", "statUpdDt"),
    ("last_ts", "lastTsdt"),
    ("last_te", "lastTedt"),
    ("now_ts", "nowTsdt"),
)


def observations(raw: DataFrame) -> DataFrame:
    """원본 행을 관측 행으로 바꾸고, 같은 run에 두 번 나온 충전기는 한 행만 남긴다.

    raw에는 응답 필드(문자열)와 collected_at, page_no가 있고, 읽는 쪽이 run_key와
    source(delta | full)를 붙여서 넘긴다. 결과 컬럼은 OBS_COLUMNS다.

    - 빈 값은 NULL, 시각으로 읽을 수 없는 값도 NULL (실패하지 않는다)
    - 키(statId, chgerId)가 빈 행은 버린다
    - 같은 run에 같은 충전기가 두 번 나오면(페이징 드리프트) statUpdDt가 늦은 행을 남긴다.
      같으면 나중 페이지의 행, 그래도 같으면 나머지 값의 순서로 정한다 (다시 돌려도 같은 행)
    """
    typed = raw.select(
        blank_to_null("statId").alias("stat_id"),
        blank_to_null("chgerId").alias("charger_id"),
        F.col("run_key"),
        F.col("collected_at").alias("observed_at"),
        F.to_date(F.col("collected_at")).alias("observed_date"),
        F.col("source"),
        blank_to_null("stat").alias("status_code"),
        *[
            F.try_to_timestamp(blank_to_null(api), F.lit(_API_TIME)).alias(name)
            for name, api in _TIME_FIELDS
        ],
        F.col("page_no").alias("_page_no"),
    ).where(F.col("stat_id").isNotNull() & F.col("charger_id").isNotNull())

    order = Window.partitionBy(*OBS_KEY).orderBy(
        F.col("stat_upd_dt").desc_nulls_last(),
        F.col("_page_no").desc(),
        F.col("status_code").asc_nulls_last(),
        F.col("last_ts").asc_nulls_last(),
        F.col("last_te").asc_nulls_last(),
        F.col("now_ts").asc_nulls_last(),
    )
    picked = typed.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1)
    return picked.select(*OBS_COLUMNS)
