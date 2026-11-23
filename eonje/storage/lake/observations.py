"""관측 적재의 읽기·쓰기 (원본 s3a, Iceberg). 변환은 transform/observations.py가 한다."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import TYPE_CHECKING

from eonje.storage.lake.iceberg import OBS_STATUS
from eonje.storage.raw.paths import Dataset, RawRun, run_prefix

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from eonje.storage.raw.run import RunManifest

_ROWS_VIEW = "eonje_obs_rows"
# 원본 데이터셋 → 관측의 source 값
_SOURCE = {Dataset.CHARGER_STATUS: "delta", Dataset.CHARGER_INFO: "full"}
# .../run=<run_key>/page=001.parquet 에서 run_key를 꺼낸다. run_key는 경로에 안전한 문자만 쓴다
_RUN_KEY_IN_PATH = "/run=([^/]+)/"


def read_raw_runs(spark: SparkSession, bucket: str, manifests: Sequence[RunManifest]) -> DataFrame:
    """같은 데이터셋의 완료된 run 여러 개를 한 번에 읽는다. 행마다 run_key와 source가 붙는다.

    행이 없는 run은 넘기지 않는다. 그런 run의 파일에는 응답 필드 컬럼이 없다.
    """
    datasets = {m.dataset for m in manifests}
    if len(datasets) != 1:
        raise ValueError(f"expected runs of exactly one dataset, got {sorted(datasets)}")
    dataset = Dataset(datasets.pop())
    folders = [
        f"s3a://{run_prefix(bucket, RawRun(dataset, date.fromisoformat(m.dt), m.run_key))}"
        for m in manifests
    ]
    return read_run_folders(spark, folders, source=_SOURCE[dataset])


def read_run_folders(spark: SparkSession, folders: Sequence[str], *, source: str) -> DataFrame:
    """run 폴더들의 모든 페이지. run_key는 파일 경로의 run=...에서 꺼내고, source는 받은 값이다.

    페이지마다 컬럼 구성이 다를 수 있어 mergeSchema로 읽는다. _manifest.json은 이름이 _로 시작해
    Spark가 데이터 파일로 읽지 않는다.
    """
    # pyspark 없이도 이 모듈이 import돼야 해서 여기서 가져온다 (레지스트리가 모든 잡을 import한다)
    from pyspark.sql import functions as F

    raw = spark.read.option("mergeSchema", "true").parquet(*folders)
    run_key = F.regexp_extract(F.col("_metadata.file_path"), _RUN_KEY_IN_PATH, 1)
    return raw.withColumn("run_key", run_key).withColumn("source", F.lit(source))


def merge_observations(spark: SparkSession, rows: DataFrame, key: tuple[str, ...]) -> int:
    """관측 행을 stg.obs_status에 넣는다. 키가 이미 있는 행은 무시한다. 한 번의 커밋이다.

    같은 행을 다시 넣어도 결과가 같다 (추가만 하는 테이블, design.md 6장).
    ON 절에 rows에 나온 observed_date를 함께 걸어 그 파티션만 읽게 한다. observed_date는 run의
    collected_at에서 나오므로, 키가 같은 기존 행은 반드시 같은 파티션에 있다.
    돌려주는 값은 넘긴 행 수다. 이미 있어서 무시된 행도 센다.
    """
    per_date = rows.groupBy("observed_date").count().collect()
    if not per_date:
        return 0

    rows.createOrReplaceTempView(_ROWS_VIEW)
    days = sorted(r["observed_date"] for r in per_date)
    dates = ", ".join(f"DATE'{day:%Y-%m-%d}'" for day in days)
    same_key = " AND ".join(f"t.{column} = s.{column}" for column in key)
    names = [column.name for column in OBS_STATUS.columns]
    spark.sql(
        f"""
        MERGE INTO {OBS_STATUS.full_name} t
        USING {_ROWS_VIEW} s
        ON t.observed_date IN ({dates}) AND {same_key}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(names)})
          VALUES ({", ".join(f"s.{name}" for name in names)})
        """
    )
    return sum(r["count"] for r in per_date)
