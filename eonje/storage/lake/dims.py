"""차원 적재의 읽기·쓰기 (원본 s3a, Iceberg). 비교는 transform/dims.py가 하고 여기는 I/O만 한다."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from eonje.storage.raw.paths import Dataset, RawRun, run_prefix

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from eonje.storage.lake.iceberg import IcebergTable
    from eonje.storage.raw.run import RunManifest

_CHANGES_VIEW = "eonje_scd2_changes"


def read_raw_run(spark: SparkSession, bucket: str, manifest: RunManifest) -> DataFrame:
    """완료된 run의 모든 페이지. 페이지마다 컬럼 구성이 다를 수 있어 mergeSchema로 읽는다.

    _manifest.json은 이름이 _로 시작해 Spark가 데이터 파일로 읽지 않는다.
    """
    run = RawRun(Dataset(manifest.dataset), date.fromisoformat(manifest.dt), manifest.run_key)
    return spark.read.option("mergeSchema", "true").parquet(f"s3a://{run_prefix(bucket, run)}")


def current_rows(spark: SparkSession, table: IcebergTable) -> int:
    """지금 유효한 버전(is_current)의 수."""
    return spark.table(table.full_name).where("is_current").count()


def merge_changes(
    spark: SparkSession, table: IcebergTable, key: tuple[str, ...], changes: DataFrame
) -> None:
    """scd2_changes의 결과를 (키, valid_from) 기준으로 차원 테이블에 반영한다. 한 번의 커밋이다.

    닫는 행은 기존 버전과 valid_from이 같아 valid_to·is_current만 갱신되고, 새 버전은 추가된다.
    """
    # 변경분을 먼저 계산해 고정한다. changes는 대상 테이블을 읽어 만든 것이라, 그대로 뷰로
    # 등록하면 MERGE 소스가 대상 테이블을 다시 참조해 Spark 4.1에서 계획 수립이 실패한다.
    changes.localCheckpoint().createOrReplaceTempView(_CHANGES_VIEW)
    on = " AND ".join(f"t.{column} = s.{column}" for column in (*key, "valid_from"))

    spark.sql(
        f"""
        MERGE INTO {table.full_name} t
        USING {_CHANGES_VIEW} s
        ON {on}
        WHEN MATCHED THEN UPDATE SET t.valid_to = s.valid_to, t.is_current = s.is_current
        WHEN NOT MATCHED THEN INSERT *
        """
    )
