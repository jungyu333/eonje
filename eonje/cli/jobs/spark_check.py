"""Spark 실행 경로 확인: 드라이버 → master → executor, Iceberg JDBC 카탈로그.

데이터를 쓰지 않으므로 몇 번을 돌려도 결과가 같다. 실패하면 Spark 쪽 인프라
(네트워크, jar, 카탈로그 설정) 문제다. run_id·run_after는 쓰지 않는다.
원본 s3a 읽기는 원본을 읽는 첫 변환 잡이 검증한다.
"""

from __future__ import annotations

import logging

from eonje.cli.context import JobContext
from eonje.storage.lake.spark import CATALOG, spark_session

log = logging.getLogger(__name__)

_CHECK_ROWS = 100_000
_CHECK_PARTITIONS = 4  # 파티션을 나눠야 실제로 executor에 태스크가 간다


def run(ctx: JobContext) -> int:
    settings = ctx.settings
    spark = spark_session(settings, app_name="spark-check")
    try:
        # 1) executor가 붙어서 계산하는지 (worker가 없으면 자원을 기다리며 멈춤 → 바깥 timeout)
        rows = spark.range(_CHECK_ROWS).repartition(_CHECK_PARTITIONS).count()
        log.info("executors ok: master=%s rows=%d", settings.spark_master, rows)

        # 2) JDBC 카탈로그(iceberg 스키마) + S3FileIO 설정이 읽히는지
        namespaces = [row[0] for row in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
        log.info("catalog ok: %s namespaces=%s", CATALOG, namespaces)
    finally:
        spark.stop()
    return 0
