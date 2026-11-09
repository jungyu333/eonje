from __future__ import annotations

import logging

from eonje.cli.context import JobContext
from eonje.storage.lake.iceberg import TABLES, ensure_tables
from eonje.storage.lake.spark import spark_session

log = logging.getLogger(__name__)


def run(ctx: JobContext) -> int:
    spark = spark_session(ctx.settings, app_name="ensure_iceberg_tables")

    try:
        result = ensure_tables(spark, TABLES)
    finally:
        spark.stop()

    for name in result.created:
        log.info("created: %s", name)

    for name, columns in result.altered.items():
        log.info("added columns: %s %s", name, list(columns))

    log.info(
        "iceberg tables: created=%d altered=%d unchanged=%d",
        len(result.created),
        len(result.altered),
        len(result.unchanged),
    )

    return 0
