"""마스터 full run을 차원 테이블에 반영하는 잡. 흐름은 load/dims.py에 있고 여기는 연결만 한다.

pyspark가 필요한 load.dims는 run() 안에서 import한다. 레지스트리가 모든 잡 모듈을 import하는데
collector 이미지에는 pyspark가 없다.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eonje.cli import exit_codes
from eonje.cli.context import JobContext
from eonje.storage.lake.dims import read_raw_run
from eonje.storage.lake.spark import spark_session
from eonje.storage.postgres.db import make_engine, make_session_factory, session_scope
from eonje.storage.postgres.watermark import Watermark, advance_watermark, read_watermark
from eonje.storage.raw.fs import raw_filesystem
from eonje.storage.raw.paths import Dataset
from eonje.storage.raw.run import list_manifests

if TYPE_CHECKING:
    from eonje.load.dims import RunResult

TASK = "load_dims"


def run(ctx: JobContext) -> int:
    from eonje.load.dims import load_dims  # pyspark가 있어야 import된다

    settings = ctx.settings
    engine = make_engine(settings)
    factory = make_session_factory(engine)

    def advance(to: Watermark) -> None:
        with session_scope(factory) as session:
            advance_watermark(session, TASK, to, now=datetime.now(UTC))

    try:
        with session_scope(factory) as session:
            watermark = read_watermark(session, TASK)
        fs = raw_filesystem(settings)
        manifests = list_manifests(fs, settings.raw_bucket, Dataset.CHARGER_INFO)

        spark = spark_session(settings, app_name="load_dims")

        try:
            results = load_dims(
                spark,
                manifests,
                watermark=watermark,
                read_raw=lambda manifest: read_raw_run(spark, settings.raw_bucket, manifest),
                advance=advance,
                min_row_ratio=settings.master_min_row_ratio,
            )
        finally:
            spark.stop()
    finally:
        engine.dispose()

    return exit_code(results)


def exit_code(results: list[RunResult]) -> int:
    """마지막으로 본 run을 건너뛰었으면 skipped. 볼 run이 없거나 마지막 run을 반영했으면 성공."""
    if results and results[-1].skipped is not None:
        return exit_codes.SKIPPED

    return exit_codes.OK
