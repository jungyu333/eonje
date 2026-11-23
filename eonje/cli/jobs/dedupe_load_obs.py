"""원본 run(상태 변경분·마스터)을 관측(stg.obs_status)으로 적재하는 잡.

흐름은 load/observations.py에 있고 여기는 연결만 한다. 데이터셋마다 워터마크와 확정 시간(settle)을
따로 쓴다. 마스터 run은 늦게 끝날 수 있어서, 워터마크를 같이 쓰면 상태 변경분이 먼저 지나가
마스터 run을 놓친다.

처리할 run은 워터마크와 지금 시각으로 정하므로 run_id·run_after는 쓰지 않는다.
pyspark가 필요한 load.observations는 run() 안에서 import한다. 레지스트리가 모든 잡 모듈을
import하는데 collector 이미지에는 pyspark가 없다.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from eonje.cli import exit_codes
from eonje.cli.context import JobContext
from eonje.storage.lake.observations import read_raw_runs
from eonje.storage.lake.spark import spark_session
from eonje.storage.postgres.db import make_engine, make_session_factory, session_scope
from eonje.storage.postgres.watermark import Watermark, advance_watermark, read_watermark
from eonje.storage.raw.fs import raw_filesystem
from eonje.storage.raw.paths import Dataset, partition_date
from eonje.storage.raw.run import list_manifests

TASK = "dedupe_load_obs"


def watermark_task(dataset: Dataset) -> str:
    """데이터셋별 워터마크 이름 (transform_watermark.task)."""
    return f"{TASK}:{dataset}"


def run(ctx: JobContext) -> int:
    from eonje.load.observations import load_observations

    settings = ctx.settings
    now = datetime.now(UTC)
    engine = make_engine(settings)
    factory = make_session_factory(engine)

    try:
        fs = raw_filesystem(settings)
        spark = spark_session(settings, app_name=TASK)

        def load(dataset: Dataset, settle: timedelta) -> None:
            task = watermark_task(dataset)
            with session_scope(factory) as session:
                watermark = read_watermark(session, task)

            def advance(to: Watermark) -> None:
                with session_scope(factory) as session:
                    advance_watermark(session, task, to, now=datetime.now(UTC))

            # 워터마크의 날짜부터만 훑는다. 그보다 앞 날짜의 run은 모두 처리했다
            since = None if watermark is None else partition_date(watermark.collected_at)
            load_observations(
                spark,
                list_manifests(fs, settings.raw_bucket, dataset, since=since),
                watermark=watermark,
                now=now,
                settle=settle,
                read_raw=lambda manifests: read_raw_runs(spark, settings.raw_bucket, manifests),
                advance=advance,
            )

        try:
            load(Dataset.CHARGER_STATUS, settings.status_settle)
            load(Dataset.CHARGER_INFO, settings.master_settle)
        finally:
            spark.stop()
    finally:
        engine.dispose()
    return exit_codes.OK
