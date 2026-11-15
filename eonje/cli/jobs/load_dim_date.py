"""dim_date를 채우는 잡. 흐름은 load/dates.py에 있고 여기는 연결만 한다.

오늘 날짜는 실행 시각(now)이 아니라 run_after의 KST 날짜를 쓴다.
"""

from __future__ import annotations

from datetime import date

import httpx

from eonje.cli import exit_codes
from eonje.cli.context import JobContext
from eonje.client.holidays import fetch_holidays
from eonje.load.dates import load_dim_date
from eonje.storage.lake.dates import overwrite_dim_date, read_dim_date
from eonje.storage.lake.spark import spark_session
from eonje.timeutil import KST


def run(ctx: JobContext) -> int:
    settings = ctx.settings
    spark = spark_session(settings, app_name="load_dim_date")
    try:
        with httpx.Client() as http:

            def fetch(year: int) -> list[tuple[date, str]]:
                return [(h.day, h.name) for h in fetch_holidays(http, settings, year)]

            load_dim_date(
                first_year=settings.dim_date_first_year,
                today=ctx.run_after.astimezone(KST).date(),
                fetch=fetch,
                read=lambda: read_dim_date(spark),
                overwrite=lambda rows: overwrite_dim_date(spark, rows),
            )
    finally:
        spark.stop()
    return exit_codes.OK
