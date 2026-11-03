from __future__ import annotations

from collections.abc import Callable

from eonje.cli.context import JobContext
from eonje.cli.jobs import fetch_delta, fetch_master, spark_check

Job = Callable[[JobContext], int]

JOBS: dict[str, Job] = {
    "fetch-delta": fetch_delta.run,
    "fetch-master": fetch_master.run,
    "spark-check": spark_check.run,
}
