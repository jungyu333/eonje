from __future__ import annotations

from collections.abc import Callable

from eonje.cli.context import JobContext
from eonje.cli.jobs import (
    dedupe_load_obs,
    ensure_iceberg_tables,
    fetch_delta,
    fetch_master,
    load_dim_date,
    load_dims,
    spark_check,
)

Job = Callable[[JobContext], int]

JOBS: dict[str, Job] = {
    "fetch-delta": fetch_delta.run,
    "fetch-master": fetch_master.run,
    "spark-check": spark_check.run,
    "ensure-iceberg-tables": ensure_iceberg_tables.run,
    "load-dims": load_dims.run,
    "load-dim-date": load_dim_date.run,
    "dedupe-load-obs": dedupe_load_obs.run,
}
