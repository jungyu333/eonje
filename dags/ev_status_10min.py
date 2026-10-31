from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

IMAGE = f"eonje-collector:{os.environ.get('EONJE_IMAGE_TAG', 'dev')}"
NETWORK = os.environ.get("EONJE_TASK_NETWORK", "eonje_default")
INTERVAL_MIN = int(os.environ.get("EONJE_STATUS_INTERVAL_MIN", "10"))
TIMEOUT_MIN = int(os.environ.get("EONJE_COLLECT_TIMEOUT_MIN", "5"))
SKIPPED = 99  # eonje.cli.exit_codes.SKIPPED (예산 소진, 늦은 run)


class EonjeTask(DockerOperator):
    def execute(self, context: Any) -> Any:
        # noinspection PyAttributeOutsideInit
        self._private_environment = {k: v for k, v in os.environ.items() if k.startswith("EONJE_")}

        return super().execute(context)


with DAG(
    dag_id="ev_status_10min",
    schedule=f"*/{INTERVAL_MIN} * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,  # 지난 구간은 API가 주지 않는다
    max_active_runs=1,
    tags=["collect"],
) as dag:
    EonjeTask(
        task_id="fetch_delta",
        image=IMAGE,
        command=[
            "fetch-delta",
            "--run-id",
            "{{ run_id }}",
            "--run-after",
            "{{ dag_run.run_after.isoformat() }}",
        ],
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        skip_on_exit_code=SKIPPED,
        execution_timeout=timedelta(minutes=TIMEOUT_MIN),
        retries=2,
        retry_delay=timedelta(minutes=1),
        do_xcom_push=False,
    )
