from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

# 개발 PC는 make build로 만든 eonje-collector:dev, 운영은 ghcr.io/jungyu333/eonje-collector:<sha>
IMAGE_REPO = os.environ.get("EONJE_IMAGE_REPO", "eonje")
IMAGE = f"{IMAGE_REPO}-collector:{os.environ.get('EONJE_IMAGE_TAG', 'dev')}"
NETWORK = os.environ.get("EONJE_TASK_NETWORK", "eonje_default")
INTERVAL_MIN = int(os.environ.get("EONJE_STATUS_INTERVAL_MIN", "10"))
TIMEOUT_MIN = int(os.environ.get("EONJE_COLLECT_TIMEOUT_MIN", "5"))
SKIPPED = 99  # eonje.cli.exit_codes.SKIPPED (예산 소진, 늦은 run)

# */N cron은 N이 60의 약수일 때만 매시 같은 간격이 된다
# (7이면 :56 다음 :00으로 어긋나고, 60 이상이면 잘못된 cron)
if INTERVAL_MIN < 1 or 60 % INTERVAL_MIN != 0:
    raise ValueError(f"EONJE_STATUS_INTERVAL_MIN must divide 60, got {INTERVAL_MIN}")

# compose·Airflow 쪽에서만 쓰는 값. 태스크 컨테이너에는 넘기지 않는다
_NOT_FOR_TASKS = {
    "EONJE_DB_PASSWORD",
    "EONJE_IMAGE_REPO",
    "EONJE_IMAGE_TAG",
    "EONJE_COLLECT_TIMEOUT_MIN",
    "EONJE_TASK_NETWORK",
}


class EonjeTask(DockerOperator):
    """EONJE_* 설정을 실행 시점에 scheduler 환경에서 읽어 태스크 컨테이너로 넘긴다.

    파싱 시점에 넘기면 서비스 키 같은 값이 직렬화된 DAG(Airflow DB)에 남는다.
    """

    def execute(self, context: Any) -> Any:
        # DockerOperator는 private_environment를 비공개 속성 _private_environment에 저장하고
        # create_container에서 읽는다 (docker provider 4.4.5 기준, 공개 API 아님).
        # Airflow·provider를 올릴 때 이 이름을 다시 확인한다.
        # 바뀌면 서비스 키 없이 떠서 설정 검증에서 실패한다
        # noinspection PyAttributeOutsideInit
        self._private_environment = {
            k: v
            for k, v in os.environ.items()
            if k.startswith("EONJE_") and k not in _NOT_FOR_TASKS
        }

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
