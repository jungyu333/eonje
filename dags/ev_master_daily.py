from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import DAG
from eonje_airflow.operators import SKIPPED, EonjeTask

# 개발 PC는 make build로 만든 eonje-collector:dev, 운영은 ghcr.io/jungyu333/eonje-collector:<sha>
IMAGE_REPO = os.environ.get("EONJE_IMAGE_REPO", "eonje")
IMAGE = f"{IMAGE_REPO}-collector:{os.environ.get('EONJE_IMAGE_TAG', 'dev')}"
NETWORK = os.environ.get("EONJE_TASK_NETWORK", "eonje_default")

# 전국 마스터 약 53페이지 × 페이지당 약 8초 ≈ 7분. 재시도 여유를 두고 20분
TIMEOUT_MIN = 20

with DAG(
    dag_id="ev_master_daily",
    # KST 03:15 = UTC 18:15. API 한도가 하루 단위로 리셋된 직후라 예산이 넉넉하고
    # 배포와 겹칠 일이 적다
    schedule="15 18 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,  # API는 '지금 전체'만 준다. 지난 날짜는 받을 수 없다
    max_active_runs=1,
    tags=["collect", "master"],
) as dag:
    EonjeTask(
        task_id="fetch_master",
        image=IMAGE,
        command=[
            "fetch-master",
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
        retry_delay=timedelta(minutes=5),
        do_xcom_push=False,
    )
