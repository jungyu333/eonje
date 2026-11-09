from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import DAG
from eonje_airflow.operators import SKIPPED, EonjeTask

# 개발 PC는 make build로 만든 eonje-collector:dev, 운영은 ghcr.io/jungyu333/eonje-collector:<sha>
IMAGE_REPO = os.environ.get("EONJE_IMAGE_REPO", "eonje")
IMAGE = f"{IMAGE_REPO}-collector:{os.environ.get('EONJE_IMAGE_TAG', 'dev')}"
NETWORK = os.environ.get("EONJE_TASK_NETWORK", "eonje_default")
INTERVAL_MIN = int(os.environ.get("EONJE_STATUS_INTERVAL_MIN", "10"))
TIMEOUT_MIN = int(os.environ.get("EONJE_COLLECT_TIMEOUT_MIN", "5"))

# */N cron은 N이 60의 약수일 때만 매시 같은 간격이 된다
# (7이면 :56 다음 :00으로 어긋나고, 60 이상이면 잘못된 cron)
if INTERVAL_MIN < 1 or 60 % INTERVAL_MIN != 0:
    raise ValueError(f"EONJE_STATUS_INTERVAL_MIN must divide 60, got {INTERVAL_MIN}")

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
        # pool evcharger_api에 넣지 않는다. API가 호출 시각 기준 최근 N분만 주므로
        # 늦게 부른 만큼 앞쪽 변경분을 잃는다. 마스터와 동시에 돌아도 delta 1회는
        # 최대 3페이지 × 3회라 예산 상한 여유가 흡수한다
    )
