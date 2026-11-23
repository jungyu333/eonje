from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import DAG
from eonje_airflow.operators import API_POOL, SKIPPED, SPARK_POOL, EonjeTask

# 개발 PC는 make build로 만든 eonje-collector:dev, 운영은 ghcr.io/jungyu333/eonje-collector:<sha>
IMAGE_REPO = os.environ.get("EONJE_IMAGE_REPO", "eonje")
IMAGE_TAG = os.environ.get("EONJE_IMAGE_TAG", "dev")
IMAGE = f"{IMAGE_REPO}-collector:{IMAGE_TAG}"
SPARK_IMAGE = f"{IMAGE_REPO}-spark:{IMAGE_TAG}"
NETWORK = os.environ.get("EONJE_TASK_NETWORK", "eonje_default")

# 전국 마스터 약 53페이지 × 페이지당 약 8초 ≈ 7분. 재시도 여유를 두고 20분.
# 변환 잡이 같은 값으로 마스터 run의 완료가 확정되는 시간을 계산한다
TIMEOUT_MIN = int(os.environ.get("EONJE_MASTER_TIMEOUT_MIN", "20"))

LOAD_TIMEOUT_MIN = 15
# 날짜 차원은 API 호출 몇 번과 천 행 안팎의 쓰기라 1분 안쪽이다
DATE_TIMEOUT_MIN = 10

with DAG(
    dag_id="ev_master_daily",
    # KST 03:11 = UTC 18:11. API 한도가 하루 단위로 리셋된 직후라 예산이 넉넉하고
    # 배포와 겹칠 일이 적다 (KST 03:05~03:35에는 배포하지 않는다, node-a.sh)
    schedule="11 18 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,  # API는 '지금 전체'만 준다. 지난 날짜는 받을 수 없다
    max_active_runs=1,
    tags=["collect", "master"],
) as dag:
    fetch_master = EonjeTask(
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
        pool=API_POOL,
    )

    load_dims = EonjeTask(
        task_id="load_dims",
        image=SPARK_IMAGE,
        command=[
            "load-dims",
            "--run-id",
            "{{ run_id }}",
            "--run-after",
            "{{ dag_run.run_after.isoformat() }}",
        ],
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        skip_on_exit_code=SKIPPED,
        execution_timeout=timedelta(minutes=LOAD_TIMEOUT_MIN),
        retries=2,
        retry_delay=timedelta(minutes=5),
        do_xcom_push=False,
        pool=SPARK_POOL,
        # 드라이버 컨테이너의 상한. JVM 힙(기본 1g) + 파이썬 프로세스
        mem_limit="2g",
        # 워터마크 태스크라 수집이 건너뛰어져도 돌아 밀린 run을 처리한다.
        # 수집이 실패했으면 돌지 않는다
        trigger_rule="none_failed",
    )

    fetch_master >> load_dims

    # 날짜 차원은 마스터와 무관해서 다른 태스크와 묶지 않는다.
    # 특일 정보 API가 실패해도 수집과 차원 적재는 그대로 돈다
    EonjeTask(
        task_id="load_dim_date",
        image=SPARK_IMAGE,
        command=[
            "load-dim-date",
            "--run-id",
            "{{ run_id }}",
            "--run-after",
            "{{ dag_run.run_after.isoformat() }}",
        ],
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        skip_on_exit_code=SKIPPED,
        execution_timeout=timedelta(minutes=DATE_TIMEOUT_MIN),
        retries=2,
        retry_delay=timedelta(minutes=5),
        do_xcom_push=False,
        pool=SPARK_POOL,
        mem_limit="2g",
    )
