from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import DAG
from eonje_airflow.operators import SPARK_POOL, EonjeTask

IMAGE_REPO = os.environ.get("EONJE_IMAGE_REPO", "eonje")
SPARK_IMAGE = f"{IMAGE_REPO}-spark:{os.environ.get('EONJE_IMAGE_TAG', 'dev')}"
NETWORK = os.environ.get("EONJE_TASK_NETWORK", "eonje_default")

# 평소에는 run 1개(약 1만 4천 행)라 1분 안쪽이다. 밀린 날짜는 하루치(run 144개, 약 200만 행)를
# 한 묶음으로 처리하고 묶음마다 1분쯤 걸린다 (힙 1g 실측).
# 처음 켤 때 밀린 날짜를 따라잡을 여유를 둔다. 도중에 끊겨도 묶음마다 워터마크가 올라가 있어
# 다음 실행이 이어서 한다
TIMEOUT_MIN = 30

with DAG(
    dag_id="ev_transform_10min",
    # 수집(ev_status_10min)은 매 10분 정각에 돌고, 그 run은 15분 뒤에 확정된다
    # (지연 허용치 10분 + 수집 타임아웃 5분). 5분 어긋나게 돌아 확정된 run을 바로 집는다:
    # 10:00에 수집한 run을 10:15에 변환한다
    schedule="5-59/10 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,  # 처리할 run은 워터마크로 정한다. 지난 회차를 따로 돌릴 필요가 없다
    max_active_runs=1,
    tags=["transform"],
) as dag:
    # 수집 DAG 뒤에 붙이지 않는다. 변환은 워터마크로 밀린 run을 처리하므로
    # 수집이 건너뛰어진 회차에도 돌아야 한다
    EonjeTask(
        task_id="dedupe_load_obs",
        image=SPARK_IMAGE,
        command=[
            "dedupe-load-obs",
            "--run-id",
            "{{ run_id }}",
            "--run-after",
            "{{ dag_run.run_after.isoformat() }}",
        ],
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        execution_timeout=timedelta(minutes=TIMEOUT_MIN),
        # 다시 돌려도 결과가 같고 10분 뒤에 다음 회차가 온다. 일시적인 실패만 한 번 더 해 본다
        retries=1,
        retry_delay=timedelta(minutes=1),
        do_xcom_push=False,
        pool=SPARK_POOL,
        # 드라이버 컨테이너의 상한. JVM 힙(기본 1g) + 파이썬 프로세스
        mem_limit="2g",
    )
