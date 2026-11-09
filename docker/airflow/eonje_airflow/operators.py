"""eonje 태스크를 실행하는 DockerOperator."""

from __future__ import annotations

import os
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator

SKIPPED = 99  # eonje.cli.exit_codes.SKIPPED (예산 소진, 늦은 run)
API_POOL = "evcharger_api"

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
        # Airflow·provider를 올릴 때 이 이름을 다시 확인한다 (check_dags.py가 검사).
        # noinspection PyAttributeOutsideInit
        self._private_environment = {
            k: v
            for k, v in os.environ.items()
            if k.startswith("EONJE_") and k not in _NOT_FOR_TASKS
        }
        return super().execute(context)
