"""DAG 무결성 검사. Airflow 이미지 안에서 dags/의 모든 파일을 실행해 DAG를 만들고 규칙을 확인한다.

DB와 스케줄러 없이 돈다 (DAG 정의만 만든다). CI와 `make dags-verify`가 쓴다.
    python check_dags.py /opt/airflow/dags
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG


def _check_private_environment_attribute() -> list[str]:
    # dags/의 EonjeTask는 실행 시점에 _private_environment를 덮어써 비밀값을 넘긴다.
    # 공개 API가 아니므로, provider가 이 이름을 쓰지 않게 되면 여기서 실패시킨다
    probe = DockerOperator(task_id="probe", image="probe", private_environment={"K": "v"})
    if getattr(probe, "_private_environment", None) != {"K": "v"}:
        return [
            "DockerOperator no longer keeps private_environment in _private_environment; "
            "update EonjeTask in dags/"
        ]
    return []


def _check_dag(dag: DAG) -> list[str]:
    errors = []
    if not dag.tags:
        errors.append(f"{dag.dag_id}: no tags")
    if dag.catchup:
        errors.append(f"{dag.dag_id}: catchup must be False (the API does not serve past windows)")
    for task in dag.tasks:
        if isinstance(task, DockerOperator) and task.do_xcom_push:
            errors.append(f"{dag.dag_id}.{task.task_id}: set do_xcom_push=False")
    return errors


def main(dags_dir: Path) -> int:
    errors = _check_private_environment_attribute()
    files = sorted(p for p in dags_dir.glob("*.py") if not p.name.startswith("_"))
    if not files:
        errors.append(f"no DAG files in {dags_dir}")
    for path in files:
        try:
            namespace = runpy.run_path(str(path))
        except Exception as e:  # noqa: BLE001 - report every broken file, not just the first
            errors.append(f"{path.name}: {type(e).__name__}: {e}")
            continue
        dags = [v for v in namespace.values() if isinstance(v, DAG)]
        if not dags:
            errors.append(f"{path.name}: defines no DAG")
        for dag in dags:
            errors.extend(_check_dag(dag))
            print(f"ok  {path.name}: {dag.dag_id} ({len(dag.tasks)} task(s))")

    for error in errors:
        print(f"ERROR  {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dags")))
