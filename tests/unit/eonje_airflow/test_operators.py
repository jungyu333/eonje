"""EonjeTask hands EONJE_* settings from the scheduler env to the task container.

Airflow is not installed in the dev env (only in the eonje-airflow image), so the docker
provider's DockerOperator is replaced by a stand-in that records the private environment the
container would receive. That the real provider still reads ``_private_environment`` is checked
inside the Airflow image by docker/airflow/check_dags.py.
"""

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any

import pytest

# The autouse fixture in conftest.py chdirs into tmp_path, so resolve paths from the repo root.
REPO_ROOT = Path(__file__).resolve().parents[3]
OPERATORS_PY = REPO_ROOT / "docker" / "airflow" / "eonje_airflow" / "operators.py"
DOCKER_OPERATOR_MODULE = "airflow.providers.docker.operators.docker"


class FakeDockerOperator:
    """Stands in for DockerOperator: keeps private_environment where the provider does."""

    def __init__(self, **kwargs: Any) -> None:
        self.task_id = kwargs["task_id"]
        self._private_environment: dict[str, str] = kwargs.get("private_environment") or {}
        self.container_environment: dict[str, str] | None = None

    def execute(self, context: Any) -> Any:
        # The provider reads _private_environment when it creates the container in execute
        self.container_environment = dict(self._private_environment)
        return None


@pytest.fixture
def eonje_task_cls(monkeypatch: pytest.MonkeyPatch) -> type[FakeDockerOperator]:
    """EonjeTask loaded from docker/airflow with the Airflow docker provider stubbed out."""
    parts = DOCKER_OPERATOR_MODULE.split(".")
    for i in range(1, len(parts) + 1):
        name = ".".join(parts[:i])
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    provider = sys.modules[DOCKER_OPERATOR_MODULE]
    provider.DockerOperator = FakeDockerOperator  # type: ignore[attr-defined]

    module_name = "eonje_airflow_operators_under_test"
    spec = importlib.util.spec_from_file_location(module_name, OPERATORS_PY)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.EonjeTask  # type: ignore[no-any-return]


def _container_environment(eonje_task_cls: type[FakeDockerOperator]) -> dict[str, str]:
    task = eonje_task_cls(task_id="fetch_delta", image="eonje-collector:test")
    task.execute(context={})
    assert task.container_environment is not None
    return task.container_environment


@pytest.mark.parametrize(
    ("name", "value"),
    [("EONJE_COLLECT_TIMEOUT_MIN", "7"), ("EONJE_MASTER_TIMEOUT_MIN", "30")],
)
def test_collection_timeout_is_passed_to_the_task_container(
    eonje_task_cls: type[FakeDockerOperator],
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    # GIVEN the scheduler env sets a collection timeout that differs from its default
    monkeypatch.setenv(name, value)

    # WHEN the task executes
    env = _container_environment(eonje_task_cls)

    # THEN the container gets the same value the DAG uses as execution_timeout
    assert env[name] == value


@pytest.mark.parametrize(
    "name",
    ["EONJE_DB_PASSWORD", "EONJE_IMAGE_REPO", "EONJE_IMAGE_TAG", "EONJE_TASK_NETWORK"],
)
def test_compose_only_setting_is_not_passed_to_the_task_container(
    eonje_task_cls: type[FakeDockerOperator],
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    # GIVEN the scheduler env sets a value used only by compose and Airflow
    monkeypatch.setenv(name, "scheduler-only")

    # WHEN the task executes
    env = _container_environment(eonje_task_cls)

    # THEN the value stays out of the container
    assert name not in env
