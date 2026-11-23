"""dedupe-load-obs job: import without pyspark and per-dataset watermark names.

The load flow itself is tested in tests/spark/load/test_load_observations.py.
"""

import subprocess
import sys

import pytest

from eonje.cli.jobs.dedupe_load_obs import watermark_task
from eonje.storage.postgres.models import TransformWatermark
from eonje.storage.raw.paths import Dataset


def test_job_module_imports_in_an_interpreter_without_pyspark() -> None:
    # GIVEN a fresh interpreter where any pyspark import fails, as in the collector image
    script = (
        "import sys\n"
        "sys.modules['pyspark'] = None\n"  # makes every 'import pyspark...' raise ImportError
        "import eonje.cli.jobs.dedupe_load_obs\n"
    )
    # WHEN it imports the job module directly
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )
    # THEN the import succeeds
    assert result.returncode == 0, result.stderr


def test_status_watermark_task_is_the_job_name_and_the_status_dataset() -> None:
    # GIVEN the status-delta dataset
    dataset = Dataset.CHARGER_STATUS
    # WHEN its watermark task name is built
    task = watermark_task(dataset)
    # THEN it names the job and the dataset
    assert task == "dedupe_load_obs:charger_status"


def test_master_watermark_task_is_the_job_name_and_the_master_dataset() -> None:
    # GIVEN the master dataset
    dataset = Dataset.CHARGER_INFO
    # WHEN its watermark task name is built
    task = watermark_task(dataset)
    # THEN it names the job and the dataset
    assert task == "dedupe_load_obs:charger_info"


def test_every_dataset_gets_its_own_watermark_task() -> None:
    # GIVEN every raw dataset
    datasets = list(Dataset)
    # WHEN a watermark task name is built for each
    tasks = {watermark_task(dataset) for dataset in datasets}
    # THEN no two datasets share a watermark
    assert len(tasks) == len(datasets)


@pytest.mark.parametrize("dataset", list(Dataset), ids=str)
def test_watermark_task_fits_in_the_watermark_task_column(dataset: Dataset) -> None:
    # GIVEN the length of transform_watermark.task (String(64))
    max_length = TransformWatermark.__table__.c.task.type.length
    # WHEN the dataset's watermark task name is built
    task = watermark_task(dataset)
    # THEN it fits in the column
    assert len(task) <= max_length
