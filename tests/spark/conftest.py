"""Spark 테스트 공용. eonje-spark 이미지 안에서 돈다 (make test-spark)."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

SPARK_TESTS_DIR = Path(__file__).resolve().parent


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Every test under tests/spark gets the spark marker, so the default run skips them.

    tryfirst: the marker must be on the item before pytest's own -m deselection runs.
    """
    for item in items:
        if item.path.is_relative_to(SPARK_TESTS_DIR):
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One local SparkSession for the whole run (starting a JVM per test is too slow)."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("eonje-tests")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
