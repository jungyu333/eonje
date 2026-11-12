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
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """One local SparkSession for the whole run (starting a JVM per test is too slow).

    `lake` is a throwaway Iceberg catalog in a temp folder (same name as production, so the
    SQL under test is unchanged). Production uses the JDBC catalog on S3 instead.
    """
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("eonje-tests")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", str(warehouse))
        .getOrCreate()
    )
    yield session
    session.stop()
