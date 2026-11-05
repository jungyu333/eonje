import pytest

from eonje.config import Settings
from eonje.storage.lake.spark import CATALOG, jdbc_url, spark_conf

DB_PASSWORD = "db-pw-should-not-leak"
DB_URL = f"postgresql+psycopg://eonje:{DB_PASSWORD}@postgres:5432/eonje"
ACCESS_KEY = "test-access"
SECRET_KEY = "test-secret"
C = f"spark.sql.catalog.{CATALOG}"


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "api_service_key": "test-service-key",
        "database_url": DB_URL,
        "s3_endpoint_url": "http://seaweedfs:8333",
        "s3_access_key": ACCESS_KEY,
        "s3_secret_key": SECRET_KEY,
        "warehouse_bucket": "eonje-warehouse",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_jdbc_url_drops_driver_and_password_and_targets_catalog_schema() -> None:
    # GIVEN a SQLAlchemy URL with the psycopg driver and a password
    # WHEN it is converted
    url, user, password = jdbc_url(DB_URL)

    # THEN the JDBC URL points at the iceberg schema and carries no password
    assert url == "jdbc:postgresql://postgres:5432/eonje?currentSchema=iceberg"
    assert DB_PASSWORD not in url
    assert (user, password) == ("eonje", DB_PASSWORD)


def test_jdbc_url_defaults_port_and_decodes_password() -> None:
    # GIVEN a URL without a port and with a percent-encoded password
    url, _, password = jdbc_url("postgresql+psycopg://eonje:p%40ss@db.test/eonje")

    # THEN the default port is filled in and the password is decoded
    assert url.startswith("jdbc:postgresql://db.test:5432/eonje?")
    assert password == "p@ss"


@pytest.mark.parametrize(
    "database_url",
    [
        "sqlite:///eonje.db",
        "postgresql+psycopg://eonje:pw@postgres:5432/",
        "postgresql+psycopg://eonje@postgres:5432/eonje",
    ],
)
def test_jdbc_url_rejects_incomplete_or_non_postgres_urls(database_url: str) -> None:
    with pytest.raises(ValueError):
        jdbc_url(database_url)


def test_spark_conf_sets_catalog_keys_the_poc_proved_necessary() -> None:
    # GIVEN complete settings
    # WHEN the config is built for local mode
    conf = spark_conf(_settings(), driver_host=None)

    # THEN the JDBC catalog, S3FileIO and long key names are set
    assert conf[f"{C}.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    assert conf[f"{C}.jdbc.schema-version"] == "V1"
    assert conf[f"{C}.warehouse"] == "s3://eonje-warehouse/"
    assert conf[f"{C}.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf[f"{C}.s3.access-key-id"] == ACCESS_KEY
    assert conf[f"{C}.s3.secret-access-key"] == SECRET_KEY
    assert conf[f"{C}.client.region"]
    # short names are silently ignored by Iceberg; they must not be used by mistake
    assert f"{C}.s3.access-key" not in conf
    assert f"{C}.s3.secret-key" not in conf
    # the password goes in its own key, never in the URL
    assert conf[f"{C}.jdbc.password"] == DB_PASSWORD
    assert DB_PASSWORD not in conf[f"{C}.uri"]


def test_spark_conf_sets_s3a_for_raw_parquet_and_iceberg_paths() -> None:
    conf = spark_conf(_settings(), driver_host=None)

    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://seaweedfs:8333"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.access.key"] == ACCESS_KEY
    assert conf["spark.hadoop.fs.s3.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"


def test_spark_conf_enables_ssl_for_https_endpoint() -> None:
    conf = spark_conf(_settings(s3_endpoint_url="https://s3.test"), driver_host=None)

    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"


def test_spark_conf_sets_driver_address_only_for_cluster_mode() -> None:
    # WHEN built without and with a driver host
    local = spark_conf(_settings(), driver_host=None)
    cluster = spark_conf(_settings(), driver_host="172.18.0.9")

    # THEN only cluster mode pins the driver address
    assert not any(key.startswith("spark.driver.") for key in local)
    assert cluster["spark.driver.host"] == "172.18.0.9"
    assert cluster["spark.driver.bindAddress"] == "0.0.0.0"


@pytest.mark.parametrize(
    ("missing", "env_name"),
    [("s3_access_key", "EONJE_S3_ACCESS_KEY"), ("s3_secret_key", "EONJE_S3_SECRET_KEY")],
)
def test_spark_conf_fails_without_s3_credentials(missing: str, env_name: str) -> None:
    # GIVEN settings with one S3 key unset
    settings = _settings(**{missing: None})

    # THEN it fails naming the variable instead of letting Spark try anonymous access
    with pytest.raises(ValueError, match=env_name):
        spark_conf(settings, driver_host=None)
