"""Spark 세션. Iceberg 카탈로그(`lake`, JDBC + S3FileIO)와 원본용 s3a를 설정값으로 채운다.

설정 계산(`spark_conf`)은 순수 함수로 두어 JVM 없이 테스트한다. pyspark는 spark extra에만 있으므로
`spark_session` 안에서만 import한다 (collector 이미지·개발 PC 기본 환경에는 없음).
필요한 키와 이유는 docs/design.md 8.2 (2026.11.03 PoC로 확인).
"""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

from sqlalchemy.engine import make_url

from eonje.storage.raw.fs import s3_credentials

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from eonje.config import Settings

CATALOG = "lake"
CATALOG_SCHEMA = "iceberg"  # eonje DB 안에서 Iceberg JDBC 카탈로그 테이블이 있는 스키마
# SeaweedFS는 리전을 보지 않지만 AWS SDK가 값을 요구한다
_REGION = "us-east-1"
_TIMEZONE = "Asia/Seoul"
_DEFAULT_PG_PORT = 5432


def jdbc_url(database_url: str) -> tuple[str, str, str]:
    """SQLAlchemy URL → (JDBC URL, 사용자, 비밀번호).

    드라이버 표기(+psycopg)는 버리고, 비밀번호는 URL에 넣지 않는다
    (Spark UI Environment 탭에 설정값이 보이므로 jdbc.password로 따로 넘긴다).
    """
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        raise ValueError(f"Iceberg catalog needs a PostgreSQL URL, got {url.get_backend_name()!r}")
    if not url.host or not url.database or not url.username or url.password is None:
        raise ValueError("database URL must include host, database, user and password")
    port = url.port or _DEFAULT_PG_PORT
    jdbc = f"jdbc:postgresql://{url.host}:{port}/{url.database}?currentSchema={CATALOG_SCHEMA}"
    return jdbc, url.username, str(url.password)


def spark_conf(settings: Settings, *, driver_host: str | None) -> dict[str, str]:
    """SparkSession에 넣을 설정. driver_host가 있으면 클러스터 모드(드라이버 주소 지정)."""
    access_key, secret_key = s3_credentials(settings)
    url, user, password = jdbc_url(settings.database_url.get_secret_value())
    endpoint = settings.s3_endpoint_url
    c = f"spark.sql.catalog.{CATALOG}"

    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.session.timeZone": _TIMEZONE,
        # Iceberg 카탈로그: 메타데이터 위치는 Postgres(JDBC), 데이터 파일은 S3FileIO
        c: "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"{c}.uri": url,
        f"{c}.jdbc.user": user,
        f"{c}.jdbc.password": password,
        f"{c}.jdbc.schema-version": "V1",  # Iceberg 1.12는 지정하지 않으면 거부
        f"{c}.warehouse": f"s3://{settings.warehouse_bucket}/",
        f"{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{c}.s3.endpoint": endpoint,
        f"{c}.s3.path-style-access": "true",
        # 긴 이름만 인식한다. 짧은 이름(s3.access-key)은 오류 없이 무시된다
        f"{c}.s3.access-key-id": access_key,
        f"{c}.s3.secret-access-key": secret_key,
        f"{c}.client.region": _REGION,
        # 원본 Parquet(s3a://)과, Iceberg의 s3:// 경로를 Hadoop이 다룰 때(고아 파일 목록 등)
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": _REGION,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https://")).lower(),
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }
    if driver_host is not None:
        # executor가 드라이버에 직접 접속한다. compose 네트워크에서는 드라이버 컨테이너 IP
        conf["spark.driver.host"] = driver_host
        conf["spark.driver.bindAddress"] = "0.0.0.0"
    return conf


def driver_host() -> str:
    """드라이버 컨테이너의 IP.

    컨테이너 hostname(ID)은 다른 컨테이너에서 이름으로 찾지 못할 수 있어 IP를 쓴다.
    """
    return socket.gethostbyname(socket.gethostname())


def spark_session(settings: Settings, app_name: str) -> SparkSession:
    from pyspark.sql import SparkSession

    master = settings.spark_master
    host = None if master.startswith("local") else driver_host()
    builder = SparkSession.builder.master(master).appName(app_name)
    for key, value in spark_conf(settings, driver_host=host).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
