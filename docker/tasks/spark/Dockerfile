# eonje-spark: Spark 드라이버·master·worker 공용 이미지 (명령만 다름)
#  - 드라이버: DockerOperator가 `eonje <job>`으로 실행 (ENTRYPOINT)
#  - master·worker: compose에서 entrypoint를 spark-class로 바꿔 실행
#
# 버전 조합 (2026.11 기준, 근거와 PoC 결과는 docs/design.md 8.2)
#  - Spark 4.1.3 (pyproject의 spark extra), Iceberg 1.12.0, hadoop-aws 3.4.2 (Spark의 Hadoop과 같은 버전)
#  - AWS SDK v2는 iceberg-aws-bundle의 것 한 벌(2.54.17). hadoop-aws가 더 요구하는
#    s3-transfer-manager, apache-client만 같은 버전으로 넣는다
#  - Java 21 (Debian trixie 기본), Python 3.11 (eonje와 같음)
FROM python:3.11-slim-trixie

ARG ICEBERG_VERSION=1.12.0
ARG HADOOP_VERSION=3.4.2
ARG POSTGRES_JDBC_VERSION=42.7.13
ARG AWS_SDK_VERSION=2.54.17
ARG MAVEN=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.9.11 /uv /bin/uv

# SPARK_WORKER_DIR: 기본값($SPARK_HOME/work)은 venv 안이라 app 사용자가 쓸 수 없다
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    SPARK_WORKER_DIR=/tmp/spark-work

WORKDIR /app

# 1. 의존성 (pyspark 포함). 코드보다 먼저 두어 코드만 바뀌면 이 층과 jar 층은 캐시를 쓴다
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --extra spark --no-install-project

# 2. 추가 jar를 pyspark의 jars 폴더에 넣어 driver·executor 클래스패스에 기본으로 올린다.
#    실행 중 내려받지 않는다 (CLAUDE.md Spark 규칙). Maven의 sha1로 무결성 확인
RUN set -eu; \
    jars="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    fetch() { \
      url="$MAVEN/$1"; out="$jars/$(basename "$1")"; \
      curl -fsSL -o "$out" "$url"; \
      echo "$(curl -fsSL "$url.sha1" | cut -c1-40)  $out" | sha1sum -c -; \
    }; \
    fetch "org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar"; \
    fetch "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    fetch "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    fetch "software/amazon/awssdk/s3-transfer-manager/${AWS_SDK_VERSION}/s3-transfer-manager-${AWS_SDK_VERSION}.jar"; \
    fetch "software/amazon/awssdk/apache-client/${AWS_SDK_VERSION}/apache-client-${AWS_SDK_VERSION}.jar"; \
    fetch "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# 3. eonje 코드
COPY pyproject.toml uv.lock ./
COPY eonje/ eonje/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra spark

RUN useradd --create-home --uid 10001 app
USER app

ENTRYPOINT ["python", "-m", "eonje.cli"]
