# eonje-spark: Spark 드라이버·master·worker 공용 이미지 (명령만 다름)
# 3단계 PoC 단계: Spark + Iceberg + SeaweedFS(S3) + JDBC 카탈로그 호환성 확인용.
# eonje 패키지는 아직 넣지 않는다 (Spark 실행 기반 PR에서 추가).
#
# 버전 조합 (2026.11 기준)
#  - Spark 4.1.3: Iceberg 1.12가 정식 runtime을 배포한 최신 계열. Iceberg 빌드가 4.1.3으로 검증됨
#  - Iceberg 1.12.0: iceberg-spark-runtime-4.1_2.13 + iceberg-aws-bundle(S3FileIO, AWS SDK v2 포함)
#  - hadoop-aws 3.4.2: Spark 4.1.3에 들어 있는 Hadoop과 같은 버전. 원본 Parquet을 s3a://로 읽는 데 필요
#    AWS SDK는 따로 넣지 않고 iceberg-aws-bundle의 것을 함께 쓴다 (두 벌이 섞이지 않게)
#  - Java 21(Debian trixie 기본), Python 3.11(eonje와 같음)
FROM python:3.11-slim-trixie

ARG SPARK_VERSION=4.1.3
ARG ICEBERG_VERSION=1.12.0
ARG HADOOP_VERSION=3.4.2
ARG POSTGRES_JDBC_VERSION=42.7.13
ARG MAVEN=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# pip pyspark에는 Spark jar와 spark-submit·spark-class(master·worker 실행)가 함께 들어 있다
# pyiceberg·duckdb: Spark 밖에서 같은 테이블을 복사 없이 읽는지 확인용 (PoC)
RUN pip install --no-cache-dir \
      "pyspark==${SPARK_VERSION}" \
      "pyiceberg[pyarrow,sql-postgres,s3fs]==0.12.0" \
      "duckdb==2.0.0"

# 추가 jar를 pyspark의 jars 폴더에 넣어 driver·executor 클래스패스에 기본으로 올린다.
# 실행 중 내려받지 않는다 (CLAUDE.md Spark 규칙). Maven의 sha1로 무결성 확인
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
    fetch "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    ls -1 "$jars" | grep -E 'iceberg|hadoop-(aws|client)|postgresql|awssdk|bundle'

RUN useradd --create-home --uid 10001 app
USER app
WORKDIR /home/app
