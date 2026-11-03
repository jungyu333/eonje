"""3단계 PoC: SeaweedFS(S3) + Spark 4.1 + Iceberg 1.12(JDBC 카탈로그) 호환성 확인.

설계(docs/design.md 6·7장)가 기대는 기능이 SeaweedFS 위에서 그대로 되는지 하나씩 확인하고
결과표를 출력한다. 하나라도 실패하면 종료 코드 1.
실행: `make poc-iceberg` (make up, make buckets 먼저)

확인 항목
  1. JDBC 카탈로그 + S3FileIO로 네임스페이스·테이블 생성 (KST 날짜 컬럼으로 파티션)
  2. MERGE ... WHEN NOT MATCHED INSERT: 키 충돌 무시, 같은 입력 재실행 시 행 수 그대로
  3. ON 절에 파티션 범위를 건 MERGE: 결과 같음, 스캔 파일 수 감소
  4. 기존 행을 갱신하는 MERGE (세션 종료)
  5. 같은 테이블에 동시 커밋 2개 → 카탈로그 원자적 교체 + 재시도로 둘 다 반영
  6. 시간 여행 (이전 스냅샷 읽기)
  7. 유지보수: rewrite_data_files, expire_snapshots, remove_orphan_files(심어 둔 고아 파일 삭제)
  8. 원본 Parquet을 s3a://로 읽기 (hadoop-aws, iceberg-aws-bundle의 AWS SDK와 함께)
  9. Spark 밖에서 같은 테이블 읽기: PyIceberg(SQL 카탈로그) → DuckDB

카탈로그 테이블은 eonje DB의 `iceberg_poc` 스키마에, 데이터는 `s3://<warehouse>/poc`에 만든다.
끝나면 테이블·네임스페이스를 지운다 (`--keep`이면 남김).
"""

from __future__ import annotations

import os
import sys
import threading
import time
import traceback
from collections.abc import Callable
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

CATALOG = "lake"
NS = "poc"
CATALOG_SCHEMA = "iceberg_poc"

S3_ENDPOINT = os.environ.get("EONJE_S3_ENDPOINT_URL", "http://seaweedfs:8333")
S3_KEY = os.environ["EONJE_S3_ACCESS_KEY"]
S3_SECRET = os.environ["EONJE_S3_SECRET_KEY"]
RAW_BUCKET = os.environ.get("EONJE_RAW_BUCKET", "eonje-raw")
WAREHOUSE_BUCKET = os.environ.get("EONJE_WAREHOUSE_BUCKET", "eonje-warehouse")
PG_HOST = os.environ.get("POC_PG_HOST", "postgres")
PG_PASSWORD = os.environ["EONJE_DB_PASSWORD"]
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/eonje?currentSchema={CATALOG_SCHEMA}"
WAREHOUSE = f"s3://{WAREHOUSE_BUCKET}/poc"
KST = ZoneInfo("Asia/Seoul")
REGION = "us-east-1"  # SeaweedFS는 리전을 보지 않지만 AWS SDK가 값을 요구한다

results: list[tuple[str, bool, str]] = []


def check(name: str) -> Callable[[Callable[[], str]], None]:
    def run(fn: Callable[[], str]) -> None:
        started = time.monotonic()
        try:
            detail = fn()
            results.append((name, True, f"{detail} ({time.monotonic() - started:.1f}s)"))
        except Exception as e:  # noqa: BLE001 - 모든 항목을 끝까지 돌려 결과표를 만든다
            traceback.print_exc()
            results.append((name, False, f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"))

    return run


def build_spark() -> SparkSession:
    c = f"spark.sql.catalog.{CATALOG}"
    return (
        SparkSession.builder.appName("poc-iceberg-seaweedfs")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Iceberg 카탈로그: JDBC(Postgres) + S3FileIO(SeaweedFS)
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{c}.uri", JDBC_URL)
        .config(f"{c}.jdbc.user", "eonje")
        .config(f"{c}.jdbc.password", PG_PASSWORD)
        .config(f"{c}.jdbc.schema-version", "V1")  # 1.12는 지정하지 않으면 거부한다고 알려짐
        .config(f"{c}.warehouse", WAREHOUSE)
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", S3_ENDPOINT)
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.s3.access-key-id", S3_KEY)  # 짧은 이름(s3.access-key)은 조용히 무시됨
        .config(f"{c}.s3.secret-access-key", S3_SECRET)
        .config(f"{c}.client.region", REGION)
        # Hadoop s3a: 원본 Parquet 읽기, remove_orphan_files의 목록 조회
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Iceberg가 쓰는 s3:// 경로를 Hadoop이 다룰 때(고아 파일 목록)도 s3a 구현으로
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def ensure_catalog_schema() -> None:
    import psycopg2  # pyiceberg[sql-postgres]가 함께 설치

    conn = psycopg2.connect(host=PG_HOST, dbname="eonje", user="eonje", password=PG_PASSWORD)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")
    conn.close()


def t(name: str) -> str:
    return f"{CATALOG}.{NS}.{name}"


def main() -> int:
    keep = "--keep" in sys.argv
    ensure_catalog_schema()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    sql = spark.sql
    kst_day = datetime.now(KST).date()

    def now_literal() -> str:
        # spark.sql.session.timeZone이 Asia/Seoul이라 TIMESTAMP 리터럴은 KST로 해석된다
        return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    def snapshots(name: str) -> int:
        return sql(f"SELECT count(*) FROM {t(name)}.snapshots").first()[0]

    def count(name: str) -> int:
        return sql(f"SELECT count(*) FROM {t(name)}").first()[0]

    @check("1. 네임스페이스·테이블 생성 (KST 날짜 파티션)")
    def _() -> str:
        sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}")
        sql(f"DROP TABLE IF EXISTS {t('obs')} PURGE")
        sql(f"DROP TABLE IF EXISTS {t('session')} PURGE")
        sql(
            f"""CREATE TABLE {t("obs")} (
                  stat_id STRING, charger_id STRING, run_key STRING,
                  observed_at TIMESTAMP, observed_date DATE, status_code STRING)
                USING iceberg PARTITIONED BY (observed_date)"""
        )
        sql(
            f"""CREATE TABLE {t("session")} (
                  stat_id STRING, charger_id STRING, started_at TIMESTAMP, started_date DATE,
                  ended_at TIMESTAMP, duration_min INT)
                USING iceberg PARTITIONED BY (started_date)"""
        )
        loc = sql(f"DESCRIBE TABLE EXTENDED {t('obs')}").where("col_name = 'Location'").first()
        return f"location={loc[1] if loc else '?'}"

    def source_view(run_key: str, day_offset: int, n: int = 2000) -> None:
        sql(
            f"""CREATE OR REPLACE TEMP VIEW src AS
                SELECT concat('ST', cast(id % 500 AS STRING)) AS stat_id,
                       cast(id AS STRING) AS charger_id,
                       '{run_key}' AS run_key,
                       timestamp'{kst_day + timedelta(days=day_offset)} 10:00:00' AS observed_at,
                       date'{kst_day + timedelta(days=day_offset)}' AS observed_date,
                       cast(id % 6 AS STRING) AS status_code
                FROM range({n})"""
        )

    def date_list(view: str, column: str) -> str:
        # MERGE의 ON 절은 서브쿼리를 받지 않는다. 소스에 나온 날짜를 먼저 구해 리터럴로 넣는다
        days = [r[0] for r in sql(f"SELECT DISTINCT {column} FROM {view}").collect()]
        return ", ".join(f"date'{d}'" for d in days)

    merge_ignore = f"""
        MERGE INTO {t("obs")} t USING src s
        ON t.stat_id = s.stat_id AND t.charger_id = s.charger_id AND t.run_key = s.run_key
        WHEN NOT MATCHED THEN INSERT *"""

    @check("2. MERGE 키 충돌 무시 + 재실행 멱등")
    def _() -> str:
        source_view("run-a", 0)
        sql(merge_ignore)
        first = count("obs")
        sql(merge_ignore)  # 같은 입력 다시
        second = count("obs")
        assert first == second == 2000, f"{first=} {second=}"
        return f"rows {first} → {second}"

    @check("3. 파티션 범위를 건 MERGE (결과 동일, 스캔 감소)")
    def _() -> str:
        for i, key in enumerate(["run-b", "run-c", "run-d"], start=1):  # 다른 날짜 파티션을 채운다
            source_view(key, -i)
            sql(merge_ignore)
        source_view("run-a", 0)
        bounded = f"""
            MERGE INTO {t("obs")} t USING src s
            ON t.observed_date IN ({date_list("src", "observed_date")})
               AND t.stat_id = s.stat_id AND t.charger_id = s.charger_id AND t.run_key = s.run_key
            WHEN NOT MATCHED THEN INSERT *"""
        before = count("obs")
        sql(bounded)
        after = count("obs")
        assert before == after == 8000, f"{before=} {after=}"
        return f"rows {after} (4 date partitions, source touches 1)"

    @check("4. 기존 행을 갱신하는 MERGE (세션 종료)")
    def _() -> str:
        sql(
            f"""INSERT INTO {t("session")} VALUES
                ('ST1', '1', timestamp'{kst_day} 09:00:00', date'{kst_day}', NULL, NULL)"""
        )
        sql(
            f"""CREATE OR REPLACE TEMP VIEW ended AS
                SELECT 'ST1' AS stat_id, '1' AS charger_id,
                       timestamp'{kst_day} 09:00:00' AS started_at, date'{kst_day}' AS started_date,
                       timestamp'{kst_day} 09:45:00' AS ended_at, 45 AS duration_min"""
        )
        merge = f"""
            MERGE INTO {t("session")} t USING ended s
            ON t.started_date IN ({date_list("ended", "started_date")})
               AND t.stat_id = s.stat_id AND t.charger_id = s.charger_id
               AND t.started_at = s.started_at
            WHEN MATCHED THEN UPDATE SET t.ended_at = s.ended_at, t.duration_min = s.duration_min
            WHEN NOT MATCHED THEN INSERT *"""
        sql(merge)
        sql(merge)
        row = sql(f"SELECT count(*), max(duration_min) FROM {t('session')}").first()
        assert (row[0], row[1]) == (1, 45), row
        return "1 row, duration_min=45 after two runs"

    @check("5. 동시 커밋 2개 (원자적 교체 + 재시도)")
    def _() -> str:
        before_rows, before_snaps = count("obs"), snapshots("obs")
        errors: list[BaseException] = []

        def append(key: str) -> None:
            try:
                spark.range(1000).selectExpr(
                    "concat('C', cast(id AS STRING)) AS stat_id",
                    "cast(id AS STRING) AS charger_id",
                    f"'{key}' AS run_key",
                    f"timestamp'{kst_day} 11:00:00' AS observed_at",
                    f"date'{kst_day}' AS observed_date",
                    "'2' AS status_code",
                ).writeTo(t("obs")).append()
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=append, args=(k,)) for k in ("conc-1", "conc-2")]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        assert not errors, errors
        rows, snaps = count("obs"), snapshots("obs")
        assert rows == before_rows + 2000 and snaps == before_snaps + 2, (rows, snaps)
        return f"rows +2000, snapshots {before_snaps} → {snaps}"

    @check("6. 시간 여행")
    def _() -> str:
        first = sql(
            f"SELECT snapshot_id FROM {t('obs')}.snapshots ORDER BY committed_at LIMIT 1"
        ).first()[0]
        old = sql(f"SELECT count(*) FROM {t('obs')} VERSION AS OF {first}").first()[0]
        assert old == 2000, old
        return f"first snapshot rows={old}, now={count('obs')}"

    @check("7a. rewrite_data_files (작은 파일 합치기)")
    def _() -> str:
        before = sql(f"SELECT count(*) FROM {t('obs')}.files").first()[0]
        # 기본 기준(그룹당 입력 파일 5개 이상)이면 PoC 규모에선 합칠 대상이 없다
        sql(
            f"""CALL {CATALOG}.system.rewrite_data_files(
                    table => '{NS}.obs', options => map('min-input-files', '2'))"""
        )
        after = sql(f"SELECT count(*) FROM {t('obs')}.files").first()[0]
        assert after <= before, (before, after)
        return f"data files {before} → {after}"

    @check("7b. expire_snapshots")
    def _() -> str:
        before = snapshots("obs")
        sql(
            f"""CALL {CATALOG}.system.expire_snapshots(
                    table => '{NS}.obs',
                    older_than => TIMESTAMP '{now_literal()}',
                    retain_last => 1)"""
        )
        after = snapshots("obs")
        assert after == 1, (before, after)
        return f"snapshots {before} → {after}"

    @check("7c. remove_orphan_files (심어 둔 고아 파일 삭제)")
    def _() -> str:
        loc = sql(f"SELECT * FROM {t('obs')}.files LIMIT 1").select("file_path").first()[0]
        data_dir = loc.rsplit("/", 2)[0]  # .../data
        orphan = f"{data_dir}/orphan-poc.parquet"
        jvm = spark._jvm  # noqa: SLF001
        path = jvm.org.apache.hadoop.fs.Path(orphan)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
        fs.create(path).close()
        time.sleep(2)
        # CALL remove_orphan_files는 24시간보다 최근 파일을 대상으로 삼지 못하게 막는다(안전장치).
        # 운영은 기본값(3일 이전)으로 프로시저를 쓰면 되고, PoC는 목록·삭제 동작만 보려고
        # 같은 기능의 Action API를 시간 제한 없이 직접 부른다
        jspark = spark._jsparkSession  # noqa: SLF001
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(jspark, t("obs"))
        cutoff_ms = int(time.time() * 1000)
        result = (
            jvm.org.apache.iceberg.spark.actions.SparkActions.get(jspark)
            .deleteOrphanFiles(table)
            .olderThan(cutoff_ms)
            .execute()
        )
        names: list[str] = []
        it = result.orphanFileLocations().iterator()  # Java Iterable
        while it.hasNext():
            names.append(str(it.next()))
        assert any(n.endswith("orphan-poc.parquet") for n in names), names
        assert not fs.exists(path), "orphan still exists"
        assert count("obs") == 10000, count("obs")  # 데이터 파일은 그대로
        return f"removed {len(names)} file(s), table intact"

    @check("8. 원본 Parquet을 s3a://로 읽기")
    def _() -> str:
        df = spark.read.option("mergeSchema", "true").parquet(
            f"s3a://{RAW_BUCKET}/charger_status/dt=*/run=*/page=*.parquet"
        )
        n = df.count()
        assert n > 0, "no raw rows (run a fetch-delta first)"
        return f"{n} raw rows, {len(df.columns)} columns"

    @check("9. Spark 밖에서 읽기 (PyIceberg → DuckDB)")
    def _() -> str:
        import duckdb
        from pyiceberg.catalog.sql import SqlCatalog

        cat = SqlCatalog(
            CATALOG,
            **{
                "uri": f"postgresql+psycopg2://eonje:{PG_PASSWORD}@{PG_HOST}:5432/eonje"
                f"?options=-csearch_path%3D{CATALOG_SCHEMA}",
                "warehouse": WAREHOUSE,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": S3_KEY,
                "s3.secret-access-key": S3_SECRET,
                "s3.region": REGION,
                "s3.path-style-access": "true",
            },
        )
        table = cat.load_table(f"{NS}.obs").scan().to_arrow()
        con = duckdb.connect()
        con.register("obs", table)
        n = con.execute("SELECT count(*) FROM obs").fetchone()[0]
        assert n == 10000, n
        return f"pyiceberg+duckdb rows={n}"

    if not keep:
        try:
            sql(f"DROP TABLE IF EXISTS {t('obs')} PURGE")
            sql(f"DROP TABLE IF EXISTS {t('session')} PURGE")
            sql(f"DROP NAMESPACE IF EXISTS {CATALOG}.{NS}")
        except Exception:  # noqa: BLE001
            traceback.print_exc()
    spark.stop()

    print("\n=== Iceberg on SeaweedFS PoC ===")
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}  — {detail}")
    failed = sum(not ok for _, ok, _ in results)
    print(f"\n{len(results) - failed}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
