# 언제충전 (eonje)

[![ci](https://github.com/jungyu333/eonje/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jungyu333/eonje/actions/workflows/ci.yml?query=branch%3Amain)

전기차 충전기의 **이력**을 쌓아 "이 충전소는 언제 비는가"에 답하는 서비스.

공식 API와 기존 앱은 충전기의 현재 상태만 보여준다. 언제충전은 전국 충전기 상태를
계속 수집해 충전소별 요일×시간대 점유 패턴, 장기 비정상 충전기, 빈 충전기가 있을 확률을
제공한다.

## 구조

```
환경부 EvCharger API → Airflow 수집 → 원본 Parquet (레이크) → Spark 정제 → Iceberg DW → 마트 (Postgres) → FastAPI → 웹
```

- 수집: 변경분 10분 주기 + 전체 1일 1회. 일 호출 한도 안에서 동작하도록 예산 관리
- 원본은 가공 없이 보존하고, 모든 정제·집계는 원본에서 재생성 가능
- DW는 SeaweedFS 위 Iceberg 테이블(팩트·디멘션), API가 읽는 집계와 현재 상태는 Postgres
- 파이프라인 코드 (`eonje/`)는 Airflow에 의존하지 않고, 태스크는 별도 컨테이너에서 실행

## 문서

- [요구사항](docs/requirements.md) — 무엇을 만들고 만들지 않는지, 수용 기준
- [설계](docs/design.md) — 아키텍처, 데이터 모델, 수집 전략, 기술 스택

## 로컬에서 수집 돌려 보기

필요한 것: [uv](https://docs.astral.sh/uv/), Docker, 공공데이터포털 EvCharger API 서비스 키.

**1. 설정**

```bash
make install
cp infra/.env.example .env
```

`.env`에서 `CHANGE_ME`와 빈 값을 채운다. 개발 PC에서는 호스트를 `127.0.0.1`로 바꾼다
(`localhost`는 Windows·Docker Desktop에서 IPv6로 먼저 붙어 멈출 수 있다).

```dotenv
EONJE_API_SERVICE_KEY=<발급받은 키>
EONJE_DATABASE_URL=postgresql+psycopg://eonje:<EONJE_DB_PASSWORD>@127.0.0.1:5432/eonje
EONJE_S3_ENDPOINT_URL=http://127.0.0.1:8333
EONJE_S3_ACCESS_KEY=<임의 값>
EONJE_S3_SECRET_KEY=<임의 값>
```

S3 키는 SeaweedFS가 이 값으로 계정을 만들므로 아무 값이나 정하면 된다. 비어 있으면 빈 키로 열린 저장소가
뜨지 않도록 compose가 멈춘다. 그래서 `make up SERVICES=postgres`처럼 S3를 안 쓰는 명령도 키를 먼저 채워야 한다.

**2. 인프라와 스키마**

```bash
make up        # Postgres + SeaweedFS
make migrate   # eonje DB에 테이블 생성
make buckets   # 원본 버킷 생성 (이미 있으면 그대로 성공)
```

**3. 수집 1회**

```bash
# RUN_AFTER는 지금 시각(UTC). PowerShell: (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss+00:00")
make run JOB=fetch-delta RUN_ID=manual__1 RUN_AFTER=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)
```

- `RUN_AFTER`는 Airflow의 `run_after` 자리다. 수집 시각과 날짜 파티션(KST)이 이 값으로 정해진다
- API는 지금부터 최근 10분(`EONJE_STATUS_PERIOD_MIN`)만 주므로, `RUN_AFTER`가 지금과 그보다 멀면 받지 않는다
- 완료된 run은 다시 받지 않는다. 같은 `RUN_ID`로 다시 돌리면 호출 없이 끝나므로, 다시 수집하려면 새 `RUN_ID`를 쓴다
- 종료 코드: `0` 수집 완료 또는 이미 완료된 run, `99` 건너뜀(예산 소진, 늦은 `RUN_AFTER`), `1` 실패, `2` 사용법·설정 오류

**4. 결과 확인**

- 원본: `eonje-raw/charger_status/dt=<KST 날짜>/run=<run_key>/` 아래 `page=NNN.parquet`와
  `_manifest.json`. manifest가 있어야 완료된 run이다
- 호출 기록: `make psql` 후 `SELECT * FROM api_call_log ORDER BY id DESC LIMIT 5;`
- 로그 경고: `run truncated by budget cap`은 예산 때문에 페이지를 덜 받은 것,
  `totalCount drifted during paging`은 페이지 사이에 원본이 바뀐 것 ([설계 §5](docs/design.md#5-수집-전략))

DB 테스트는 `make up` 상태에서 `make test-db`로 돈다 (세션마다 임시 스키마를 만들고 지운다).

## 상태

초기 구축 중. 수집기부터 올린다.

## 데이터 출처

한국환경공단 전기자동차 충전소 정보 (공공데이터포털)
