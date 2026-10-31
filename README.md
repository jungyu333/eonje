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

## Airflow로 돌려 보기

`make up`은 Airflow(api-server, scheduler, dag-processor)도 함께 띄운다. 먼저 `.env`에 Airflow 값을 채운다.

```dotenv
AIRFLOW_ADMIN_PASSWORD=<UI 비밀번호. " 와 \ 는 쓰지 않는다>
AIRFLOW_FERNET_KEY=<python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())">
AIRFLOW_JWT_SECRET=<python -c "import secrets; print(secrets.token_urlsafe(32))">
DOCKER_GID=0   # Docker Desktop. Linux는 getent group docker | cut -d: -f3
```

```bash
make build        # eonje-collector:dev, eonje-airflow:dev
make up
make dags-check   # DAG import 오류 확인. "No data found"면 정상
make dags-verify  # CI와 같은 DAG 무결성 검사 (Airflow 이미지만 있으면 됨, make up 불필요)
```

- UI: http://127.0.0.1:8080, `admin` / `AIRFLOW_ADMIN_PASSWORD`
- `ev_status_10min`은 **멈춘 상태(paused)로 등록**되고, 개발 PC에서는 켜지 않는다. 켜면 10분마다 수집한다
- 한 번 실행해 보려면 `airflow dags test`를 쓴다. 스케줄러를 거치지 않고 그 자리에서 끝까지 돌려서, DAG가 멈춰 있어도 된다

  ```bash
  make dag-test DAG=ev_status_10min
  ```

  UI의 Trigger나 `airflow dags trigger`는 쓰지 않는다. 멈춘 DAG의 run은 실행되지 않고 대기하고,
  UI Trigger는 기본으로 DAG를 함께 켠다(켜지는 순간 직전 스케줄 구간 run도 하나 더 돈다)
- 켜졌다면 끈다: `make dag-pause DAG=ev_status_10min`
- 개발 PC와 운영 노드는 같은 서비스 키, 즉 같은 하루 한도를 쓰지만 호출 기록은 각자의 DB에 있어 서로의 호출을 모른다.
  운영 수집이 돌기 시작하면 개발 PC `.env`의 `EONJE_API_DAILY_LIMIT`을 작게(예: 30) 두거나 개발용 키를 따로 쓴다
- `AIRFLOW_ADMIN_PASSWORD`를 바꾸면 `make airflow-recreate`. 비밀번호 파일 내용만 바뀐 것은 `make up`이 반영하지 않는다
- DAG는 `dags/`가 마운트돼 있어 저장하면 바로 다시 읽힌다(이미지 재빌드 불필요). 태스크 코드(`eonje/`)를 바꾸면 `make build`
- 이미지 안의 마이그레이션을 운영과 같은 방식으로 적용하려면 `make migrate-image`

## 운영 노드 (eonje-a)

main에 머지되면 CI가 이미지를 GHCR에 푸시하고 eonje-a에 배포한다 ([설계 §8.3](docs/design.md#83-배포-cicd)).
서버 `~/eonje/.env`는 사람이 관리하고, 나머지는 CD가 덮어쓴다.

| 대상 | 접근 |
|---|---|
| Airflow UI | tailnet 안에서 `http://eonje-a:8080` |
| Postgres | SSH 터널 (`ssh -N -L 15432:127.0.0.1:5432 eonje-a` 또는 PyCharm Database의 SSH 탭). 읽기 전용으로 연결 |
| SeaweedFS S3 | SSH 터널 (`-L 18333:127.0.0.1:8333`, Big Data Tools의 터널 옵션). endpoint `127.0.0.1`, path-style |
| SeaweedFS filer UI | 필요할 때만 `-L 18888:127.0.0.1:8888` → `http://localhost:18888` (로그인 없이 삭제 가능하니 주의) |

- 배포 계정은 `deploy`(`ssh eonje-deploy`), 관리·터널은 개인 계정(`ssh eonje-a`). `deploy` 키는 포워딩이 막혀 터널에 못 쓴다
- 롤백: Actions → deploy → Run workflow에 `~/eonje/image.env.prev`의 sha
- 운영에서도 DAG는 멈춘 상태로 등록된다. 첫 배포 후 UI에서 한 번 켠다

## 상태

수집기·Airflow DAG·CD까지 갖췄다. 운영 노드(eonje-a)에서 10분 수집을 시작한다.

## 데이터 출처

한국환경공단 전기자동차 충전소 정보 (공공데이터포털)
