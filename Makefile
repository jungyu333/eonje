.DEFAULT_GOAL := help
.PHONY: help install hooks lint format typecheck test check build run test-pdb test-debug up down psql migrate migration test-db migration-check buckets migrate-image airflow-recreate dags-check dag-test dag-pause dags-verify

TAG ?= dev
COMPOSE := docker compose -f infra/compose.node-a.yaml --env-file .env

help:            ## 타깃 목록
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

install:         ## 의존성 설치 (dev, api 포함)
	uv sync --all-extras

hooks:           ## pre-commit 훅 설치 (clone 후 1회)
	uv run pre-commit install

lint:            ## ruff 검사 + 포맷 확인
	uv run ruff check .
	uv run ruff format --check .

format:          ## ruff 자동 수정 + 포맷
	uv run ruff check --fix .
	uv run ruff format .

typecheck:       ## mypy
	uv run mypy

test:            ## pytest. 예: make test ARGS="-k cli"
	uv run pytest $(ARGS)

test-db:         ## DB 테스트 (make up 필요)
	uv run pytest -m db tests/db $(ARGS)

test-debug:      ## 첫 실패에서 멈추고 로컬 변수·전체 출력 표시
	uv run pytest -x -vv -l -s --tb=long $(ARGS)

test-pdb:        ## 첫 실패 지점에서 pdb 진입
	uv run pytest -x --pdb $(ARGS)

check: lint typecheck test   ## CI와 동일한 전체 검사

build:           ## eonje 이미지 빌드
	docker build -f docker/tasks/collector/Dockerfile -t eonje-collector:$(TAG) .
	docker build -f docker/airflow/Dockerfile -t eonje-airflow:$(TAG) docker/airflow

run:             ## job 실행. 예: make run JOB=fetch-delta RUN_ID=manual__x RUN_AFTER=2026-10-28T01:10:00+00:00
	$(if $(and $(JOB),$(RUN_ID),$(RUN_AFTER)),,$(error usage: make run JOB=<job> RUN_ID=<id> RUN_AFTER=<ISO 8601 with offset like 2026-10-28T01:10:00+00:00>))
	uv run python -m eonje.cli $(JOB) --run-id $(RUN_ID) --run-after $(RUN_AFTER)

up:              ## 로컬 인프라 시작. 일부만: make up SERVICES=postgres
	$(COMPOSE) up -d --wait $(SERVICES)

down:            ## 로컬 인프라 중지 (데이터 볼륨 유지)
	$(COMPOSE) down

psql:            ## eonje DB에 psql 접속
	$(COMPOSE) exec postgres psql -U eonje -d eonje

migrate:         ## 마이그레이션 적용 (head까지)
	uv run alembic upgrade head

migration:       ## 새 리비전 생성. 예: make migration MSG="add dim tables"
	uv run alembic revision --autogenerate -m "$(MSG)"

migration-check: ## models.py와 마이그레이션이 일치하는지 (DB 필요)
	uv run alembic check

buckets:         ## 원본 버킷 생성 (이미 있으면 성공으로 봄)
	$(COMPOSE) exec -T seaweedfs sh -c 'b="$${EONJE_RAW_BUCKET:-eonje-raw}"; out=$$(echo "s3.bucket.create -name $$b" | weed shell 2>&1); echo "$$out"; case "$$out" in *"created bucket"*|*"already exists"*) exit 0 ;; *) exit 1 ;; esac'

migrate-image:   ## 이미지 안의 마이그레이션 적용 (CD와 같은 경로). make build 먼저
	$(COMPOSE) run --rm migrate

airflow-recreate: ## .env 변경(비밀번호 등)을 Airflow api-server에 반영
	$(COMPOSE) up -d --force-recreate airflow-api-server

dags-check:      ## Airflow 컨테이너에서 DAG import 오류 확인 (make up 필요)
	$(COMPOSE) exec airflow-dag-processor airflow dags list-import-errors

# MSYS_NO_PATHCONV: Windows Git Bash가 /opt/... 인자를 C:/Program Files/Git/opt/...로 바꾸지 않게 (Linux에선 무시됨)
dags-verify:     ## Airflow 이미지로 DAG 무결성 검사 (CI와 같음, DB 불필요). make build 먼저
	MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR)/dags:/opt/airflow/dags:ro" \
		-v "$(CURDIR)/docker/airflow/check_dags.py:/opt/airflow/check_dags.py:ro" \
		eonje-airflow:$(TAG) python /opt/airflow/check_dags.py /opt/airflow/dags

dag-test:        ## DAG를 켜지 않고 한 번 실행. 예: make dag-test DAG=ev_status_10min
	$(if $(DAG),,$(error usage: make dag-test DAG=<dag_id>))
	$(COMPOSE) exec airflow-scheduler airflow dags test $(DAG)

dag-pause:       ## DAG 끄기 (실수로 켰을 때). 예: make dag-pause DAG=ev_status_10min
	$(if $(DAG),,$(error usage: make dag-pause DAG=<dag_id>))
	$(COMPOSE) exec airflow-scheduler airflow dags pause $(DAG)
