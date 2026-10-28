.DEFAULT_GOAL := help
.PHONY: help install hooks lint format typecheck test check build run test-pdb test-debug up down psql migrate migration test-db migration-check

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

run:             ## job 실행. 예: make run JOB=fetch-delta RUN_ID=manual__x RUN_AFTER=2026-10-28T01:10:00+00:00
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
