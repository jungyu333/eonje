"""DB tests for eonje.collect.master: getChargerInfo -> charger_info raw run + api_call_log.

HTTP is mocked at the transport level with the real getChargerInfo page, the raw store is an
in-memory fsspec filesystem, and api_call_log is real Postgres (rolled back per test). The
flow's wall-clock "now" is frozen.
"""

import io
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from tenacity import wait_none

from eonje.collect.delta import collect_delta
from eonje.collect.master import collect_master
from eonje.collect.run import Outcome
from eonje.config import Settings
from eonje.storage.postgres.models import ApiCallLog
from eonje.storage.raw.paths import run_key_from_run_id

pytestmark = pytest.mark.db

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "evcharger"
INFO_PAGE = (FIXTURES / "real_info_page.json").read_text(encoding="utf-8")
STATUS_PAGE = (FIXTURES / "real_status_page.json").read_text(encoding="utf-8")
EMPTY_PAGE = (FIXTURES / "real_no_data.json").read_text(encoding="utf-8")
INFO_ITEMS: list[dict[str, str]] = json.loads(INFO_PAGE)["items"]["item"]  # 10 items
INFO_TOTAL = 76183  # totalCount of real_info_page.json
INFO_PAGES = 8  # ceil(76183 / 9999)

BUCKET = "eonje-raw"
# Airflow gives every DAG the same scheduled__ run_id for the same logical time
RUN_ID = "scheduled__2026-10-28T16:00:00+00:00"
# 2026-10-29 01:00 KST: the UTC date and the KST date differ
RUN_AFTER = datetime(2026, 10, 28, 16, 0, tzinfo=UTC)
NOW = datetime(2026, 10, 28, 16, 5, tzinfo=UTC)
MASTER_MAX_LAG = timedelta(minutes=60)

RUN_KEY = run_key_from_run_id(RUN_ID)
INFO_PREFIX = f"{BUCKET}/charger_info/dt=2026-10-29/run={RUN_KEY}"
INFO_MANIFEST = f"{INFO_PREFIX}/_manifest.json"
STATUS_PREFIX = f"{BUCKET}/charger_status/dt=2026-10-29/run={RUN_KEY}"


@dataclass
class FakeApi:
    """Answers `body` for every request and records the requests."""

    body: str = INFO_PAGE
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, text=self.body)

    @property
    def page_nos(self) -> list[str]:
        return [r.url.params["pageNo"] for r in self.requests]


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "api_service_key": "test",
        "api_base_url": "https://api.test/B552584/EvCharger",
        "page_size": 9999,
        "api_max_attempts": 3,
        "api_daily_limit": 1000,
        "budget_safety_ratio": 0.95,
        "budget_reserve_calls": 0,
        "status_period_min": 10,
        "fetch_max_lag_min": 10,
        "master_max_lag_min": 60,
        "raw_bucket": BUCKET,
    }
    return Settings(**{**base, **overrides})


def _run(
    api: FakeApi,
    fs: MemoryFileSystem,
    factory: sessionmaker[Session],
    *,
    run_after: datetime = RUN_AFTER,
) -> Outcome:
    with httpx.Client(transport=httpx.MockTransport(api)) as http:
        result = collect_master(
            _settings(),
            job="fetch-master",
            run_id=RUN_ID,
            run_after=run_after,
            http=http,
            fs=fs,
            factory=factory,
            wait=wait_none(),
            now=lambda: NOW,
        )
    return result.outcome


def _log_rows(factory: sessionmaker[Session]) -> list[ApiCallLog]:
    with factory() as session:
        return list(session.scalars(select(ApiCallLog).order_by(ApiCallLog.id)))


def _manifest(fs: MemoryFileSystem) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads(fs.cat_file(INFO_MANIFEST))
    return manifest


def _files(fs: MemoryFileSystem) -> list[str]:
    return sorted(p.lstrip("/") for p in fs.find(""))


def _all_page_nos() -> list[int]:
    return list(range(1, INFO_PAGES + 1))


# --- one master collection ---------------------------------------------------------


def test_master_run_writes_every_page_and_the_manifest_under_charger_info(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API answering the real getChargerInfo page (totalCount 76183 -> 8 pages)
    api = FakeApi()

    # WHEN one master collection runs
    outcome = _run(api, mem_fs, session_factory)

    # THEN it is collected after requesting all 8 pages
    assert outcome is Outcome.COLLECTED
    assert api.page_nos == [str(n) for n in _all_page_nos()]
    # AND every object is in the charger_info run directory, nothing under charger_status
    assert _files(mem_fs) == [
        INFO_MANIFEST,
        *(f"{INFO_PREFIX}/page={n:03d}.parquet" for n in _all_page_nos()),
    ]


def test_master_manifest_names_the_charger_info_dataset_and_get_charger_info(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API answering the real getChargerInfo page
    api = FakeApi()

    # WHEN one master collection runs
    _run(api, mem_fs, session_factory)

    # THEN the manifest records the dataset, the operation and a complete run
    manifest = _manifest(mem_fs)
    assert manifest["dataset"] == "charger_info"
    assert manifest["operation"] == "getChargerInfo"
    assert manifest["dt"] == "2026-10-29"
    assert manifest["run_key"] == RUN_KEY
    assert manifest["collected_at"] == RUN_AFTER.isoformat()
    assert manifest["total_count"] == INFO_TOTAL
    assert manifest["expected_pages"] == INFO_PAGES
    assert manifest["pages"] == _all_page_nos()
    assert manifest["row_count"] == INFO_PAGES * len(INFO_ITEMS)


def test_master_raw_page_keeps_the_info_items_as_received(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API answering the real getChargerInfo page
    api = FakeApi()

    # WHEN one master collection runs
    _run(api, mem_fs, session_factory)

    # THEN page 1 holds every info field of every item as the received string
    table = pq.read_table(io.BytesIO(mem_fs.cat_file(f"{INFO_PREFIX}/page=001.parquet")))
    fields = list(INFO_ITEMS[0])
    assert table.column_names == [*fields, "collected_at", "page_fetched_at", "page_no"]
    assert table.select(fields).to_pylist() == INFO_ITEMS


def test_master_run_logs_one_get_charger_info_call_per_page_tagged_with_job_and_run_id(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API answering the real getChargerInfo page
    api = FakeApi()

    # WHEN one master collection runs
    _run(api, mem_fs, session_factory)

    # THEN api_call_log has one successful getChargerInfo call per page
    rows = _log_rows(session_factory)
    assert [(r.operation, r.page_no, r.attempt, r.http_status, r.result_code) for r in rows] == [
        ("getChargerInfo", n, 1, 200, "00") for n in _all_page_nos()
    ]
    # AND each row carries the job, the Airflow run_id and the page 1 totalCount
    assert all(r.job == "fetch-master" and r.run_id == RUN_ID for r in rows)
    assert all(r.total_count == INFO_TOTAL for r in rows)
    # AND the logged params are the paging params only: no period, no serviceKey
    assert [r.params for r in rows] == [
        {"pageNo": str(n), "numOfRows": "9999", "dataType": "JSON"} for n in _all_page_nos()
    ]


def test_master_requests_hit_get_charger_info_with_only_the_paging_params(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API answering the real getChargerInfo page
    api = FakeApi()

    # WHEN one master collection runs
    _run(api, mem_fs, session_factory)

    # THEN every request goes to getChargerInfo without a period or filter param
    assert api.requests
    assert all(r.url.path == "/B552584/EvCharger/getChargerInfo" for r in api.requests)
    assert all(
        set(r.url.params.keys()) == {"serviceKey", "pageNo", "numOfRows", "dataType"}
        for r in api.requests
    )


# --- empty input ----------------------------------------------------------------------


def test_empty_master_answer_completes_the_run_with_one_empty_page(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API answering a normal result with totalCount 0 and no items
    api = FakeApi(body=EMPTY_PAGE)

    # WHEN one master collection runs
    outcome = _run(api, mem_fs, session_factory)

    # THEN only page 1 is requested and the run completes with zero rows
    assert outcome is Outcome.COLLECTED
    assert api.page_nos == ["1"]
    manifest = _manifest(mem_fs)
    assert (manifest["pages"], manifest["total_count"], manifest["row_count"]) == ([1], 0, 0)
    assert [r.operation for r in _log_rows(session_factory)] == ["getChargerInfo"]


# --- idempotency and dataset separation --------------------------------------------------


def test_rerunning_a_completed_master_run_fetches_nothing_and_keeps_the_original(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN a completed master run
    _run(FakeApi(), mem_fs, session_factory)
    first = {f: mem_fs.cat_file(f) for f in _files(mem_fs)}

    # WHEN the same run_id / run_after runs again
    api = FakeApi()
    outcome = _run(api, mem_fs, session_factory)

    # THEN it is already complete, makes no request and leaves the raw objects untouched
    assert outcome is Outcome.ALREADY_COMPLETE
    assert api.requests == []
    assert {f: mem_fs.cat_file(f) for f in _files(mem_fs)} == first
    # AND only the first run's calls are logged
    assert [r.page_no for r in _log_rows(session_factory)] == _all_page_nos()


def test_a_completed_delta_run_with_the_same_run_id_does_not_block_the_master_run(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN a completed charger_status run with the same run_id and run_after
    with httpx.Client(transport=httpx.MockTransport(FakeApi(body=STATUS_PAGE))) as http:
        delta = collect_delta(
            _settings(),
            job="fetch-delta",
            run_id=RUN_ID,
            run_after=RUN_AFTER,
            http=http,
            fs=mem_fs,
            factory=session_factory,
            wait=wait_none(),
            now=lambda: NOW,
        )
    assert delta.outcome is Outcome.COLLECTED
    status_objects = {f: mem_fs.cat_file(f) for f in _files(mem_fs)}
    assert status_objects
    assert all(f.startswith(f"{STATUS_PREFIX}/") for f in status_objects)

    # WHEN the master run for the same run_id runs
    api = FakeApi()
    outcome = _run(api, mem_fs, session_factory)

    # THEN it is collected into charger_info and the status run is untouched
    assert outcome is Outcome.COLLECTED
    assert api.page_nos == [str(n) for n in _all_page_nos()]
    assert mem_fs.exists(INFO_MANIFEST)
    assert {f: mem_fs.cat_file(f) for f in status_objects} == status_objects


# --- master_max_lag -------------------------------------------------------------------


@pytest.mark.parametrize(
    "offset",
    [
        -timedelta(minutes=30),  # beyond the 10-minute delta lag, within the master lag
        -MASTER_MAX_LAG,  # exactly at the master lag: the boundary is inclusive
        MASTER_MAX_LAG,
    ],
)
def test_run_after_within_the_master_max_lag_is_collected(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session], offset: timedelta
) -> None:
    # GIVEN master_max_lag_min=60, fetch_max_lag_min=10 and a run_after `offset` from now
    api = FakeApi()

    # WHEN the master collection runs
    outcome = _run(api, mem_fs, session_factory, run_after=NOW + offset)

    # THEN the master lag, not the delta lag, decides: it is collected
    assert outcome is Outcome.COLLECTED
    assert api.page_nos == [str(n) for n in _all_page_nos()]


@pytest.mark.parametrize(
    "offset",
    [
        -MASTER_MAX_LAG - timedelta(microseconds=1),  # late or catchup run
        MASTER_MAX_LAG + timedelta(microseconds=1),  # run_after too far in the future
    ],
)
def test_run_after_beyond_the_master_max_lag_is_skipped_without_any_request_file_or_log(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session], offset: timedelta
) -> None:
    # GIVEN master_max_lag_min=60 and a run_after just over 60 minutes away from now
    api = FakeApi()

    # WHEN the master collection runs
    outcome = _run(api, mem_fs, session_factory, run_after=NOW + offset)

    # THEN it is skipped as stale: no HTTP request, no file, no call log
    assert outcome is Outcome.SKIPPED_STALE
    assert api.requests == []
    assert _files(mem_fs) == []
    assert _log_rows(session_factory) == []
