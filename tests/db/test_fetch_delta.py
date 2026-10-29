"""DB tests for eonje.cli.jobs.fetch_delta: budget -> paged fetch -> raw run + api_call_log.

HTTP is mocked at the transport level, the raw store is an in-memory fsspec filesystem,
and api_call_log is real Postgres (rolled back per test). The job's wall-clock "now"
(used only for the quota day) is frozen.
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

from eonje.cli import exit_codes
from eonje.cli.context import JobContext
from eonje.cli.jobs import fetch_delta as job
from eonje.client.errors import ApiError
from eonje.client.types import CallRecord
from eonje.config import Settings
from eonje.storage.call_log import CallLogWriter
from eonje.storage.models import ApiCallLog
from eonje.storage.raw_paths import run_key_from_run_id

pytestmark = pytest.mark.db

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "evcharger"
STATUS_PAGE = (FIXTURES / "real_status_page.json").read_text(encoding="utf-8")
GATEWAY_ERROR = (FIXTURES / "real_gateway_error.json").read_text(encoding="utf-8")
FIXTURE_ITEMS = 10  # items in real_status_page.json
FIXTURE_TOTAL = 16239  # its totalCount -> 2 pages at page_size=9999

BUCKET = "eonje-raw"
RUN_ID = "scheduled__2026-10-28T16:00:00+00:00"
# 2026-10-29 01:00 KST: the UTC date and the KST date differ
RUN_AFTER = datetime(2026, 10, 28, 16, 0, tzinfo=UTC)
# frozen wall clock, deliberately != RUN_AFTER so collected_at provably comes from run_after
NOW = datetime(2026, 10, 28, 16, 5, tzinfo=UTC)
# quota day containing NOW: 2026-10-29 00:00 KST .. 2026-10-30 00:00 KST
QUOTA_START = datetime(2026, 10, 28, 15, 0, tzinfo=UTC)
QUOTA_END = QUOTA_START + timedelta(days=1)

RUN_PREFIX = f"{BUCKET}/charger_status/dt=2026-10-29/run={run_key_from_run_id(RUN_ID)}"
MANIFEST = f"{RUN_PREFIX}/_manifest.json"


@dataclass
class FakeApi:
    """Answers scripted responses in order, then the real status page for every request."""

    scripted: list[httpx.Response] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.scripted:
            return self.scripted.pop(0)
        return httpx.Response(200, text=STATUS_PAGE)

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
        "raw_bucket": BUCKET,
    }
    return Settings(**{**base, **overrides})


def _run(
    api: FakeApi,
    fs: MemoryFileSystem,
    factory: sessionmaker[Session],
    *,
    settings: Settings | None = None,
    run_after: datetime = RUN_AFTER,
) -> int:
    ctx = JobContext(run_id=RUN_ID, run_after=run_after, settings=settings or _settings())
    with httpx.Client(transport=httpx.MockTransport(api)) as http:
        return job.fetch_delta(
            ctx, http=http, fs=fs, factory=factory, wait=wait_none(), now=lambda: NOW
        )


def _log_rows(factory: sessionmaker[Session]) -> list[ApiCallLog]:
    with factory() as session:
        return list(session.scalars(select(ApiCallLog).order_by(ApiCallLog.id)))


def _manifest(fs: MemoryFileSystem) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads(fs.cat_file(MANIFEST))
    return manifest


def _files(fs: MemoryFileSystem) -> list[str]:
    return sorted(p.lstrip("/") for p in fs.find(""))


def _seed_calls(factory: sessionmaker[Session], *called_at: datetime) -> None:
    writer = CallLogWriter(factory, job="seed", run_id="seed")
    for n, at in enumerate(called_at, start=1):
        writer(
            CallRecord(
                operation="getChargerStatus",
                page_no=1,
                params={},
                called_at=at,
                elapsed_ms=1,
                attempt=n,
                http_status=200,
                result_code="00",
                total_count=FIXTURE_TOTAL,
            )
        )


# --- normal run ---------------------------------------------------------------


def test_normal_run_exits_ok_and_writes_both_pages_and_the_manifest(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API whose totalCount needs 2 pages at page_size=9999 and plenty of budget
    api = FakeApi()

    # WHEN the job runs
    code = _run(api, mem_fs, session_factory)

    # THEN it exits OK after requesting pages 1 and 2
    assert code == exit_codes.OK
    assert api.page_nos == ["1", "2"]
    # AND the run directory holds page=001, page=002 and the manifest
    assert _files(mem_fs) == [
        MANIFEST,
        f"{RUN_PREFIX}/page=001.parquet",
        f"{RUN_PREFIX}/page=002.parquet",
    ]
    manifest = _manifest(mem_fs)
    assert manifest["pages"] == [1, 2]
    assert manifest["total_count"] == FIXTURE_TOTAL
    assert manifest["row_count"] == 2 * FIXTURE_ITEMS
    assert manifest["page_size"] == 9999
    assert manifest["expected_pages"] == 2


def test_normal_run_logs_one_call_per_page_tagged_with_job_and_run_id(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN a 2-page API
    api = FakeApi()

    # WHEN the job runs
    _run(api, mem_fs, session_factory)

    # THEN api_call_log has 2 rows, one per page, carrying the job name and run_id
    rows = _log_rows(session_factory)
    assert [(r.page_no, r.attempt, r.http_status) for r in rows] == [(1, 1, 200), (2, 1, 200)]
    assert all(r.job == "fetch-delta" and r.run_id == RUN_ID for r in rows)
    assert all("serviceKey" not in r.params for r in rows)


def test_parquet_collected_at_is_run_after_not_wall_clock(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN a run_after that differs from the (frozen) wall clock
    assert RUN_AFTER != NOW

    # WHEN the job runs
    _run(FakeApi(), mem_fs, session_factory)

    # THEN every row of every page has collected_at == run_after, and so does the manifest
    for page in ("page=001.parquet", "page=002.parquet"):
        table = pq.read_table(io.BytesIO(mem_fs.cat_file(f"{RUN_PREFIX}/{page}")))
        assert set(table.column("collected_at").to_pylist()) == {RUN_AFTER}
    assert _manifest(mem_fs)["collected_at"] == RUN_AFTER.isoformat()


def test_dt_partition_is_the_kst_date_of_run_after(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN run_after 2026-10-28T16:00Z, which is 2026-10-29 01:00 KST
    run_after = datetime(2026, 10, 28, 16, 0, tzinfo=UTC)

    # WHEN the job runs
    _run(FakeApi(), mem_fs, session_factory, run_after=run_after)

    # THEN everything lands under dt=2026-10-29 and nothing under the UTC date
    files = _files(mem_fs)
    assert files
    assert all("/dt=2026-10-29/" in f for f in files)
    assert _manifest(mem_fs)["dt"] == "2026-10-29"


# --- budget -----------------------------------------------------------------------


def test_insufficient_budget_skips_without_any_request_file_or_log(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN a daily limit too small for even one page at 3 attempts
    api = FakeApi()
    settings = _settings(api_daily_limit=1)

    # WHEN the job runs
    code = _run(api, mem_fs, session_factory, settings=settings)

    # THEN it exits SKIPPED, makes no HTTP request, writes no file and logs no call
    assert code == exit_codes.SKIPPED
    assert api.requests == []
    assert _files(mem_fs) == []
    assert _log_rows(session_factory) == []


def test_budget_cap_of_one_page_truncates_the_run(
    mem_fs: MemoryFileSystem,
    session_factory: sessionmaker[Session],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # GIVEN a budget of 3 calls at 3 attempts per page -> cap of 1 page
    api = FakeApi()
    settings = _settings(api_daily_limit=3, budget_safety_ratio=1.0)

    # WHEN the job runs
    code = _run(api, mem_fs, session_factory, settings=settings)

    # THEN it exits OK having fetched only page 1, and the manifest shows a truncated run
    assert code == exit_codes.OK
    assert api.page_nos == ["1"]
    manifest = _manifest(mem_fs)
    assert manifest["pages"] == [1]
    assert manifest["row_count"] < manifest["total_count"]
    assert manifest["expected_pages"] == 2
    assert not mem_fs.exists(f"{RUN_PREFIX}/page=002.parquet")
    # AND the warning names the budget cap, not drift
    assert "run truncated by budget cap: 1 of 2 pages" in caplog.text
    assert "drifted" not in caplog.text


def test_all_pages_with_fewer_rows_than_total_is_logged_as_drift_not_truncation(
    mem_fs: MemoryFileSystem,
    session_factory: sessionmaker[Session],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # GIVEN an API that returns 2 pages whose rows do not add up to totalCount
    api = FakeApi()

    # WHEN the job runs with enough budget for both pages
    code = _run(api, mem_fs, session_factory)

    # THEN it exits OK and warns about drift, not about the budget
    assert code == exit_codes.OK
    drift = FIXTURE_TOTAL - 2 * FIXTURE_ITEMS
    assert f"totalCount drifted during paging: {2 * FIXTURE_ITEMS} rows written" in caplog.text
    assert f"(drift {drift:+d})" in caplog.text
    assert "truncated" not in caplog.text


@pytest.mark.parametrize(
    ("already_used", "expected_code", "expected_pages"),
    [
        (0, exit_codes.OK, ["1", "2"]),  # 6 usable -> 2 pages
        (3, exit_codes.OK, ["1"]),  # 3 usable -> 1 page
        (4, exit_codes.SKIPPED, []),  # 2 usable -> 0 pages
    ],
)
def test_existing_calls_today_reduce_the_budget(
    mem_fs: MemoryFileSystem,
    session_factory: sessionmaker[Session],
    already_used: int,
    expected_code: int,
    expected_pages: list[str],
) -> None:
    # GIVEN a ceiling of 6 calls (3 attempts per page) and some calls already logged today
    _seed_calls(session_factory, *(QUOTA_START + timedelta(minutes=m) for m in range(already_used)))
    api = FakeApi()
    settings = _settings(api_daily_limit=6, budget_safety_ratio=1.0)

    # WHEN the job runs
    code = _run(api, mem_fs, session_factory, settings=settings)

    # THEN the remaining budget decides how many pages are fetched
    assert code == expected_code
    assert api.page_nos == expected_pages


def test_calls_outside_the_current_kst_quota_day_do_not_reduce_the_budget(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN a ceiling of 6 calls and many calls logged just before and at the end of today (KST)
    _seed_calls(
        session_factory,
        QUOTA_START - timedelta(microseconds=1),
        QUOTA_START - timedelta(hours=1),
        QUOTA_START - timedelta(hours=2),
        QUOTA_END,
        QUOTA_END + timedelta(hours=1),
    )
    api = FakeApi()
    settings = _settings(api_daily_limit=6, budget_safety_ratio=1.0)

    # WHEN the job runs
    code = _run(api, mem_fs, session_factory, settings=settings)

    # THEN the full budget is available: both pages are fetched
    assert code == exit_codes.OK
    assert api.page_nos == ["1", "2"]


# --- failures -------------------------------------------------------------------


def test_transient_500_on_page_one_is_retried_and_every_attempt_is_logged(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API that answers 500 once for page 1, then normally
    api = FakeApi(scripted=[httpx.Response(500, text="server error")])

    # WHEN the job runs
    code = _run(api, mem_fs, session_factory)

    # THEN the run completes normally
    assert code == exit_codes.OK
    assert _manifest(mem_fs)["pages"] == [1, 2]
    # AND 3 calls are logged: page 1 twice (500, 200) and page 2 once
    rows = _log_rows(session_factory)
    assert [(r.page_no, r.attempt, r.http_status) for r in rows] == [
        (1, 1, 500),
        (1, 2, 200),
        (2, 1, 200),
    ]


def test_api_gateway_error_raises_leaves_no_manifest_and_keeps_the_call_log(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN an API that answers the gateway "service key not registered" error
    api = FakeApi(scripted=[httpx.Response(200, text=GATEWAY_ERROR)])

    # WHEN the job runs THEN ApiError propagates, without a retry
    with pytest.raises(ApiError) as exc_info:
        _run(api, mem_fs, session_factory)
    assert exc_info.value.result_code == "30"
    assert len(api.requests) == 1

    # AND no manifest or page was written, so the run stays incomplete
    assert not mem_fs.exists(MANIFEST)
    assert _files(mem_fs) == []
    # AND the failed call is still in api_call_log
    (row,) = _log_rows(session_factory)
    assert (row.page_no, row.attempt, row.http_status, row.result_code) == (1, 1, 200, "30")
    assert row.run_id == RUN_ID


# --- idempotency ------------------------------------------------------------------


def test_rerunning_the_same_run_rewrites_the_same_files_and_manifest(
    mem_fs: MemoryFileSystem, session_factory: sessionmaker[Session]
) -> None:
    # GIVEN a completed run
    _run(FakeApi(), mem_fs, session_factory)
    first_files = _files(mem_fs)
    first_manifest = mem_fs.cat_file(MANIFEST)

    # WHEN the same run_id / run_after runs again
    code = _run(FakeApi(), mem_fs, session_factory)

    # THEN the same paths exist with a byte-identical manifest
    assert code == exit_codes.OK
    assert _files(mem_fs) == first_files
    assert mem_fs.cat_file(MANIFEST) == first_manifest
    # AND the call log grows by the calls actually made (2 + 2), all under the same run_id
    rows = _log_rows(session_factory)
    assert [r.page_no for r in rows] == [1, 2, 1, 2]
    assert {r.run_id for r in rows} == {RUN_ID}
