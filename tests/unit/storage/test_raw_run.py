import io
import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from eonje.client.response import parse_response
from eonje.client.types import Page
from eonje.storage.raw_paths import RawRun, manifest_path, page_path, run_prefix
from eonje.storage.raw_run import MANIFEST_VERSION, RawRunWriter, RunManifest, read_manifest

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"
BUCKET = "eonje-raw"
KST = timezone(timedelta(hours=9))
COLLECTED_AT = datetime(2026, 10, 26, 10, 10, 0, tzinfo=UTC)
FETCHED_AT = datetime(2026, 10, 26, 10, 10, 3, 250000, tzinfo=UTC)


def _fixture_items(name: str) -> list[dict[str, Any]]:
    return parse_response((FIXTURES / name).read_text(encoding="utf-8")).items


def _page(
    items: list[dict[str, Any]],
    page_no: int = 1,
    operation: str = "getChargerStatus",
    total_count: int | None = None,
    fetched_at: datetime = FETCHED_AT,
) -> Page:
    return Page(
        operation=operation,
        page_no=page_no,
        total_count=len(items) if total_count is None else total_count,
        items=items,
        fetched_at=fetched_at,
    )


@pytest.fixture
def status_items() -> list[dict[str, Any]]:
    return _fixture_items("real_status_page.json")


def _started(
    fs: MemoryFileSystem, run: RawRun, collected_at: datetime = COLLECTED_AT
) -> RawRunWriter:
    writer = RawRunWriter(fs, BUCKET, run, collected_at)
    writer.start()
    return writer


def _objects(fs: MemoryFileSystem, prefix: str) -> list[str]:
    return sorted(p.lstrip("/") for p in fs.find(prefix))


def _read(fs: MemoryFileSystem, path: str) -> pa.Table:
    return pq.read_table(io.BytesIO(fs.cat_file(path)))


# --- construction ---------------------------------------------------------------------


def test_writer_rejects_a_naive_collected_at(mem_fs: MemoryFileSystem, run: RawRun) -> None:
    # GIVEN a collection time without a timezone
    naive = datetime(2026, 10, 26, 19, 10, 0)
    # WHEN a writer is created
    # THEN it raises before anything is touched
    with pytest.raises(ValueError, match="collected_at"):
        RawRunWriter(mem_fs, BUCKET, run, naive)
    assert mem_fs.find(f"{BUCKET}/") == []


# --- start ----------------------------------------------------------------------------


def test_start_on_a_fresh_run_leaves_the_store_empty(mem_fs: MemoryFileSystem, run: RawRun) -> None:
    # GIVEN no prior objects for the run
    writer = RawRunWriter(mem_fs, BUCKET, run, COLLECTED_AT)
    # WHEN the writer is started
    writer.start()
    # THEN nothing is created
    assert mem_fs.find(f"{BUCKET}/") == []


def test_start_removes_leftovers_of_an_earlier_attempt_of_the_same_run(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN an earlier attempt that wrote pages 1..3 and a manifest
    first = _started(mem_fs, run)
    for n in (1, 2, 3):
        first.write(_page(status_items, page_no=n))
    first.finish()
    # WHEN the run is retried
    _started(mem_fs, run)
    # THEN the run directory is gone, including the manifest
    assert _objects(mem_fs, f"{BUCKET}/") == []
    assert read_manifest(mem_fs, BUCKET, run) is None


def test_start_does_not_touch_other_runs(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a completed sibling run on the same day and a run on another day
    sibling = RawRun(dataset=run.dataset, dt=run.dt, run_key="20261026T100000")
    other_day = RawRun(dataset=run.dataset, dt=date(2026, 10, 25), run_key=run.run_key)
    for r in (sibling, other_day):
        w = _started(mem_fs, r)
        w.write(_page(status_items))
        w.finish()
    before = _objects(mem_fs, f"{BUCKET}/")
    # WHEN the target run is started
    _started(mem_fs, run)
    # THEN the other runs are intact
    assert _objects(mem_fs, f"{BUCKET}/") == before


def test_start_does_not_remove_a_run_whose_key_shares_a_prefix(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a run whose key extends the target run's key
    longer = RawRun(dataset=run.dataset, dt=run.dt, run_key=run.run_key + "_retry")
    w = _started(mem_fs, longer)
    w.write(_page(status_items))
    w.finish()
    before = _objects(mem_fs, f"{BUCKET}/")
    # WHEN the target run is started
    _started(mem_fs, run)
    # THEN the longer-keyed run survives
    assert _objects(mem_fs, f"{BUCKET}/") == before


# --- write ----------------------------------------------------------------------------


def test_write_before_start_raises_and_writes_nothing(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a writer that was not started
    writer = RawRunWriter(mem_fs, BUCKET, run, COLLECTED_AT)
    # WHEN a page is written
    # THEN it raises and the store is empty
    with pytest.raises(RuntimeError, match="start"):
        writer.write(_page(status_items))
    assert mem_fs.find(f"{BUCKET}/") == []


def test_write_stores_the_page_at_its_page_path_and_returns_it(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a started writer
    writer = _started(mem_fs, run)
    # WHEN page 2 is written
    path = writer.write(_page(status_items, page_no=2))
    # THEN the returned path is the page path and holds every row
    assert path == page_path(BUCKET, run, 2)
    assert _read(mem_fs, path).num_rows == len(status_items)


def test_write_stamps_rows_with_the_writer_collected_at(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a writer with a collection time and pages fetched later
    writer = _started(mem_fs, run)
    # WHEN two pages fetched at different times are written
    p1 = writer.write(_page(status_items, page_no=1, fetched_at=FETCHED_AT))
    p2 = writer.write(_page(status_items, page_no=2, fetched_at=FETCHED_AT + timedelta(seconds=5)))
    # THEN both share the run's collected_at (the observation bucket)
    for path in (p1, p2):
        values = set(_read(mem_fs, path).column("collected_at").to_pylist())
        assert values == {COLLECTED_AT}


def test_write_rejects_the_same_page_twice_in_one_run(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN page 1 already written
    writer = _started(mem_fs, run)
    path = writer.write(_page(status_items, page_no=1))
    original = mem_fs.cat_file(path)
    # WHEN page 1 is written again with different content
    # THEN it raises and the stored object is unchanged
    with pytest.raises(ValueError, match="page 1 already written"):
        writer.write(_page(status_items[:1], page_no=1))
    assert mem_fs.cat_file(path) == original


def test_write_rejects_a_page_from_a_different_operation(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a run that started with getChargerStatus
    writer = _started(mem_fs, run)
    writer.write(_page(status_items, page_no=1))
    # WHEN a getChargerInfo page is written
    # THEN it raises and no page 2 object exists
    info_items = _fixture_items("real_info_page.json")
    with pytest.raises(ValueError, match="mixed operations"):
        writer.write(_page(info_items, page_no=2, operation="getChargerInfo"))
    assert not mem_fs.exists(page_path(BUCKET, run, 2))


def test_write_keeps_duplicate_observations_and_counts_them(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a page holding the same (statId, chgerId, statUpdDt) observation twice
    dup = [status_items[0], dict(status_items[0])]
    writer = _started(mem_fs, run)
    # WHEN it is written and the run finished
    path = writer.write(_page(dup))
    manifest = writer.finish()
    # THEN both rows are stored raw and counted
    assert _read(mem_fs, path).num_rows == 2
    assert manifest.row_count == 2


@pytest.mark.parametrize("code", ["7", ""])
def test_write_keeps_an_unknown_status_code(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]], code: str
) -> None:
    # GIVEN an item with a status code outside the known table
    item = {**status_items[0], "stat": code}
    writer = _started(mem_fs, run)
    # WHEN it is written
    path = writer.write(_page([item]))
    # THEN it is stored as received
    assert _read(mem_fs, path).column("stat").to_pylist() == [code]


def test_write_keeps_empty_optional_fields_as_received(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN an item whose lastTedt and nowTsdt are empty strings
    item = {**status_items[0], "lastTedt": "", "nowTsdt": ""}
    writer = _started(mem_fs, run)
    # WHEN it is written
    table = _read(mem_fs, writer.write(_page([item])))
    # THEN the empty strings are kept, not turned into nulls
    assert table.column("lastTedt").to_pylist() == [""]
    assert table.column("nowTsdt").to_pylist() == [""]


def test_write_keeps_a_heartbeat_observation_as_a_separate_row(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN two observations of one charger differing only in statUpdDt
    first = status_items[0]
    heartbeat = {**first, "statUpdDt": "20261025190410"}
    writer = _started(mem_fs, run)
    # WHEN they are written
    table = _read(mem_fs, writer.write(_page([first, heartbeat])))
    # THEN raw keeps both rows unchanged; transition derivation is not raw's job
    assert table.column("stat").to_pylist() == [first["stat"], first["stat"]]
    assert table.column("statUpdDt").to_pylist() == [first["statUpdDt"], "20261025190410"]


# --- finish / manifest ----------------------------------------------------------------


def test_finish_without_any_page_raises_and_writes_no_manifest(
    mem_fs: MemoryFileSystem, run: RawRun
) -> None:
    # GIVEN a started writer with no pages
    writer = _started(mem_fs, run)
    # WHEN it is finished
    # THEN it raises and the run is not marked complete
    with pytest.raises(RuntimeError, match="no pages"):
        writer.finish()
    assert read_manifest(mem_fs, BUCKET, run) is None


def test_finish_returns_a_manifest_describing_the_run(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN two pages of a 3-page result written
    writer = _started(mem_fs, run)
    writer.write(_page(status_items, page_no=1, total_count=25))
    writer.write(_page(status_items[:3], page_no=2, total_count=25))
    # WHEN the run is finished
    manifest = writer.finish()
    # THEN the manifest has the run identity, totalCount, and rows actually written
    assert manifest == RunManifest(
        version=MANIFEST_VERSION,
        dataset="charger_status",
        operation="getChargerStatus",
        dt="2026-10-26",
        run_key=run.run_key,
        collected_at="2026-10-26T10:10:00+00:00",
        total_count=25,
        row_count=len(status_items) + 3,
        pages=[1, 2],
    )


def test_finish_reports_fewer_rows_than_total_when_the_budget_cut_the_run(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN only page 1 of a large result written (page cap reached)
    writer = _started(mem_fs, run)
    writer.write(_page(status_items, page_no=1, total_count=5000))
    # WHEN the run is finished
    manifest = writer.finish()
    # THEN row_count is below total_count
    assert manifest.total_count == 5000
    assert manifest.row_count == len(status_items)


def test_finish_lists_pages_sorted_regardless_of_write_order(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN pages written out of order
    writer = _started(mem_fs, run)
    for n in (3, 1, 2):
        writer.write(_page(status_items, page_no=n))
    # WHEN the run is finished
    # THEN pages are listed ascending
    assert writer.finish().pages == [1, 2, 3]


def test_finish_stores_a_kst_collected_at_as_utc(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a collection time given in KST
    writer = _started(mem_fs, run, collected_at=datetime(2026, 10, 26, 19, 10, 0, tzinfo=KST))
    writer.write(_page(status_items))
    # WHEN the run is finished
    # THEN the manifest records the same instant in UTC
    assert writer.finish().collected_at == "2026-10-26T10:10:00+00:00"


def test_finish_of_an_empty_page_run_records_zero_rows(
    mem_fs: MemoryFileSystem, run: RawRun
) -> None:
    # GIVEN a run whose only page is empty
    items = _fixture_items("getChargerStatus_empty_page.json")
    writer = _started(mem_fs, run)
    writer.write(_page(items, total_count=0))
    # WHEN the run is finished
    manifest = writer.finish()
    # THEN the run is complete with zero rows
    assert manifest.row_count == 0
    assert manifest.total_count == 0
    assert manifest.pages == [1]
    assert read_manifest(mem_fs, BUCKET, run) == manifest


def test_finish_writes_the_manifest_as_sorted_utf8_json_at_the_manifest_path(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a run with one page
    writer = _started(mem_fs, run)
    writer.write(_page(status_items))
    # WHEN it is finished
    writer.finish()
    # THEN the manifest object is JSON with sorted keys under the run directory
    body = mem_fs.cat_file(manifest_path(BUCKET, run)).decode("utf-8")
    keys = list(json.loads(body))
    assert keys == sorted(keys)
    assert manifest_path(BUCKET, run).startswith(run_prefix(BUCKET, run) + "/")


def test_finish_does_not_count_a_page_whose_write_failed(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN page 1 written and page 2 rejected (field collides with a metadata column)
    writer = _started(mem_fs, run)
    writer.write(_page(status_items, page_no=1))
    bad = [{**status_items[0], "page_no": "2"}]
    with pytest.raises(ValueError, match="page_no"):
        writer.write(_page(bad, page_no=2))
    # WHEN the run is finished
    manifest = writer.finish()
    # THEN only page 1 is listed and counted
    assert manifest.pages == [1]
    assert manifest.row_count == len(status_items)


def test_finish_raises_when_the_only_attempted_page_failed_to_write(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a run whose first and only page was rejected by the page writer
    writer = _started(mem_fs, run)
    with pytest.raises(ValueError):
        writer.write(_page(status_items, fetched_at=datetime(2026, 10, 26, 10, 10, 3)))
    # WHEN the run is finished
    # THEN it raises like a run with no pages, and no manifest marks it complete
    with pytest.raises(RuntimeError, match="no pages"):
        writer.finish()
    assert read_manifest(mem_fs, BUCKET, run) is None


def test_a_failed_first_page_does_not_lock_the_run_to_its_operation(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a first page that was rejected by the page writer
    writer = _started(mem_fs, run)
    with pytest.raises(ValueError):
        writer.write(_page(status_items, fetched_at=datetime(2026, 10, 26, 10, 10, 3)))

    # WHEN a valid page of another operation is written
    writer.write(_page(_fixture_items("real_info_page.json"), operation="getChargerInfo"))

    # THEN it is accepted and the run belongs to that operation
    assert writer.finish().operation == "getChargerInfo"


def test_finish_raises_without_page_one(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN only page 2 written (page 1, which carries totalCount, is missing)
    writer = _started(mem_fs, run)
    writer.write(_page(status_items, page_no=2, total_count=25))

    # WHEN / THEN finishing fails and the run is not marked complete
    with pytest.raises(RuntimeError, match="page 1"):
        writer.finish()
    assert read_manifest(mem_fs, BUCKET, run) is None


def test_total_count_comes_from_page_one_even_when_written_later(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN page 2 written before page 1, with different totalCount values
    writer = _started(mem_fs, run)
    writer.write(_page(status_items, page_no=2, total_count=40))
    writer.write(_page(status_items, page_no=1, total_count=25))

    # WHEN the run is finished
    # THEN total_count is page 1's value
    assert writer.finish().total_count == 25


# --- read_manifest --------------------------------------------------------------------


def test_read_manifest_returns_none_for_a_run_that_never_finished(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a run with pages written but not finished
    writer = _started(mem_fs, run)
    writer.write(_page(status_items))
    # WHEN its manifest is read
    # THEN there is none
    assert read_manifest(mem_fs, BUCKET, run) is None


def test_read_manifest_round_trips_the_finished_manifest(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a finished run
    writer = _started(mem_fs, run)
    writer.write(_page(status_items, page_no=2))
    writer.write(_page(status_items, page_no=1))
    written = writer.finish()
    # WHEN the manifest is read back
    # THEN it equals what finish returned
    assert read_manifest(mem_fs, BUCKET, run) == written


# --- idempotency ----------------------------------------------------------------------


def test_rerunning_the_same_run_with_the_same_input_yields_identical_objects(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a run executed once
    def execute() -> dict[str, bytes]:
        writer = _started(mem_fs, run)
        writer.write(_page(status_items, page_no=1, total_count=40))
        writer.write(_page(status_items[:5], page_no=2, total_count=40))
        writer.finish()
        return {p: mem_fs.cat_file(p) for p in _objects(mem_fs, f"{BUCKET}/")}

    first = execute()
    # WHEN it is executed again with the same input
    second = execute()
    # THEN the same objects exist with the same manifest and page contents
    assert first.keys() == second.keys()
    assert first[manifest_path(BUCKET, run)] == second[manifest_path(BUCKET, run)]
    for path in first:
        if path.endswith(".parquet"):
            assert _read(mem_fs, path).equals(pq.read_table(io.BytesIO(first[path])))


def test_rerun_with_fewer_pages_leaves_no_stale_page_from_the_earlier_attempt(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN an earlier attempt that wrote pages 1..3
    first = _started(mem_fs, run)
    for n in (1, 2, 3):
        first.write(_page(status_items, page_no=n))
    first.finish()
    # WHEN the run is retried and only page 1 is written (budget cap)
    retry = _started(mem_fs, run)
    retry.write(_page(status_items, page_no=1))
    retry.finish()
    # THEN only page 1 and the manifest remain
    assert _objects(mem_fs, f"{BUCKET}/") == sorted(
        [page_path(BUCKET, run, 1), manifest_path(BUCKET, run)]
    )


def test_read_manifest_rejects_an_unknown_version(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a finished run whose manifest was written by a newer format
    writer = _started(mem_fs, run)
    writer.write(_page(status_items))
    writer.finish()
    data = json.loads(mem_fs.cat_file(manifest_path(BUCKET, run)))
    data["version"] = MANIFEST_VERSION + 1
    data["new_field"] = "x"
    mem_fs.pipe_file(manifest_path(BUCKET, run), json.dumps(data).encode("utf-8"))

    # WHEN / THEN reading it fails with a clear version error, not a TypeError
    with pytest.raises(ValueError, match="unsupported manifest version"):
        read_manifest(mem_fs, BUCKET, run)
