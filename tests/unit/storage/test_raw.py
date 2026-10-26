import io
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from eonje.client.response import parse_response
from eonje.client.types import Page
from eonje.storage.raw import META_FIELDS, page_table, write_page
from eonje.storage.raw_paths import RawRun, page_path

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"
BUCKET = "eonje-raw"
KST = timezone(timedelta(hours=9))
COLLECTED_AT = datetime(2026, 10, 26, 10, 10, 0, tzinfo=UTC)
FETCHED_AT = datetime(2026, 10, 26, 10, 10, 3, 250000, tzinfo=UTC)
META_NAMES = [f.name for f in META_FIELDS]


def _fixture_items(name: str) -> list[dict[str, Any]]:
    return parse_response((FIXTURES / name).read_text(encoding="utf-8")).items


def _page(
    items: list[dict[str, Any]],
    page_no: int = 1,
    operation: str = "getChargerStatus",
    fetched_at: datetime = FETCHED_AT,
) -> Page:
    return Page(
        operation=operation,
        page_no=page_no,
        total_count=len(items),
        items=items,
        fetched_at=fetched_at,
    )


def _status_item(**overrides: Any) -> dict[str, Any]:
    item = dict(_fixture_items("real_status_page.json")[0])
    item.update(overrides)
    return item


@pytest.fixture
def status_items() -> list[dict[str, Any]]:
    return _fixture_items("real_status_page.json")


def _read(mem_fs: MemoryFileSystem, path: str) -> pa.Table:
    with mem_fs.open(path, "rb") as f:
        return pq.read_table(io.BytesIO(f.read()))


# --- page_table: response columns ----------------------------------------------------


def test_page_table_keeps_every_response_field_as_a_string_column(
    status_items: list[dict[str, Any]],
) -> None:
    # GIVEN a real getChargerStatus page
    page = _page(status_items)
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN every response field is a string column, followed by the metadata columns
    response_fields = list(status_items[0])
    assert table.column_names == response_fields + META_NAMES
    for name in response_fields:
        assert table.schema.field(name).type == pa.string()


def test_page_table_preserves_values_exactly_as_received(
    status_items: list[dict[str, Any]],
) -> None:
    # GIVEN a real page whose values include leading zeros and timestamps as strings
    page = _page(status_items)
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN the rows round-trip to the received items unchanged
    rows = table.drop_columns(META_NAMES).to_pylist()
    assert rows == status_items
    assert rows[0]["chgerId"] == "01"


def test_page_table_keeps_all_columns_of_a_real_charger_info_page() -> None:
    # GIVEN a real getChargerInfo page with many optional fields
    items = _fixture_items("real_info_page.json")
    page = _page(items, operation="getChargerInfo")
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN no field is dropped and the row count matches
    expected = list(dict.fromkeys(k for item in items for k in item))
    assert table.column_names == expected + META_NAMES
    assert table.num_rows == len(items)


def test_page_table_keeps_an_unknown_response_field() -> None:
    # GIVEN an item carrying a field the API did not document
    page = _page([_status_item(newField="x")])
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN the unknown field is stored as a string column
    assert table.column("newField").to_pylist() == ["x"]
    assert table.schema.field("newField").type == pa.string()


def test_page_table_builds_the_union_of_fields_in_first_seen_order() -> None:
    # GIVEN items with differing key sets
    items = [{"statId": "A", "stat": "2"}, {"statId": "B", "extra": "e"}]
    # WHEN they are converted to a table
    table = page_table(_page(items), COLLECTED_AT)
    # THEN columns are the union in first-seen order and absent fields are null
    assert table.column_names == ["statId", "stat", "extra", *META_NAMES]
    assert table.column("stat").to_pylist() == ["2", None]
    assert table.column("extra").to_pylist() == [None, "e"]


@pytest.mark.parametrize("code", ["7", "", "99"])
def test_page_table_keeps_an_unknown_status_code(code: str) -> None:
    # GIVEN an item whose stat is not a documented code
    page = _page([_status_item(stat=code)])
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN the row is kept with the code unchanged
    assert table.column("stat").to_pylist() == [code]


def test_page_table_keeps_empty_optional_fields_as_empty_strings_not_null() -> None:
    # GIVEN an item with empty lastTedt and nowTsdt
    page = _page([_status_item(lastTedt="", nowTsdt="")])
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN the empty strings are stored as received (not converted to null)
    assert table.column("lastTedt").to_pylist() == [""]
    assert table.column("nowTsdt").to_pylist() == [""]


def test_page_table_stores_a_none_value_as_null() -> None:
    # GIVEN an item with an explicit null value
    page = _page([_status_item(nowTsdt=None)])
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN the value is null
    assert table.column("nowTsdt").to_pylist() == [None]


def test_page_table_keeps_duplicate_observations() -> None:
    # GIVEN the same (statId, chgerId, statUpdDt) twice in one page
    item = _status_item()
    page = _page([item, dict(item)])
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN both rows are stored (dedupe happens in transform/, not in raw)
    assert table.num_rows == 2


def test_page_table_keeps_a_heartbeat_observation_unchanged() -> None:
    # GIVEN two observations of one charger with the same stat and different statUpdDt
    first = _status_item(stat="2", statUpdDt="20261026100000")
    second = _status_item(stat="2", statUpdDt="20261026100500")
    # WHEN they are converted to a table
    table = page_table(_page([first, second]), COLLECTED_AT)
    # THEN both raw rows are kept as received
    assert table.column("statUpdDt").to_pylist() == ["20261026100000", "20261026100500"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (13668, "13668"),
        (37.5665, "37.5665"),
        (0, "0"),
        (True, "true"),
        (False, "false"),
        ({"k": "충전"}, '{"k": "충전"}'),
        (["a", 1], '["a", 1]'),
    ],
)
def test_page_table_converts_non_string_values_to_strings(value: Any, expected: str) -> None:
    # GIVEN an item whose value is not a string in the parsed JSON
    page = _page([{"statId": "A", "v": value}])
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN the value is stored as its JSON text (nested values as non-ASCII-escaped JSON)
    assert table.schema.field("v").type == pa.string()
    assert table.column("v").to_pylist() == [expected]


def test_page_table_rejects_a_naive_collected_at() -> None:
    # GIVEN a collected_at without a timezone (e.g. KST passed as naive local time)
    page = _page([_status_item()])
    naive = datetime(2026, 10, 26, 19, 10, 0)

    # WHEN / THEN it is rejected instead of being stored 9 hours off
    with pytest.raises(ValueError, match="collected_at"):
        page_table(page, naive)


def test_page_table_rejects_a_naive_page_fetched_at() -> None:
    # GIVEN a page whose fetched_at has no timezone
    page = _page([_status_item()], fetched_at=datetime(2026, 10, 26, 10, 10, 3))

    # WHEN / THEN it is rejected
    with pytest.raises(ValueError, match="fetched_at"):
        page_table(page, COLLECTED_AT)


def test_write_page_leaves_no_object_for_a_naive_timestamp(
    mem_fs: MemoryFileSystem, run: RawRun
) -> None:
    # GIVEN a naive collected_at
    page = _page([_status_item()])

    # WHEN writing is attempted
    with pytest.raises(ValueError):
        write_page(mem_fs, "eonje-raw", run, page, datetime(2026, 10, 26, 19, 10, 0))

    # THEN nothing is stored
    assert not mem_fs.find("eonje-raw")


# --- page_table: metadata columns ----------------------------------------------------


def test_page_table_appends_metadata_columns_with_fixed_types() -> None:
    # GIVEN any page
    page = _page([_status_item()], page_no=7)
    # WHEN it is converted to a table
    table = page_table(page, COLLECTED_AT)
    # THEN the metadata columns have the declared types and are non-nullable
    for field in META_FIELDS:
        assert table.schema.field(field.name) == field
    assert table.schema.field("collected_at").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("page_no").type == pa.int32()


def test_page_table_stamps_every_row_with_collection_and_page_metadata(
    status_items: list[dict[str, Any]],
) -> None:
    # GIVEN a multi-row page with its own fetch time and page number
    page = _page(status_items, page_no=3)
    # WHEN it is converted with the run's collection start time
    table = page_table(page, COLLECTED_AT)
    # THEN every row carries the same collected_at, page_fetched_at, and page_no
    n = len(status_items)
    assert table.column("collected_at").to_pylist() == [COLLECTED_AT] * n
    assert table.column("page_fetched_at").to_pylist() == [FETCHED_AT] * n
    assert table.column("page_no").to_pylist() == [3] * n


def test_page_table_stores_a_kst_collected_at_as_the_same_utc_instant() -> None:
    # GIVEN a collection time expressed in Asia/Seoul
    collected_kst = datetime(2026, 10, 26, 19, 10, 0, tzinfo=KST)
    # WHEN a page is converted
    table = page_table(_page([_status_item()]), collected_kst)
    # THEN the stored instant is unchanged (19:10 KST == 10:10 UTC)
    assert table.column("collected_at").to_pylist() == [COLLECTED_AT]


@pytest.mark.parametrize("name", META_NAMES)
def test_page_table_rejects_a_response_field_that_collides_with_metadata(name: str) -> None:
    # GIVEN an item with a field named like a metadata column
    page = _page([_status_item(**{name: "x"})])
    # WHEN it is converted
    # THEN it raises instead of silently overwriting received data
    with pytest.raises(ValueError, match=name):
        page_table(page, COLLECTED_AT)


# --- page_table: empty input and idempotency -----------------------------------------


def test_page_table_of_an_empty_page_has_zero_rows_and_only_metadata_columns() -> None:
    # GIVEN a page parsed from an empty-items response
    items = _fixture_items("getChargerStatus_empty_page.json")
    page = _page(items)
    # WHEN it is converted
    table = page_table(page, COLLECTED_AT)
    # THEN the table is empty but carries the metadata schema
    assert items == []
    assert table.num_rows == 0
    assert table.column_names == META_NAMES
    assert table.schema == pa.schema(list(META_FIELDS))


def test_page_table_is_deterministic_for_the_same_input(
    status_items: list[dict[str, Any]],
) -> None:
    # GIVEN the same page and collection time
    page = _page(status_items)
    # WHEN it is converted twice
    first = page_table(page, COLLECTED_AT)
    second = page_table(page, COLLECTED_AT)
    # THEN the tables are equal including schema
    assert first.equals(second, check_metadata=True)


def test_page_table_does_not_mutate_the_page_items(
    status_items: list[dict[str, Any]],
) -> None:
    # GIVEN a page
    snapshot = [dict(item) for item in status_items]
    page = _page(status_items)
    # WHEN it is converted
    page_table(page, COLLECTED_AT)
    # THEN the input items are unchanged
    assert page.items == snapshot


# --- write_page ----------------------------------------------------------------------


def test_write_page_writes_to_the_run_page_path_and_returns_it(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN page 2 of a status run
    page = _page(status_items, page_no=2)
    # WHEN it is written
    path = write_page(mem_fs, BUCKET, run, page, COLLECTED_AT)
    # THEN the returned path is the layout path and the object exists there
    assert path == page_path(BUCKET, run, 2)
    assert path == "eonje-raw/charger_status/dt=2026-10-26/run=20261026T101000/page=002.parquet"
    assert mem_fs.exists(path)


def test_write_page_round_trips_the_page_table(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a real page
    page = _page(status_items)
    # WHEN it is written and read back
    path = write_page(mem_fs, BUCKET, run, page, COLLECTED_AT)
    stored = _read(mem_fs, path)
    # THEN the stored table equals page_table's output
    assert stored.equals(page_table(page, COLLECTED_AT))


def test_write_page_compresses_with_zstd(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a written page
    path = write_page(mem_fs, BUCKET, run, _page(status_items), COLLECTED_AT)
    # WHEN its Parquet metadata is inspected
    with mem_fs.open(path, "rb") as f:
        meta = pq.ParquetFile(io.BytesIO(f.read())).metadata
    # THEN column chunks use zstd
    assert meta.row_group(0).column(0).compression == "ZSTD"


def test_write_page_writes_an_empty_page_as_a_readable_zero_row_file(
    mem_fs: MemoryFileSystem, run: RawRun
) -> None:
    # GIVEN an empty page
    page = _page([])
    # WHEN it is written
    path = write_page(mem_fs, BUCKET, run, page, COLLECTED_AT)
    # THEN a readable zero-row file with the metadata schema exists
    stored = _read(mem_fs, path)
    assert stored.num_rows == 0
    assert stored.column_names == META_NAMES


def test_write_page_rerun_overwrites_the_same_object(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a page already written by an earlier attempt of the same run
    write_page(mem_fs, BUCKET, run, _page(status_items[:1]), COLLECTED_AT)
    # WHEN the run is retried and the page is written again with different content
    path = write_page(mem_fs, BUCKET, run, _page(status_items), COLLECTED_AT)
    # THEN there is still exactly one object and it holds the latest content
    assert [p.lstrip("/") for p in mem_fs.find(f"{BUCKET}/")] == [path]
    assert _read(mem_fs, path).num_rows == len(status_items)


def test_write_page_twice_with_the_same_input_produces_identical_content(
    mem_fs: MemoryFileSystem, run: RawRun, status_items: list[dict[str, Any]]
) -> None:
    # GIVEN a page
    page = _page(status_items)
    # WHEN it is written twice
    path = write_page(mem_fs, BUCKET, run, page, COLLECTED_AT)
    first = _read(mem_fs, path)
    write_page(mem_fs, BUCKET, run, page, COLLECTED_AT)
    second = _read(mem_fs, path)
    # THEN the stored tables are equal
    assert first.equals(second)


def test_write_page_rejects_an_out_of_range_page_without_writing(
    mem_fs: MemoryFileSystem, run: RawRun
) -> None:
    # GIVEN a page number beyond the path layout's range
    page = _page([_status_item()], page_no=1000)
    # WHEN it is written
    # THEN it raises and nothing is stored
    with pytest.raises(ValueError, match="page_no"):
        write_page(mem_fs, BUCKET, run, page, COLLECTED_AT)
    assert mem_fs.find(f"{BUCKET}/") == []


def test_write_page_leaves_no_object_when_the_page_is_rejected(
    mem_fs: MemoryFileSystem, run: RawRun
) -> None:
    # GIVEN a page whose field collides with a metadata column
    page = _page([_status_item(page_no="1")])
    # WHEN it is written
    # THEN it raises and no (empty or partial) object is left at the page path
    with pytest.raises(ValueError, match="page_no"):
        write_page(mem_fs, BUCKET, run, page, COLLECTED_AT)
    assert not mem_fs.exists(page_path(BUCKET, run, 1))
