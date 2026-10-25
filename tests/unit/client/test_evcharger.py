import json
from collections.abc import Callable
from datetime import UTC

import httpx
import pytest
from tenacity import wait_none

from eonje.client.errors import ApiError, TransientError, UnexpectedResponseError
from eonje.client.evcharger import EvChargerClient
from eonje.client.types import CallRecord, Page
from eonje.config import Settings

Load = Callable[[str], str]
Step = httpx.Response | Exception

SERVICE_KEY = "test-service-key"
BASE_URL = "https://api.test/B552584/EvCharger"


class ScriptedTransport(httpx.BaseTransport):
    """Returns (or raises) the scripted steps in order and records every request."""

    def __init__(self, steps: list[Step]) -> None:
        self._steps = list(steps)
        self.requests: list[httpx.Request] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        step = self._steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


def _settings(max_attempts: int = 3) -> Settings:
    return Settings(
        api_service_key=SERVICE_KEY,  # type: ignore[arg-type]
        api_base_url=BASE_URL,
        page_size=500,
        api_timeout_sec=7.5,
        api_max_attempts=max_attempts,
    )


def _client(
    steps: list[Step], max_attempts: int = 3
) -> tuple[EvChargerClient, ScriptedTransport, list[CallRecord]]:
    transport = ScriptedTransport(steps)
    records: list[CallRecord] = []
    client = EvChargerClient(
        httpx.Client(transport=transport),
        _settings(max_attempts),
        on_call=records.append,
        wait=wait_none(),
    )
    return client, transport, records


def _ok(text: str) -> httpx.Response:
    return httpx.Response(200, text=text)


# --- request building -------------------------------------------------------


def test_request_targets_operation_url_with_common_and_caller_params(
    fixture_text: Load,
) -> None:
    # GIVEN a client and a normal page response
    client, transport, _ = _client([_ok(fixture_text("getChargerStatus_page.json"))])

    # WHEN page 2 of getChargerStatus is fetched with a period filter
    client.fetch_page("getChargerStatus", 2, {"period": "10"})

    # THEN one GET goes to <base>/<operation> with common and caller params
    (req,) = transport.requests
    assert req.method == "GET"
    assert str(req.url.copy_with(params=None)) == f"{BASE_URL}/getChargerStatus"
    assert dict(req.url.params) == {
        "period": "10",
        "serviceKey": SERVICE_KEY,
        "pageNo": "2",
        "numOfRows": "500",
        "dataType": "JSON",
    }


def test_caller_params_cannot_override_paging_or_key(fixture_text: Load) -> None:
    # GIVEN caller params that collide with the client-controlled params
    client, transport, _ = _client([_ok(fixture_text("getChargerStatus_page.json"))])
    params = {"pageNo": "99", "numOfRows": "1", "serviceKey": "other", "dataType": "XML"}

    # WHEN a page is fetched
    client.fetch_page("getChargerStatus", 1, params)

    # THEN the client's values win
    sent = transport.requests[0].url.params
    assert sent["pageNo"] == "1"
    assert sent["numOfRows"] == "500"
    assert sent["serviceKey"] == SERVICE_KEY
    assert sent["dataType"] == "JSON"


def test_configured_timeout_is_applied_to_the_request(fixture_text: Load) -> None:
    # GIVEN a client with api_timeout_sec=7.5
    client, transport, _ = _client([_ok(fixture_text("getChargerStatus_page.json"))])

    # WHEN a page is fetched
    client.fetch_page("getChargerStatus", 1, {})

    # THEN every timeout component is 7.5s
    timeout = transport.requests[0].extensions["timeout"]
    assert set(timeout.values()) == {7.5}


# --- successful page --------------------------------------------------------


def test_normal_response_returns_page_with_items_unchanged(fixture_text: Load) -> None:
    # GIVEN a normal page with duplicate rows, unknown stat codes and blank optionals
    client, _, _ = _client([_ok(fixture_text("getChargerStatus_page.json"))])

    # WHEN it is fetched
    page = client.fetch_page("getChargerStatus", 1, {})

    # THEN the page carries every item as received
    assert page.operation == "getChargerStatus"
    assert page.page_no == 1
    assert page.total_count == 4
    assert len(page.items) == 4
    assert [i["stat"] for i in page.items] == ["3", "3", "7", ""]
    assert page.fetched_at.tzinfo is UTC


def test_successful_call_is_reported_once(fixture_text: Load) -> None:
    # GIVEN a normal page response
    client, _, records = _client([_ok(fixture_text("getChargerStatus_page.json"))])

    # WHEN it is fetched
    page = client.fetch_page("getChargerStatus", 3, {})

    # THEN exactly one call record describes it
    (rec,) = records
    assert rec.operation == "getChargerStatus"
    assert rec.page_no == 3
    assert rec.attempt == 1
    assert rec.http_status == 200
    assert rec.result_code == "00"
    assert rec.total_count == 4
    assert rec.elapsed_ms >= 0
    assert rec.called_at == page.fetched_at


def test_page_past_the_end_returns_empty_items(fixture_text: Load) -> None:
    # GIVEN a normal response with items=""
    client, _, records = _client([_ok(fixture_text("getChargerStatus_empty_page.json"))])

    # WHEN it is fetched
    page = client.fetch_page("getChargerStatus", 3, {})

    # THEN the page is empty and reported as a normal call
    assert page.items == []
    assert page.total_count == 13668
    assert records[0].result_code == "00"


def test_no_data_is_an_empty_page_and_reported_with_its_code(fixture_text: Load) -> None:
    # GIVEN resultCode 03 (NODATA)
    client, transport, records = _client([_ok(fixture_text("getChargerStatus_no_data.json"))])

    # WHEN a page is fetched
    page = client.fetch_page("getChargerStatus", 1, {"period": "1"})

    # THEN an empty page is returned without retry and the code is logged
    assert page.items == []
    assert page.total_count == 0
    assert len(transport.requests) == 1
    assert (records[0].result_code, records[0].total_count) == ("03", 0)


def test_fetching_the_same_page_twice_yields_equal_items(fixture_text: Load) -> None:
    # GIVEN the same response served twice
    text = fixture_text("getChargerStatus_page.json")
    client, _, _ = _client([_ok(text), _ok(text)])

    # WHEN the same page is fetched twice
    first = client.fetch_page("getChargerStatus", 1, {})
    second = client.fetch_page("getChargerStatus", 1, {})

    # THEN the item payloads and counts are equal
    assert first.items == second.items
    assert first.total_count == second.total_count


# --- transient failures and retry ------------------------------------------


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_http_status_is_retried_then_succeeds(status: int, fixture_text: Load) -> None:
    # GIVEN one transient HTTP failure followed by a normal page
    ok = _ok(fixture_text("getChargerStatus_page.json"))
    client, transport, records = _client([httpx.Response(status, text="busy"), ok])

    # WHEN a page is fetched
    page = client.fetch_page("getChargerStatus", 1, {})

    # THEN the page is returned after a second request
    assert page.total_count == 4
    assert len(transport.requests) == 2
    assert [r.attempt for r in records] == [1, 2]
    assert [r.http_status for r in records] == [status, 200]
    assert records[0].result_code is None


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        httpx.RemoteProtocolError("closed"),
    ],
)
def test_transport_error_is_retried_and_reported_without_status(
    error: Exception, fixture_text: Load
) -> None:
    # GIVEN a transport error followed by a normal page
    ok = _ok(fixture_text("getChargerStatus_page.json"))
    client, _, records = _client([error, ok])

    # WHEN a page is fetched
    page = client.fetch_page("getChargerStatus", 1, {})

    # THEN it succeeds on attempt 2 and attempt 1 is logged with no status
    assert page.total_count == 4
    assert [r.attempt for r in records] == [1, 2]
    assert records[0].http_status is None
    assert records[0].result_code is None


def test_exhausted_transient_retries_raise_and_log_every_attempt() -> None:
    # GIVEN a server that keeps returning 503, and max_attempts=3
    client, transport, records = _client([httpx.Response(503)] * 3, max_attempts=3)

    # WHEN a page is fetched
    with pytest.raises(TransientError):
        client.fetch_page("getChargerStatus", 1, {})

    # THEN exactly 3 requests were made and each attempt was logged
    assert len(transport.requests) == 3
    assert [r.attempt for r in records] == [1, 2, 3]
    assert all(r.http_status == 503 for r in records)


def test_exhausted_transport_errors_reraise_the_transport_error() -> None:
    # GIVEN a connection that always fails, and max_attempts=2
    client, transport, records = _client([httpx.ConnectError("x")] * 2, max_attempts=2)

    # WHEN a page is fetched
    with pytest.raises(httpx.ConnectError):
        client.fetch_page("getChargerStatus", 1, {})

    # THEN it stopped at max_attempts and logged each attempt
    assert len(transport.requests) == 2
    assert len(records) == 2


def test_single_attempt_setting_disables_retry() -> None:
    # GIVEN max_attempts=1 and a transient failure
    client, transport, records = _client([httpx.Response(500)], max_attempts=1)

    # WHEN a page is fetched
    with pytest.raises(TransientError):
        client.fetch_page("getChargerStatus", 1, {})

    # THEN only one request was made
    assert len(transport.requests) == 1
    assert len(records) == 1


# --- non-retryable failures -------------------------------------------------


def test_api_error_is_not_retried_and_logged_with_result_code(fixture_text: Load) -> None:
    # GIVEN resultCode 22 (request limit exceeded)
    client, transport, records = _client([_ok(fixture_text("getChargerStatus_api_error.json"))])

    # WHEN a page is fetched
    with pytest.raises(ApiError) as exc:
        client.fetch_page("getChargerStatus", 1, {})

    # THEN it fails after one request, logged with the API result code
    assert exc.value.result_code == "22"
    assert len(transport.requests) == 1
    (rec,) = records
    assert rec.http_status == 200
    assert rec.result_code == "22"
    assert rec.total_count is None


def test_gateway_xml_error_is_not_retried_and_still_logged(fixture_text: Load) -> None:
    # GIVEN the data.go.kr gateway XML error with HTTP 200
    client, transport, records = _client([_ok(fixture_text("gateway_error.xml"))])

    # WHEN a page is fetched
    with pytest.raises(UnexpectedResponseError):
        client.fetch_page("getChargerStatus", 1, {})

    # THEN it fails after one request and the attempt is logged
    assert len(transport.requests) == 1
    (rec,) = records
    assert rec.http_status == 200


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_client_error_status_is_not_retried(status: int, fixture_text: Load) -> None:
    # GIVEN a 4xx response (not 429) with a gateway XML body
    resp = httpx.Response(status, text=fixture_text("gateway_error.xml"))
    client, transport, records = _client([resp])

    # WHEN a page is fetched
    with pytest.raises(UnexpectedResponseError):
        client.fetch_page("getChargerStatus", 1, {})

    # THEN it is not retried and the status is logged
    assert len(transport.requests) == 1
    assert records[0].http_status == status


def test_client_works_without_on_call_callback(fixture_text: Load) -> None:
    # GIVEN a client constructed without on_call
    transport = ScriptedTransport([_ok(fixture_text("getChargerStatus_page.json"))])
    client = EvChargerClient(httpx.Client(transport=transport), _settings(), wait=wait_none())

    # WHEN a page is fetched
    page = client.fetch_page("getChargerStatus", 1, {})

    # THEN it succeeds
    assert page.total_count == 4


# --- pagination (iter_pages) ------------------------------------------------


def _with_total(text: str, total: int) -> str:
    """Return a fixture body with totalCount replaced (page size in tests is 500)."""
    body = json.loads(text)
    body["totalCount"] = total
    return json.dumps(body, ensure_ascii=False)


def _sent_page_nos(transport: ScriptedTransport) -> list[str]:
    return [r.url.params["pageNo"] for r in transport.requests]


@pytest.mark.parametrize(
    ("total", "expected"),
    [(0, 1), (1, 1), (499, 1), (500, 1), (501, 2), (1000, 2), (1001, 3)],
)
def test_requested_page_count_is_ceiling_of_total_over_page_size_with_minimum_one(
    total: int, expected: int, fixture_text: Load
) -> None:
    # GIVEN a first page reporting `total` rows with a page size of 500
    text = _with_total(fixture_text("getChargerStatus_page.json"), total)
    client, transport, _ = _client([_ok(text)] * expected)

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {}))

    # THEN ceil(total / 500) pages are requested, never fewer than 1
    expected_page_nos = list(range(1, expected + 1))
    assert [p.page_no for p in pages] == expected_page_nos
    assert _sent_page_nos(transport) == [str(n) for n in expected_page_nos]


def test_single_page_result_makes_exactly_one_request(fixture_text: Load) -> None:
    # GIVEN a first page whose totalCount fits in one page
    client, transport, _ = _client([_ok(fixture_text("getChargerStatus_page.json"))])

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {"period": "10"}))

    # THEN only page 1 is requested and returned
    assert [p.page_no for p in pages] == [1]
    assert _sent_page_nos(transport) == ["1"]


def test_all_pages_derived_from_first_total_count_are_fetched_in_order(
    fixture_text: Load,
) -> None:
    # GIVEN totalCount=1001 with page size 500 (3 pages)
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1001)
    client, transport, _ = _client([_ok(text)] * 3)

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {"period": "10"}))

    # THEN pages 1..3 are requested in order with the caller params on each
    assert [p.page_no for p in pages] == [1, 2, 3]
    assert _sent_page_nos(transport) == ["1", "2", "3"]
    assert all(r.url.params["period"] == "10" for r in transport.requests)


def test_exact_multiple_of_page_size_does_not_request_an_extra_page(
    fixture_text: Load,
) -> None:
    # GIVEN totalCount=1000 with page size 500
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1000)
    client, transport, _ = _client([_ok(text)] * 2)

    # WHEN all pages are iterated
    list(client.iter_pages("getChargerStatus", {}))

    # THEN exactly 2 requests are made
    assert _sent_page_nos(transport) == ["1", "2"]


def test_no_data_first_page_yields_one_empty_page_and_stops(fixture_text: Load) -> None:
    # GIVEN resultCode 03 (NODATA) on page 1
    client, transport, records = _client([_ok(fixture_text("getChargerStatus_no_data.json"))])

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {"period": "10"}))

    # THEN one empty page is yielded after a single request
    assert [(p.page_no, p.items) for p in pages] == [(1, [])]
    assert len(transport.requests) == 1
    assert len(records) == 1


def test_max_pages_caps_the_number_of_requests(fixture_text: Load) -> None:
    # GIVEN 3 pages available but a cap of 2
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1001)
    client, transport, records = _client([_ok(text)] * 3)

    # WHEN pages are iterated with max_pages=2
    pages = list(client.iter_pages("getChargerStatus", {}, max_pages=2))

    # THEN only pages 1 and 2 are requested and logged
    assert [p.page_no for p in pages] == [1, 2]
    assert _sent_page_nos(transport) == ["1", "2"]
    assert len(records) == 2


def test_max_pages_above_available_pages_fetches_only_available(
    fixture_text: Load,
) -> None:
    # GIVEN 2 pages available and a cap of 10
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1000)
    client, transport, _ = _client([_ok(text)] * 2)

    # WHEN pages are iterated with max_pages=10
    pages = list(client.iter_pages("getChargerStatus", {}, max_pages=10))

    # THEN only the 2 available pages are requested
    assert [p.page_no for p in pages] == [1, 2]
    assert len(transport.requests) == 2


def test_max_pages_of_one_fetches_only_the_first_page(fixture_text: Load) -> None:
    # GIVEN 3 pages available and a cap of 1
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1001)
    client, transport, _ = _client([_ok(text)])

    # WHEN pages are iterated with max_pages=1
    pages = list(client.iter_pages("getChargerStatus", {}, max_pages=1))

    # THEN only page 1 is requested
    assert [p.page_no for p in pages] == [1]
    assert _sent_page_nos(transport) == ["1"]


@pytest.mark.parametrize("max_pages", [0, -1])
def test_non_positive_max_pages_makes_no_request(max_pages: int) -> None:
    # GIVEN a budget cap of zero (or below)
    client, transport, records = _client([])

    # WHEN pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {}, max_pages=max_pages))

    # THEN nothing is fetched or logged
    assert pages == []
    assert transport.requests == []
    assert records == []


def test_pages_are_fetched_lazily_as_the_caller_consumes_them(fixture_text: Load) -> None:
    # GIVEN 3 pages available
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1001)
    client, transport, _ = _client([_ok(text)] * 3)

    # WHEN the caller takes only the first page
    first = next(client.iter_pages("getChargerStatus", {}))

    # THEN no further page has been requested
    assert first.page_no == 1
    assert len(transport.requests) == 1


def test_total_count_is_taken_from_the_first_page_only(fixture_text: Load) -> None:
    # GIVEN page 1 reports 1001 rows (3 pages) and later pages report 4 rows
    base = fixture_text("getChargerStatus_page.json")
    steps: list[Step] = [
        _ok(_with_total(base, 1001)),
        _ok(_with_total(base, 4)),
        _ok(_with_total(base, 4)),
    ]
    client, transport, _ = _client(steps)

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {}))

    # THEN the page count derived from page 1 is kept
    assert [p.page_no for p in pages] == [1, 2, 3]
    assert len(transport.requests) == 3


def test_empty_page_mid_iteration_is_yielded_and_iteration_continues(
    fixture_text: Load,
) -> None:
    # GIVEN 3 pages where page 2 comes back with items=""
    first = _with_total(fixture_text("getChargerStatus_page.json"), 1001)
    empty = fixture_text("getChargerStatus_empty_page.json")
    client, _, _ = _client([_ok(first), _ok(empty), _ok(first)])

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {}))

    # THEN the empty page is yielded in place and page 3 is still fetched
    assert [(p.page_no, len(p.items)) for p in pages] == [(1, 4), (2, 0), (3, 4)]


def test_items_are_passed_through_unchanged_across_pages(fixture_text: Load) -> None:
    # GIVEN 2 pages with duplicate rows, unknown stat codes and blank optionals
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1000)
    client, _, _ = _client([_ok(text)] * 2)

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {}))

    # THEN every item is kept as received, including duplicates and codes "7" and ""
    expected = json.loads(text)["items"]["item"]
    assert [p.items for p in pages] == [expected, expected]
    assert [i["stat"] for i in pages[0].items] == ["3", "3", "7", ""]
    assert pages[0].items[2]["lastTedt"] == ""
    assert pages[0].items[2]["maker"] == "XX"


def test_transient_failure_on_a_later_page_is_retried_within_iteration(
    fixture_text: Load,
) -> None:
    # GIVEN 2 pages where page 2 first returns 503
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1000)
    client, transport, records = _client([_ok(text), httpx.Response(503), _ok(text)])

    # WHEN all pages are iterated
    pages = list(client.iter_pages("getChargerStatus", {}))

    # THEN page 2 is retried and both pages are returned
    assert [p.page_no for p in pages] == [1, 2]
    assert _sent_page_nos(transport) == ["1", "2", "2"]
    assert [(r.page_no, r.attempt) for r in records] == [(1, 1), (2, 1), (2, 2)]


def test_api_error_on_a_later_page_propagates_after_earlier_pages_were_yielded(
    fixture_text: Load,
) -> None:
    # GIVEN 3 pages where page 2 returns resultCode 22
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1001)
    error = fixture_text("getChargerStatus_api_error.json")
    client, transport, _ = _client([_ok(text), _ok(error)])
    received: list[int] = []

    # WHEN pages are iterated
    with pytest.raises(ApiError):
        for page in client.iter_pages("getChargerStatus", {}):
            received.append(page.page_no)

    # THEN page 1 was delivered and page 3 was never requested
    assert received == [1]
    assert _sent_page_nos(transport) == ["1", "2"]


def test_iterating_twice_over_the_same_responses_yields_the_same_pages(
    fixture_text: Load,
) -> None:
    # GIVEN the same 2-page responses served for two runs
    text = _with_total(fixture_text("getChargerStatus_page.json"), 1000)
    client, _, _ = _client([_ok(text)] * 4)

    # WHEN pages are iterated twice
    first = list(client.iter_pages("getChargerStatus", {}))
    second = list(client.iter_pages("getChargerStatus", {}))

    # THEN page numbers, totals and items match
    def key(pages: list[Page]) -> list[tuple[int, int, list[dict[str, str]]]]:
        return [(p.page_no, p.total_count, p.items) for p in pages]

    assert key(first) == key(second)
