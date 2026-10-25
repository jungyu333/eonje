from collections.abc import Callable
from datetime import UTC

import httpx
import pytest
from tenacity import wait_none

from eonje.client.errors import ApiError, TransientError, UnexpectedResponseError
from eonje.client.evcharger import EvChargerClient
from eonje.client.types import CallRecord
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
