import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest
from tenacity import wait_none

from eonje.client.errors import ApiError, TransientError, UnexpectedResponseError
from eonje.client.holidays import Holiday, fetch_holidays, parse_holidays
from eonje.config import Settings

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "holidays"

Step = httpx.Response | Exception

SERVICE_KEY = "test-service-key"
BASE_URL = "https://api.test/B090041/openapi/service/SpcdeInfoService"


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
        holiday_api_base_url=BASE_URL,
        api_max_attempts=max_attempts,
    )


def _fetch(steps: list[Step], max_attempts: int = 3) -> tuple[list[Holiday], ScriptedTransport]:
    transport = ScriptedTransport(steps)
    result = fetch_holidays(
        httpx.Client(transport=transport), _settings(max_attempts), 2026, wait=wait_none()
    )
    return result, transport


def _item(locdate: int | str, name: str, *, seq: int = 1, is_holiday: str = "Y") -> dict[str, Any]:
    return {
        "dateKind": "01",
        "dateName": name,
        "isHoliday": is_holiday,
        "locdate": locdate,
        "seq": seq,
    }


def _body_text(
    items: list[dict[str, Any]] | dict[str, Any] | str,
    total: int,
    *,
    code: str = "00",
    message: str = "NORMAL SERVICE.",
) -> str:
    wrapped: Any = items if isinstance(items, str) else {"item": items}
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": code, "resultMsg": message},
                "body": {"items": wrapped, "numOfRows": 100, "pageNo": 1, "totalCount": total},
            }
        },
        ensure_ascii=False,
    )


def _ok(text: str) -> httpx.Response:
    return httpx.Response(200, text=text)


NEW_YEAR = _body_text([_item(20260101, "1월1일")], 1)


# --- real response ------------------------------------------------------------


def test_real_2026_response_yields_22_holidays_in_date_order() -> None:
    # GIVEN the real 2026 getRestDeInfo response
    text = (FIXTURES / "rest_de_info_2026.json").read_text(encoding="utf-8")

    # WHEN it is parsed
    holidays = parse_holidays(text)

    # THEN there are 22 holidays sorted by date, starting with New Year's Day
    assert len(holidays) == 22
    assert [h.day for h in holidays] == sorted(h.day for h in holidays)
    assert holidays[0] == Holiday(date(2026, 1, 1), "1월1일")


def test_real_2026_response_includes_substitute_holiday_for_samiljeol() -> None:
    # GIVEN the real 2026 getRestDeInfo response
    text = (FIXTURES / "rest_de_info_2026.json").read_text(encoding="utf-8")

    # WHEN it is parsed
    holidays = parse_holidays(text)

    # THEN the substitute holiday on March 2 is included
    assert Holiday(date(2026, 3, 2), "대체공휴일(삼일절)") in holidays


# --- parsing --------------------------------------------------------------------


@pytest.mark.parametrize("locdate", [20260505, "20260505"], ids=["number", "string"])
def test_locdate_as_number_or_string_reads_as_the_same_date(locdate: int | str) -> None:
    # GIVEN a single holiday whose locdate is a number or a string
    text = _body_text([_item(locdate, "어린이날")], 1)

    # WHEN it is parsed
    holidays = parse_holidays(text)

    # THEN it is read as May 5
    assert holidays == [Holiday(date(2026, 5, 5), "어린이날")]


def test_items_not_marked_as_holiday_are_dropped() -> None:
    # GIVEN one holiday and one non-holiday special day
    text = _body_text(
        [_item(20260101, "1월1일"), _item(20260717, "제헌절", is_holiday="N")],
        2,
    )

    # WHEN it is parsed
    holidays = parse_holidays(text)

    # THEN only the holiday remains
    assert holidays == [Holiday(date(2026, 1, 1), "1월1일")]


def test_single_item_sent_as_object_reads_as_one_holiday() -> None:
    # GIVEN a one-item response where item is an object, not a list
    text = _body_text(_item(20260101, "1월1일"), 1)

    # WHEN it is parsed
    holidays = parse_holidays(text)

    # THEN one holiday is returned
    assert holidays == [Holiday(date(2026, 1, 1), "1월1일")]


def test_zero_items_with_empty_string_items_is_an_empty_list() -> None:
    # GIVEN a response for a year not announced yet: items is "" and totalCount is 0
    text = _body_text("", 0)

    # WHEN it is parsed
    holidays = parse_holidays(text)

    # THEN no holidays are returned
    assert holidays == []


def test_two_items_on_the_same_date_are_both_returned_in_seq_order() -> None:
    # GIVEN two holidays on the same date, listed with seq 2 before seq 1
    text = _body_text(
        [
            _item(20260603, "전국동시지방선거", seq=2),
            _item(20260603, "임시공휴일", seq=1),
        ],
        2,
    )

    # WHEN it is parsed
    holidays = parse_holidays(text)

    # THEN both are returned, seq 1 first
    assert holidays == [
        Holiday(date(2026, 6, 3), "임시공휴일"),
        Holiday(date(2026, 6, 3), "전국동시지방선거"),
    ]


def test_non_normal_result_code_raises_api_error_with_the_code() -> None:
    # GIVEN a response with resultCode 30 (unregistered service key)
    text = _body_text("", 0, code="30", message="SERVICE_KEY_IS_NOT_REGISTERED_ERROR.")

    # WHEN it is parsed
    with pytest.raises(ApiError) as exc_info:
        parse_holidays(text)

    # THEN the error carries the result code
    assert exc_info.value.result_code == "30"


@pytest.mark.parametrize(
    "text",
    [
        "<OpenAPI_ServiceResponse><cmmMsgHeader>SERVICE ERROR</cmmMsgHeader>",
        json.dumps({"response": {"body": {"items": "", "totalCount": 0}}}),
        json.dumps({"response": {"header": {"resultCode": "00", "resultMsg": "NORMAL SERVICE."}}}),
        _body_text([_item(20260101, "1월1일")], 2),
        _body_text([_item("2026-01-01", "1월1일")], 1),
        _body_text([_item(20261301, "없는날")], 1),
    ],
    ids=[
        "not-json",
        "missing-header",
        "missing-body",
        "total-count-above-received",
        "locdate-with-dashes",
        "locdate-invalid-month",
    ],
)
def test_malformed_response_raises_unexpected_response_error(text: str) -> None:
    # GIVEN a malformed response
    # WHEN it is parsed
    # THEN UnexpectedResponseError is raised
    with pytest.raises(UnexpectedResponseError):
        parse_holidays(text)


# --- request building -----------------------------------------------------------


def test_request_targets_get_rest_de_info_under_configured_base_url() -> None:
    # GIVEN a configured holiday base URL and a normal response
    # WHEN the 2026 holidays are fetched
    _, transport = _fetch([_ok(NEW_YEAR)])

    # THEN one GET goes to <holiday base>/getRestDeInfo
    (req,) = transport.requests
    assert req.method == "GET"
    assert str(req.url.copy_with(params=None)) == f"{BASE_URL}/getRestDeInfo"


def test_request_query_has_year_json_type_row_count_and_service_key() -> None:
    # GIVEN a normal response
    # WHEN the 2026 holidays are fetched
    _, transport = _fetch([_ok(NEW_YEAR)])

    # THEN the query carries solYear, _type=json, numOfRows and serviceKey
    params = transport.requests[0].url.params
    assert params["solYear"] == "2026"
    assert params["_type"] == "json"
    assert int(params["numOfRows"]) > 0
    assert params["serviceKey"] == SERVICE_KEY


# --- retry ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "failure",
    [
        httpx.Response(500, text="Internal Server Error"),
        httpx.Response(429, text="Too Many Requests"),
        httpx.ConnectError("connection refused"),
    ],
    ids=["http-500", "http-429", "transport-error"],
)
def test_transient_failure_is_retried_and_success_is_returned(failure: Step) -> None:
    # GIVEN one transient failure followed by a normal response
    # WHEN the holidays are fetched
    holidays, transport = _fetch([failure, _ok(NEW_YEAR)])

    # THEN the second attempt's result is returned
    assert holidays == [Holiday(date(2026, 1, 1), "1월1일")]
    assert len(transport.requests) == 2


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (lambda: httpx.Response(500, text="Internal Server Error"), TransientError),
        (lambda: httpx.Response(429, text="Too Many Requests"), TransientError),
        (lambda: httpx.ConnectError("connection refused"), httpx.ConnectError),
    ],
    ids=["http-500", "http-429", "transport-error"],
)
def test_failing_every_attempt_raises_after_api_max_attempts(
    failure: Any, expected: type[Exception]
) -> None:
    # GIVEN api_max_attempts=3 and three transient failures in a row
    transport = ScriptedTransport([failure() for _ in range(3)])

    # WHEN the holidays are fetched
    with pytest.raises(expected):
        fetch_holidays(httpx.Client(transport=transport), _settings(3), 2026, wait=wait_none())

    # THEN exactly api_max_attempts requests were made
    assert len(transport.requests) == 3


def test_api_error_is_not_retried() -> None:
    # GIVEN an API error response followed by a normal one
    error = _ok(_body_text("", 0, code="30", message="SERVICE_KEY_IS_NOT_REGISTERED_ERROR."))
    transport = ScriptedTransport([error, _ok(NEW_YEAR)])

    # WHEN the holidays are fetched
    with pytest.raises(ApiError):
        fetch_holidays(httpx.Client(transport=transport), _settings(3), 2026, wait=wait_none())

    # THEN only one request was made
    assert len(transport.requests) == 1
