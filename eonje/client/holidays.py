"""한국천문연구원 특일 정보 API 클라이언트: 한 해의 공휴일 목록 (dim_date용).

EvCharger와 다른 API다 (주소, 응답 모양, 한도가 다르다). 호출을 api_call_log에 남기지 않는다.
그 테이블의 행 수가 EvCharger 하루 한도 계산에 쓰여서, 섞이면 예산이 틀어진다.
어떤 날이 공휴일인지는 응답의 isHoliday만 따른다 (대체공휴일·임시공휴일·선거일 포함).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential
from tenacity.wait import wait_base

from eonje.client.errors import ApiError, TransientError, UnexpectedResponseError
from eonje.config import Settings

OPERATION = "getRestDeInfo"  # 공휴일 정보
_NORMAL = "00"
# 한 해의 공휴일은 20건 안팎이라 한 페이지로 다 받는다. 넘치면 parse_holidays가 실패시킨다
_MAX_ROWS = 100


@dataclass(frozen=True)
class Holiday:
    day: date
    name: str


def fetch_holidays(
    http: httpx.Client, settings: Settings, year: int, *, wait: wait_base | None = None
) -> list[Holiday]:
    """year년의 공휴일을 날짜순으로 돌려준다. 아직 발표되지 않은 해는 빈 목록이다.

    같은 날짜에 공휴일이 둘이면 항목도 둘이다 (응답의 seq 순서).
    """
    retrying = Retrying(
        stop=stop_after_attempt(settings.api_max_attempts),
        wait=wait or wait_exponential(multiplier=1, max=30),
        retry=retry_if_exception_type((httpx.TransportError, TransientError)),
        reraise=True,
    )
    for attempt in retrying:
        with attempt:
            return _call_once(http, settings, year)
    raise AssertionError("unreachable: Retrying either returns or reraises")


def _call_once(http: httpx.Client, settings: Settings, year: int) -> list[Holiday]:
    resp = http.get(
        f"{settings.holiday_api_base_url}/{OPERATION}",
        params={
            "serviceKey": settings.api_service_key.get_secret_value(),
            "solYear": str(year),
            "numOfRows": str(_MAX_ROWS),
            "pageNo": "1",
            "_type": "json",
        },
        timeout=settings.api_timeout_sec,
    )
    if resp.status_code == 429 or resp.status_code >= 500:
        raise TransientError(f"{OPERATION} {year}: http {resp.status_code}")
    return parse_holidays(resp.text)


def parse_holidays(text: str) -> list[Holiday]:
    """응답 본문 → 공휴일 목록 (날짜, seq 순). isHoliday가 Y가 아닌 항목은 뺀다.

    - resultCode가 정상(00)이 아니면 ApiError
    - JSON이 아니거나(게이트웨이의 XML 에러 등) 필수 필드가 없으면 UnexpectedResponseError
    - totalCount보다 적게 받았으면 UnexpectedResponseError (공휴일을 조용히 빠뜨리지 않는다)
    """
    header, body = _header_and_body(text)
    code = str(header.get("resultCode", ""))
    if code != _NORMAL:
        raise ApiError(code, str(header.get("resultMsg", "")))

    items = _items(body)
    total = _total_count(body)
    if total > len(items):
        raise UnexpectedResponseError(f"got {len(items)} of {total} items; raise _MAX_ROWS")

    ordered = sorted(items, key=lambda item: (str(item.get("locdate", "")), _seq(item)))
    return [_holiday(item) for item in ordered if item.get("isHoliday") == "Y"]


def _header_and_body(text: str) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        root = json.loads(text)
    except ValueError as e:
        raise UnexpectedResponseError(f"non-JSON response: {text[:200]!r}") from e
    response = root.get("response") if isinstance(root, dict) else None
    header = response.get("header") if isinstance(response, dict) else None
    body = response.get("body") if isinstance(response, dict) else None
    if not isinstance(header, dict) or not isinstance(body, dict):
        raise UnexpectedResponseError("missing response.header or response.body")
    return header, body


def _items(body: dict[str, Any]) -> list[dict[str, Any]]:
    items = body.get("items")
    if not items:  # 0건: 빈 문자열, None, {}
        return []
    item = items.get("item") if isinstance(items, dict) else None
    if isinstance(item, dict):  # 1건이면 배열이 아니라 객체로 온다
        return [item]
    if isinstance(item, list) and all(isinstance(i, dict) for i in item):
        return item
    raise UnexpectedResponseError(f"unexpected items: {str(items)[:200]!r}")


def _total_count(body: dict[str, Any]) -> int:
    try:
        return int(body["totalCount"])
    except (KeyError, TypeError, ValueError) as e:
        raise UnexpectedResponseError("missing or invalid totalCount") from e


def _seq(item: dict[str, Any]) -> int:
    try:
        return int(item.get("seq", 0))
    except (TypeError, ValueError):
        return 0


def _holiday(item: dict[str, Any]) -> Holiday:
    raw = str(item.get("locdate", ""))  # 숫자(20260101)로 온다
    if len(raw) != 8 or not raw.isdigit():
        raise UnexpectedResponseError(f"invalid locdate: {raw!r}")
    try:
        day = date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    except ValueError as e:
        raise UnexpectedResponseError(f"invalid locdate: {raw!r}") from e
    return Holiday(day, str(item.get("dateName", "")).strip())
