from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from eonje.client.errors import ApiError, UnexpectedResponseError

NORMAL = "00"
NO_DATA = "03"


@dataclass(frozen=True)
class ParsedResponse:
    result_code: str
    total_count: int
    items: list[dict[str, Any]]


def parse_response(text: str) -> ParsedResponse:
    """응답 본문 → 결과 코드·전체 건수·items.

    - NORMAL: totalCount와 items를 돌려준다
    - NO_DATA: 0건으로 돌려준다 (에러 아님)
    - 그 외 결과 코드: ApiError
    - JSON이 아님(게이트웨이 XML 에러 등)·필수 필드 누락: UnexpectedResponseError
    """
    body = _load_json(text)
    code = str(body.get("resultCode", ""))
    if code == NO_DATA:
        return ParsedResponse(code, 0, [])
    if code != NORMAL:
        raise ApiError(code, str(body.get("resultMsg", "")))
    return ParsedResponse(code, _total_count(body), _items(body))


def _load_json(text: str) -> dict[str, Any]:
    try:
        body = json.loads(text)
    except ValueError as e:
        raise UnexpectedResponseError(f"non-JSON response: {text[:200]!r}") from e
    if not isinstance(body, dict):
        raise UnexpectedResponseError(f"unexpected JSON root: {type(body).__name__}")
    return body


def _total_count(body: dict[str, Any]) -> int:
    try:
        return int(body["totalCount"])
    except (KeyError, TypeError, ValueError) as e:
        raise UnexpectedResponseError("missing or invalid totalCount") from e


def _items(body: dict[str, Any]) -> list[dict[str, Any]]:
    items = body.get("items")
    if not items:  # "" / None / {}
        return []
    if not isinstance(items, dict):
        raise UnexpectedResponseError(f"unexpected items: {type(items).__name__}")
    item = items.get("item")
    if item is None:
        return []
    if isinstance(item, dict):  # 1건이면 객체로 오는 경우
        return [item]
    if isinstance(item, list):
        return item
    raise UnexpectedResponseError(f"unexpected item: {type(item).__name__}")
