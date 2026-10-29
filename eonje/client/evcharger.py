"""EvCharger API HTTP 클라이언트: 요청, 재시도, 시도별 보고. 응답 해석은 response.py."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential
from tenacity.wait import wait_base

from eonje.client.errors import ApiError, TransientError
from eonje.client.paging import page_count
from eonje.client.response import parse_response
from eonje.client.types import CallRecord, Page
from eonje.config import Settings

OnCall = Callable[[CallRecord], None]


class EvChargerClient:
    def __init__(
        self,
        http: httpx.Client,
        settings: Settings,
        on_call: OnCall | None = None,
        wait: wait_base | None = None,
    ) -> None:
        self._http = http
        self._settings = settings
        self._on_call: OnCall = on_call or (lambda _record: None)
        self._wait = wait or wait_exponential(multiplier=1, max=30)

    def fetch_page(self, operation: str, page_no: int, params: Mapping[str, str]) -> Page:
        retrying = Retrying(
            stop=stop_after_attempt(self._settings.api_max_attempts),
            wait=self._wait,
            retry=retry_if_exception_type((httpx.TransportError, TransientError)),
            reraise=True,
        )

        for attempt in retrying:
            with attempt:
                return self._call_once(
                    operation, page_no, params, attempt.retry_state.attempt_number
                )
        raise AssertionError("unreachable: Retrying either returns or reraises")

    def iter_pages(
        self,
        operation: str,
        params: Mapping[str, str],
        max_pages: int | None = None,
    ) -> Iterator[Page]:
        """1페이지의 totalCount로 마지막 페이지를 정하고, max_pages로 한 번 더 자른다.

        max_pages는 페이지 수 상한이지 HTTP 호출 수 상한이 아니다. 페이지마다 최대
        api_max_attempts번 시도하므로 최악의 호출 수는 max_pages * api_max_attempts다.
        예산 판단은 이 최악값을 기준으로 해야 한다.
        """
        if max_pages is not None and max_pages < 1:
            return
        first = self.fetch_page(operation, 1, params)
        yield first

        last = page_count(first.total_count, self._settings.page_size)

        if max_pages is not None:
            last = min(last, max_pages)

        for page_no in range(2, last + 1):
            yield self.fetch_page(operation, page_no, params)

    def _call_once(
        self, operation: str, page_no: int, params: Mapping[str, str], attempt_no: int
    ) -> Page:
        query = {
            **params,
            "serviceKey": self._settings.api_service_key.get_secret_value(),
            "pageNo": str(page_no),
            "numOfRows": str(self._settings.page_size),
            "dataType": "JSON",
        }
        called_at = datetime.now(UTC)
        started = time.monotonic()
        status: int | None = None
        code: str | None = None
        total: int | None = None
        try:
            resp = self._http.get(
                f"{self._settings.api_base_url}/{operation}",
                params=query,
                timeout=self._settings.api_timeout_sec,
            )
            status = resp.status_code
            if status == 429 or status >= 500:
                raise TransientError(f"{operation} page {page_no}: http {status}")

            try:
                parsed = parse_response(resp.text)
            except ApiError as e:
                code = e.result_code
                raise
            code, total = parsed.result_code, parsed.total_count
            return Page(operation, page_no, parsed.total_count, parsed.items, called_at)
        finally:
            self._on_call(
                CallRecord(
                    operation=operation,
                    page_no=page_no,
                    params={k: v for k, v in query.items() if k != "serviceKey"},
                    called_at=called_at,
                    elapsed_ms=int((time.monotonic() - started) * 1000),
                    attempt=attempt_no,
                    http_status=status,
                    result_code=code,
                    total_count=total,
                )
            )
