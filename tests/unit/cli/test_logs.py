import logging
from collections.abc import Iterator

import httpx
import pytest
from tenacity import wait_none

from eonje.cli.logs import QUIET_LOGGERS, configure_logging
from eonje.client.evcharger import EvChargerClient
from eonje.config import Settings

SERVICE_KEY = "secret-service-key-123"
OK_BODY = '{"resultCode": "00", "resultMsg": "정상", "totalCount": 0, "items": ""}'


@pytest.fixture(autouse=True)
def restore_logger_levels() -> Iterator[None]:
    """configure_logging changes global loggers; put them back for other tests."""
    saved = {name: logging.getLogger(name).level for name in QUIET_LOGGERS}
    yield
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)


def _fetch_once() -> None:
    settings = Settings(api_service_key=SERVICE_KEY)  # type: ignore[arg-type]
    transport = httpx.MockTransport(lambda _req: httpx.Response(200, text=OK_BODY))
    client = EvChargerClient(httpx.Client(transport=transport), settings, wait=wait_none())
    client.fetch_page("getChargerStatus", 1, {"period": "10"})


def test_httpx_logs_the_full_url_with_the_key_when_not_configured(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # GIVEN httpx loggers at their default level and INFO capture
    for name in QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.NOTSET)
    caplog.set_level(logging.INFO)

    # WHEN a page is fetched
    _fetch_once()

    # THEN the key leaks through the request log line (the risk configure_logging removes)
    assert SERVICE_KEY in caplog.text


def test_service_key_never_reaches_logs_after_configure_logging(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # GIVEN CLI logging is configured and everything at INFO is captured
    configure_logging()
    caplog.set_level(logging.INFO)

    # WHEN a page is fetched
    _fetch_once()

    # THEN no captured record contains the key and httpx emits nothing below WARNING
    assert SERVICE_KEY not in caplog.text
    assert not [r for r in caplog.records if r.name.startswith(QUIET_LOGGERS)]
