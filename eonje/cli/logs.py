"""CLI 프로세스의 로깅 설정. job은 모두 CLI로 실행되므로 여기 한 곳에서 정한다."""

from __future__ import annotations

import logging

# 요청 URL을 INFO로 남기는 HTTP 라이브러리. URL 쿼리에 serviceKey가 들어가므로
# WARNING 미만은 막는다.
QUIET_LOGGERS = ("httpx", "httpcore")


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    for name in QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
