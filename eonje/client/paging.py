"""페이지 계산. 클라이언트(몇 페이지를 요청할지)와 raw run(몇 페이지를 기대했는지)이 함께 쓴다."""

from __future__ import annotations


def page_count(total_count: int, page_size: int) -> int:
    """totalCount를 다 받는 데 필요한 페이지 수. 결과가 0건이어도 1페이지는 요청한다."""
    if page_size < 1:
        raise ValueError(f"page_size must be >= 1, got {page_size}")
    return max(1, -(-total_count // page_size))
