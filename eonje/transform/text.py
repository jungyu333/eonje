"""원본 문자열 값 정리. 원본을 읽는 변환(dims, observations)이 같이 쓴다."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

_EDGE_SPACE = r"^[\s\p{Z}]+|[\s\p{Z}]+$"  # 앞뒤 공백 (탭·줄바꿈·전각 공백 포함)


def blank_to_null(api_field: str) -> Column:
    """앞뒤 공백을 지우고, 남는 게 없으면 NULL. trim은 ASCII 공백만 지워서 정규식을 쓴다."""
    return F.nullif(F.regexp_replace(F.col(api_field), _EDGE_SPACE, ""), F.lit(""))
