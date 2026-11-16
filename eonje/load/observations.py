"""원본 run을 관측(stg.obs_status)으로 적재하는 흐름. 한 번에 데이터셋 하나를 처리한다.

워터마크 뒤의 확정된 run을 날짜별 묶음으로 처리한다 (pending.pending_batches). 묶음마다 원본을
읽어 관측 행으로 바꾸고, Iceberg에 넣은 뒤 워터마크를 그 묶음의 마지막 run으로 올린다.
그 사이에 죽으면 같은 묶음을 다시 넣는데, 키가 이미 있는 행은 무시되므로 결과가 같다.

차원 적재와 달리 잘린 run도 넣는다. 관측은 행 하나하나가 유효하다. 행이 없는 run은 읽지 않고
워터마크만 넘긴다 (그런 run의 파일에는 응답 필드 컬럼이 없다).

원본 읽기(read_raw)와 워터마크 올리기(advance)는 밖에서 받는다. S3·Postgres 없이 Spark만으로
테스트하기 위해서다. 실제 연결은 dedupe_load_obs 잡이 한다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from eonje.load.pending import pending_batches
from eonje.storage.lake.observations import merge_observations
from eonje.storage.postgres.watermark import Watermark
from eonje.storage.raw.run import RunManifest
from eonje.transform.observations import OBS_KEY, observations

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchResult:
    """묶음 하나(같은 날짜의 run들)를 적재한 결과."""

    dataset: str
    dt: str  # 묶음의 날짜 (KST)
    runs: int
    rows: int  # 넘긴 관측 행 수 (run 안 중복 제거 후). 이미 있어서 무시된 행도 센다


def load_observations(
    spark: SparkSession,
    manifests: Iterable[RunManifest],
    *,
    watermark: Watermark | None,
    now: datetime,
    settle: timedelta,
    read_raw: Callable[[Sequence[RunManifest]], DataFrame],
    advance: Callable[[Watermark], None],
) -> list[BatchResult]:
    """한 데이터셋의 run 중 워터마크 뒤이고 확정된 것을 관측으로 넣는다. 결과는 묶음마다 하나다."""
    results: list[BatchResult] = []
    for batch in pending_batches(manifests, watermark=watermark, now=now, settle=settle):
        filled = [m for m in batch if m.row_count > 0]
        rows = merge_observations(spark, observations(read_raw(filled)), OBS_KEY) if filled else 0
        last = batch[-1]
        advance(Watermark(last.collected, last.run_key))  # Iceberg에 쓴 뒤에 올린다

        result = BatchResult(last.dataset, last.dt, len(batch), rows)
        log.info("loaded %s %s: %d runs, %d rows", result.dataset, result.dt, result.runs, rows)
        results.append(result)
    return results
