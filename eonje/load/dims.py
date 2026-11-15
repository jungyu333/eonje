"""마스터 full run을 차원 테이블(dim_station, dim_charger)에 SCD2로 반영하는 흐름.

완료된 run을 오래된 것부터 본다. 워터마크 이전 run은 지나가고, 반영할 수 없는 run
(policy.skip_reason)은 건너뛰되 워터마크를 올리지 않는다. 뒤에 온 run이 반영되면 워터마크가
그 run까지 넘어간다.

쓰는 순서는 Iceberg 먼저, 워터마크 나중이다. 그 사이에 죽으면 같은 run을 다시 반영하는데,
변경분을 현재 테이블과 비교해 새로 계산하므로 두 번 반영해도 결과가 같다.

원본 읽기(read_raw)와 워터마크 올리기(advance)는 밖에서 받는다. S3·Postgres 없이
Spark만으로 테스트하기 위해서다. 실제 연결은 load_dims 잡이 한다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from eonje.load.policy import skip_reason
from eonje.storage.lake.dims import MergeCounts, current_rows, merge_changes
from eonje.storage.lake.iceberg import DIM_CHARGER, DIM_STATION, IcebergTable
from eonje.storage.postgres.watermark import Watermark
from eonje.storage.raw.run import RunManifest
from eonje.transform.dims import CHARGER_KEY, STATION_KEY, chargers, scd2_changes, stations

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    """run 하나를 본 결과. 반영했으면 skipped가 None이고 변경 수가 있다."""

    run_key: str
    skipped: str | None  # 건너뛴 이유
    stations: MergeCounts | None
    chargers: MergeCounts | None


def load_dims(
    spark: SparkSession,
    manifests: Iterable[RunManifest],
    *,
    watermark: Watermark | None,
    read_raw: Callable[[RunManifest], DataFrame],
    advance: Callable[[Watermark], None],
    min_row_ratio: float,
) -> list[RunResult]:
    """워터마크 뒤의 run을 차원에 반영하고, 본 run마다 결과를 순서대로 돌려준다.

    manifests는 (collected_at, run_key) 오름차순이어야 한다 (list_manifests의 결과).
    """
    results: list[RunResult] = []
    for manifest in manifests:
        mark = Watermark(manifest.collected, manifest.run_key)
        if watermark is not None and mark <= watermark:
            continue

        reason = skip_reason(
            manifest,
            current_rows=current_rows(spark, DIM_CHARGER),
            min_row_ratio=min_row_ratio,
        )
        if reason is not None:
            log.warning("skipped run %s: %s", manifest.run_key, reason)
            results.append(RunResult(manifest.run_key, reason, None, None))
            continue

        raw = read_raw(manifest)
        station_counts = _merge(spark, DIM_STATION, STATION_KEY, stations(raw), manifest)
        charger_counts = _merge(spark, DIM_CHARGER, CHARGER_KEY, chargers(raw), manifest)
        advance(mark)  # Iceberg에 쓴 뒤에 올린다

        log.info(
            "applied run %s: stations closed=%d opened=%d, chargers closed=%d opened=%d",
            manifest.run_key,
            station_counts.closed,
            station_counts.opened,
            charger_counts.closed,
            charger_counts.opened,
        )
        results.append(RunResult(manifest.run_key, None, station_counts, charger_counts))

    return results


def _merge(
    spark: SparkSession,
    table: IcebergTable,
    key: tuple[str, ...],
    incoming: DataFrame,
    manifest: RunManifest,
) -> MergeCounts:
    """현재 차원과 새 스냅샷을 비교해 바뀐 것만 반영한다."""
    changes = scd2_changes(
        spark.table(table.full_name),
        incoming,
        key,
        collected_at=manifest.collected,
        run_key=manifest.run_key,
    )
    return merge_changes(spark, table, key, changes)
