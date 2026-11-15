"""차원 적재 전에 full run을 반영해도 되는지 판단한다. 순수 함수.

마스터에 없는 충전소·충전기는 현재 버전을 닫는다(SCD2). 그래서 일부만 받은 run을 반영하면
받지 못한 것들이 없어진 것처럼 기록된다. 그런 run은 건너뛴다 (design.md 7장).
"""

from __future__ import annotations

from decimal import Decimal

from eonje.storage.raw.run import RunManifest


def skip_reason(manifest: RunManifest, *, current_rows: int, min_row_ratio: float) -> str | None:
    """반영하면 안 되는 이유를 돌려준다. 반영해도 되면 None.

    current_rows는 지금 차원에 있는 현재 버전 충전기 수다. 0이면(첫 적재) 행 수를 비교하지 않는다.
    """

    if manifest.row_count == 0:
        return "empty run (0 rows)"
    if manifest.truncated:
        return f"truncated run ({len(manifest.pages)} of {manifest.expected_pages} pages)"

    # Decimal(str(...)): 0.55를 적힌 그대로 계산한다 (float 곱셈은 100 * 0.55 = 55.00000000000001)
    if current_rows > 0 and manifest.row_count < current_rows * Decimal(str(min_row_ratio)):
        return (
            f"row count dropped: {manifest.row_count} rows against {current_rows} "
            f"current chargers (min ratio {min_row_ratio})"
        )

    return None
