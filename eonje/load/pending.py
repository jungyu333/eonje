"""처리할 원본 run 고르기. 순수 함수.

워터마크 뒤에 있고 완료가 확정된 run만 고른다. 확정이란 collected_at에서 '지연 허용치 + 수집
타임아웃'(settle)이 지났다는 뜻이다. 그 전에는 재시도로 늦게 끝나는 run이 아직 나타날 수 있다.
그런 run보다 뒤의 run을 먼저 처리하면 워터마크가 지나가 버려 그 run을 영영 읽지 않는다
(design.md 7장).
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from itertools import groupby

from eonje.storage.postgres.watermark import Watermark
from eonje.storage.raw.run import RunManifest
from eonje.timeutil import require_aware


def pending_batches(
    manifests: Iterable[RunManifest],
    *,
    watermark: Watermark | None,
    now: datetime,
    settle: timedelta,
) -> list[list[RunManifest]]:
    """처리할 run을 날짜(dt)별 묶음으로 돌려준다. 묶음도, 묶음 안의 run도 오래된 것부터다.

    - 워터마크와 같거나 그보다 앞선 run은 뺀다 (이미 처리했다)
    - collected_at이 now - settle보다 늦은 run은 뺀다 (아직 확정되지 않았다)
    묶음 하나는 MERGE 한 번이고 관측 파티션(observed_date) 하나에 쓴다.
    """
    require_aware("now", now)
    cutoff = now - settle
    pending = sorted(
        (
            m
            for m in manifests
            if m.collected <= cutoff
            and (watermark is None or Watermark(m.collected, m.run_key) > watermark)
        ),
        key=lambda m: (m.collected, m.run_key),
    )
    return [list(group) for _, group in groupby(pending, key=lambda m: m.dt)]
