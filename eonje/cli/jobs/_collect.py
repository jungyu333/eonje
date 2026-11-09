"""수집 잡 공용: collect 결과(Outcome)를 종료 코드로 바꾼다."""

from __future__ import annotations

from eonje.cli import exit_codes
from eonje.collect.run import CollectResult, Outcome


def exit_code(result: CollectResult) -> int:
    """이미 완료된 run은 성공(이후 태스크 진행), 받지 않은 run은 skipped."""
    match result.outcome:
        case Outcome.COLLECTED | Outcome.ALREADY_COMPLETE:
            return exit_codes.OK
        case Outcome.SKIPPED_STALE | Outcome.SKIPPED_BUDGET:
            return exit_codes.SKIPPED
