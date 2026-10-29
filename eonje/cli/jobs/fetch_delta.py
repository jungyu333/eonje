"""fetch-delta: 의존성을 만들어 collect_delta를 부르고, 결과를 종료 코드로 바꾼다."""

from __future__ import annotations

import httpx

from eonje.cli import exit_codes
from eonje.cli.context import JobContext
from eonje.collect.delta import DeltaResult, Outcome, collect_delta
from eonje.storage.db import make_engine, make_session_factory
from eonje.storage.fs import raw_filesystem

JOB = "fetch-delta"


def run(ctx: JobContext) -> int:
    engine = make_engine(ctx.settings)
    try:
        with httpx.Client() as http:
            result = collect_delta(
                ctx.settings,
                job=JOB,
                run_id=ctx.run_id,
                run_after=ctx.run_after,
                http=http,
                fs=raw_filesystem(ctx.settings),
                factory=make_session_factory(engine),
            )
    finally:
        engine.dispose()
    return exit_code(result)


def exit_code(result: DeltaResult) -> int:
    """이미 완료된 run은 성공(이후 태스크 진행), 받지 않은 run은 skipped."""
    match result.outcome:
        case Outcome.COLLECTED | Outcome.ALREADY_COMPLETE:
            return exit_codes.OK
        case Outcome.SKIPPED_STALE | Outcome.SKIPPED_BUDGET:
            return exit_codes.SKIPPED
