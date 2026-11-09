from __future__ import annotations

import httpx

from eonje.cli.context import JobContext
from eonje.cli.jobs._collect import exit_code
from eonje.collect.master import collect_master
from eonje.storage.postgres.db import make_engine, make_session_factory
from eonje.storage.raw.fs import raw_filesystem

JOB = "fetch-master"


def run(ctx: JobContext) -> int:
    engine = make_engine(ctx.settings)

    try:
        with httpx.Client() as http:
            result = collect_master(
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
