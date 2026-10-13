from __future__ import annotations

from collections.abc import Callable

from eonje.cli.jobs import check_budget, fetch_delta, fetch_master

Job = Callable[[str], int]

JOBS: dict[str, Job] = {
    "fetch-delta": fetch_delta.run,
    "fetch-master": fetch_master.run,
    "check-budget": check_budget.run,
}
