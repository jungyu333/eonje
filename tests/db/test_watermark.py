"""DB tests for eonje.storage.postgres.watermark: read_watermark and advance_watermark.

The forward-only guard lives in SQL (ON CONFLICT ... DO UPDATE ... WHERE row comparison), so these
run against real Postgres (rolled back per test). Every timestamp is passed explicitly.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from eonje.storage.postgres.db import session_scope
from eonje.storage.postgres.models import TransformWatermark
from eonje.storage.postgres.watermark import Watermark, advance_watermark, read_watermark
from eonje.storage.raw.paths import run_key_from_run_id
from eonje.timeutil import KST

pytestmark = pytest.mark.db

TASK = "load_dims"
OTHER_TASK = "dedupe_load_obs"
T0 = datetime(2026, 11, 10, 1, 0, tzinfo=UTC)
NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
LATER_NOW = NOW + timedelta(hours=1)
RUN_KEY = run_key_from_run_id("scheduled__2026-11-10T01:00:00+00:00")

# (earlier, later) pairs, ordered by Watermark's own ordering (collected_at, then run_key).
# Run keys are path-safe [0-9A-Za-z_.-], so mixed case and '-' vs '_' are valid keys.
ORDERED_PAIRS = [
    pytest.param(
        Watermark(T0, "run-a"),
        Watermark(T0 + timedelta(minutes=10), "run-b"),
        id="later_collected_at",
    ),
    pytest.param(
        Watermark(T0, "run-z"),
        Watermark(T0 + timedelta(minutes=10), "run-a"),
        id="later_collected_at_wins_over_smaller_run_key",
    ),
    pytest.param(
        Watermark(T0, "20261110T010000"),
        Watermark(T0, "20261110T010001"),
        id="same_collected_at_later_run_key",
    ),
    pytest.param(
        Watermark(T0, "run-B"),
        Watermark(T0, "run-a"),
        id="same_collected_at_uppercase_run_key_sorts_first",
    ),
    pytest.param(
        Watermark(T0, "run-b"),
        Watermark(T0, "run_a"),
        id="same_collected_at_hyphen_run_key_sorts_before_underscore",
    ),
]


def _advance(factory: sessionmaker[Session], task: str, to: Watermark, *, now: datetime) -> None:
    with session_scope(factory) as session:
        advance_watermark(session, task, to, now=now)


def _read(factory: sessionmaker[Session], task: str) -> Watermark | None:
    with factory() as session:
        return read_watermark(session, task)


def _row(factory: sessionmaker[Session], task: str) -> tuple[datetime, str, datetime]:
    with factory() as session:
        row = session.execute(
            select(
                TransformWatermark.collected_at,
                TransformWatermark.run_key,
                TransformWatermark.updated_at,
            ).where(TransformWatermark.task == task)
        ).one()
    return row.collected_at, row.run_key, row.updated_at


def _with_naive(field: str, to: Watermark, now: datetime) -> tuple[Watermark, datetime]:
    if field == "collected_at":
        return replace(to, collected_at=to.collected_at.replace(tzinfo=None)), now
    return to, now.replace(tzinfo=None)


# --- read_watermark on a task never advanced ---------------------------------


def test_task_never_advanced_reads_none(session_factory: sessionmaker[Session]) -> None:
    # GIVEN an empty transform_watermark table
    # WHEN a task's watermark is read
    got = _read(session_factory, TASK)

    # THEN there is none
    assert got is None


# --- advance_watermark: first write and forward moves ------------------------


def test_first_advance_is_read_back_as_given(session_factory: sessionmaker[Session]) -> None:
    # GIVEN a task never advanced and the run it just processed
    to = Watermark(T0, RUN_KEY)

    # WHEN the task advances to that run
    _advance(session_factory, TASK, to, now=NOW)

    # THEN the same watermark is read back, stamped with the given now
    assert _read(session_factory, TASK) == to
    assert _row(session_factory, TASK) == (T0, RUN_KEY, NOW)


@pytest.mark.parametrize(("earlier", "later"), ORDERED_PAIRS)
def test_advancing_to_a_later_watermark_replaces_it(
    session_factory: sessionmaker[Session], earlier: Watermark, later: Watermark
) -> None:
    # GIVEN a task at `earlier`, and `later` ordered after it by Watermark's own ordering
    assert earlier < later
    _advance(session_factory, TASK, earlier, now=NOW)

    # WHEN the task advances to `later`
    _advance(session_factory, TASK, later, now=LATER_NOW)

    # THEN `later` is read back, stamped with the second now
    assert _read(session_factory, TASK) == later
    assert _row(session_factory, TASK)[2] == LATER_NOW


# --- advance_watermark: never moves back -------------------------------------


@pytest.mark.parametrize(("earlier", "later"), ORDERED_PAIRS)
def test_advancing_to_an_earlier_watermark_keeps_the_current_one(
    session_factory: sessionmaker[Session], earlier: Watermark, later: Watermark
) -> None:
    # GIVEN a task at `later`, and `earlier` ordered before it by Watermark's own ordering
    assert earlier < later
    _advance(session_factory, TASK, later, now=NOW)
    before = _row(session_factory, TASK)

    # WHEN the task tries to move back to `earlier` (e.g. a late rerun of an old run)
    _advance(session_factory, TASK, earlier, now=LATER_NOW)

    # THEN the row is untouched: still `later`, with the first updated_at
    assert _read(session_factory, TASK) == later
    assert _row(session_factory, TASK) == before


def test_advancing_twice_to_the_same_watermark_leaves_the_same_row(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a task advanced once
    to = Watermark(T0, RUN_KEY)
    _advance(session_factory, TASK, to, now=NOW)
    first = _row(session_factory, TASK)

    # WHEN the identical call is repeated
    _advance(session_factory, TASK, to, now=NOW)

    # THEN the stored row is the same as after the first call
    assert _row(session_factory, TASK) == first


# --- time zones --------------------------------------------------------------


def test_watermark_given_in_kst_is_read_back_as_the_same_instant(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a collected_at expressed in KST
    to = Watermark(datetime(2026, 11, 10, 10, 0, tzinfo=KST), RUN_KEY)

    # WHEN the task advances to it
    _advance(session_factory, TASK, to, now=NOW)

    # THEN the read value is the same instant and still timezone-aware
    got = _read(session_factory, TASK)
    assert got == to
    assert got is not None
    assert got.collected_at.utcoffset() is not None


def test_kst_watermark_later_on_the_wall_clock_but_earlier_in_time_is_kept_out(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN a task at 10:00 UTC (19:00 KST)
    current = Watermark(datetime(2026, 11, 10, 10, 0, tzinfo=UTC), "run-a")
    _advance(session_factory, TASK, current, now=NOW)

    # AND a candidate at 18:30 KST: 18:30 > 10:00 on the wall clock, but it is 09:30 UTC
    candidate = Watermark(datetime(2026, 11, 10, 18, 30, tzinfo=KST), "run-b")
    assert candidate < current

    # WHEN the task tries to advance to the candidate
    _advance(session_factory, TASK, candidate, now=LATER_NOW)

    # THEN the comparison is by instant and the watermark stays at 10:00 UTC
    assert _read(session_factory, TASK) == current


# --- tasks are independent ---------------------------------------------------


def test_advancing_one_task_leaves_a_never_advanced_task_at_none(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN one task with a watermark
    _advance(session_factory, TASK, Watermark(T0, RUN_KEY), now=NOW)

    # WHEN a different task's watermark is read
    got = _read(session_factory, OTHER_TASK)

    # THEN that task still has none
    assert got is None


def test_a_task_behind_another_can_still_advance_and_each_reads_its_own(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN one task already an hour ahead
    ahead = Watermark(T0 + timedelta(hours=1), "run-b")
    _advance(session_factory, TASK, ahead, now=NOW)

    # WHEN another task advances to an earlier watermark
    behind = Watermark(T0, "run-a")
    _advance(session_factory, OTHER_TASK, behind, now=NOW)

    # THEN each task reads its own value; the other task's position did not block the write
    assert _read(session_factory, TASK) == ahead
    assert _read(session_factory, OTHER_TASK) == behind


def test_advancing_one_task_does_not_change_another_tasks_row(
    session_factory: sessionmaker[Session],
) -> None:
    # GIVEN two tasks at the same watermark
    start = Watermark(T0, "run-a")
    _advance(session_factory, TASK, start, now=NOW)
    _advance(session_factory, OTHER_TASK, start, now=NOW)
    other_before = _row(session_factory, OTHER_TASK)

    # WHEN only the first task moves forward
    _advance(session_factory, TASK, Watermark(T0 + timedelta(minutes=10), "run-b"), now=LATER_NOW)

    # THEN the other task's row is untouched
    assert _row(session_factory, OTHER_TASK) == other_before


# --- naive datetimes ---------------------------------------------------------


@pytest.mark.parametrize("field", ["collected_at", "now"])
def test_naive_time_is_rejected_and_nothing_is_written(
    session_factory: sessionmaker[Session], field: str
) -> None:
    # GIVEN a task never advanced and a call where one time has no timezone
    to, now = _with_naive(field, Watermark(T0, RUN_KEY), NOW)

    # WHEN the task advances THEN ValueError names the naive field
    with pytest.raises(ValueError, match=f"^{field} must be timezone-aware"):
        _advance(session_factory, TASK, to, now=now)

    # AND no watermark was stored
    assert _read(session_factory, TASK) is None


@pytest.mark.parametrize("field", ["collected_at", "now"])
def test_naive_time_leaves_an_existing_watermark_unchanged(
    session_factory: sessionmaker[Session], field: str
) -> None:
    # GIVEN a task with a watermark
    _advance(session_factory, TASK, Watermark(T0, "run-a"), now=NOW)
    before = _row(session_factory, TASK)

    # AND a later watermark where one time has no timezone
    to, now = _with_naive(field, Watermark(T0 + timedelta(minutes=10), "run-b"), LATER_NOW)

    # WHEN the task advances THEN ValueError names the naive field
    with pytest.raises(ValueError, match=f"^{field} must be timezone-aware"):
        _advance(session_factory, TASK, to, now=now)

    # AND the existing row is untouched
    assert _row(session_factory, TASK) == before
