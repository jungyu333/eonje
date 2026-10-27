import itertools
import math
from datetime import UTC, datetime, timedelta, timezone
from fractions import Fraction

import pytest

from eonje.budget.policy import BudgetDecision, decide, quota_day_bounds
from eonje.timeutil import KST


def _decide(
    *,
    daily_limit: int = 1000,
    safety_ratio: float = 0.95,
    reserve_calls: int = 0,
    used_today: int = 0,
    max_attempts: int = 3,
) -> BudgetDecision:
    return decide(
        daily_limit=daily_limit,
        safety_ratio=safety_ratio,
        reserve_calls=reserve_calls,
        used_today=used_today,
        max_attempts=max_attempts,
    )


# --- decide: worked examples ---


def test_fresh_day_runs_with_pages_capped_by_worst_case_retries() -> None:
    # GIVEN limit 1000, ratio 0.95, no reserve, nothing used, 3 attempts per page
    # WHEN deciding
    decision = _decide()

    # THEN it runs with 950 usable calls and floor(950 / 3) pages
    assert decision.run is True
    assert decision.usable_calls == 950
    assert decision.max_pages == 316


def test_remaining_calls_below_one_page_worth_of_attempts_skips() -> None:
    # GIVEN 948 used of a 950 ceiling, so 2 calls remain but a page may take 3
    # WHEN deciding
    decision = _decide(used_today=948)

    # THEN it skips with no pages, still reporting the 2 usable calls
    assert decision.run is False
    assert decision.max_pages == 0
    assert decision.usable_calls == 2


def test_remaining_calls_exactly_one_page_worth_of_attempts_runs_one_page() -> None:
    # GIVEN 947 used of a 950 ceiling, so exactly 3 calls remain for 3 attempts
    # WHEN deciding
    decision = _decide(used_today=947)

    # THEN it runs exactly one page
    assert decision.run is True
    assert decision.usable_calls == 3
    assert decision.max_pages == 1


@pytest.mark.parametrize("used_today", [950, 951, 980, 1000, 5000])
def test_usage_at_or_over_the_ceiling_skips_with_zero_not_negative_usable(
    used_today: int,
) -> None:
    # GIVEN usage at or past the 950 ceiling (e.g. calls made outside this process)
    # WHEN deciding
    decision = _decide(used_today=used_today)

    # THEN it skips and usable is clamped to 0
    assert decision.run is False
    assert decision.max_pages == 0
    assert decision.usable_calls == 0


def test_reserve_reduces_usable_calls_by_exactly_the_reserve() -> None:
    # GIVEN the same inputs with and without a reserve of 160
    without = _decide(reserve_calls=0, used_today=100)

    # WHEN a reserve of 160 is applied
    with_reserve = _decide(reserve_calls=160, used_today=100)

    # THEN usable drops by 160 and pages follow
    assert without.usable_calls - with_reserve.usable_calls == 160
    assert with_reserve.usable_calls == 690
    assert with_reserve.max_pages == 230


def test_reserve_larger_than_ceiling_skips_with_zero_usable() -> None:
    # GIVEN a reserve that exceeds the ratio-scaled ceiling
    # WHEN deciding on a fresh day
    decision = _decide(reserve_calls=2000, used_today=0)

    # THEN it skips rather than reporting negative usable calls
    assert decision.run is False
    assert decision.max_pages == 0
    assert decision.usable_calls == 0


def test_safety_ratio_of_one_makes_the_ceiling_equal_to_the_limit() -> None:
    # GIVEN ratio 1.0 and a single attempt per page
    # WHEN deciding on a fresh day
    decision = _decide(safety_ratio=1.0, max_attempts=1)

    # THEN every call up to the limit is usable
    assert decision.usable_calls == 1000
    assert decision.max_pages == 1000


def test_safety_ratio_of_one_allows_the_last_call_of_the_day() -> None:
    # GIVEN ratio 1.0, 999 of 1000 used, one attempt per page
    # WHEN deciding
    decision = _decide(safety_ratio=1.0, used_today=999, max_attempts=1)

    # THEN exactly one page may run
    assert decision.run is True
    assert decision.max_pages == 1


@pytest.mark.parametrize("max_attempts", [0, -1])
def test_non_positive_max_attempts_is_rejected(max_attempts: int) -> None:
    # GIVEN an attempt count that would divide by zero or be meaningless
    # WHEN / THEN deciding raises
    with pytest.raises(ValueError):
        _decide(max_attempts=max_attempts)


def test_negative_used_today_is_rejected() -> None:
    # GIVEN a negative usage count (corrupt lookup)
    # WHEN / THEN deciding raises instead of inflating the budget
    with pytest.raises(ValueError):
        _decide(used_today=-1)


@pytest.mark.parametrize("daily_limit", [0, -1])
def test_non_positive_daily_limit_is_rejected(daily_limit: int) -> None:
    # GIVEN a limit that allows no calls
    # WHEN / THEN deciding raises instead of silently skipping forever
    with pytest.raises(ValueError, match="daily_limit"):
        _decide(daily_limit=daily_limit)


@pytest.mark.parametrize("safety_ratio", [0.0, -0.1, 1.01, 2.0])
def test_safety_ratio_outside_unit_interval_is_rejected(safety_ratio: float) -> None:
    # GIVEN a ratio outside (0, 1] (above 1 would allow exceeding the limit)
    # WHEN / THEN deciding raises
    with pytest.raises(ValueError, match="safety_ratio"):
        _decide(safety_ratio=safety_ratio)


def test_negative_reserve_is_rejected() -> None:
    # GIVEN a negative reserve (would inflate the budget)
    # WHEN / THEN deciding raises
    with pytest.raises(ValueError, match="reserve_calls"):
        _decide(reserve_calls=-1)


def test_ratio_is_applied_exactly_without_float_rounding() -> None:
    # GIVEN 100 * 0.29, which is 28.999... in float arithmetic
    # WHEN deciding with one attempt per page
    decision = _decide(daily_limit=100, safety_ratio=0.29, max_attempts=1)

    # THEN the ceiling is exactly 29, not 28
    assert decision.usable_calls == 29


def test_deciding_twice_on_the_same_input_yields_the_same_decision() -> None:
    # GIVEN fixed inputs
    # WHEN deciding twice
    first = _decide(used_today=123, reserve_calls=10)
    second = _decide(used_today=123, reserve_calls=10)

    # THEN both decisions are equal
    assert first == second


# --- decide: invariant over many input combinations ---

_LIMITS = [1, 2, 3, 7, 100, 999, 1000, 1001, 10_000]
_RATIOS = [0.01, 0.29, 0.5, 0.57, 0.7, 0.9, 0.95, 0.99, 1.0]
_ATTEMPTS = [1, 2, 3, 4, 5]
_RESERVES = [0, 1, 5, 160, 950, 2000]


def _exact_ceiling(limit: int, ratio: float, reserve: int) -> int:
    # Decimal-exact floor(limit * ratio) - reserve, independent of float rounding.
    return math.floor(limit * Fraction(str(ratio))) - reserve


def _usage_samples(limit: int, ceiling: int, attempts: int) -> list[int]:
    # Spread across the day plus every value next to the page/ceiling boundaries.
    near = [ceiling + d for d in range(-attempts - 1, 3)]
    spread = [int(limit * f) for f in (0.0, 0.1, 0.5, 0.9, 1.0, 1.5)]
    return sorted({u for u in near + spread if u >= 0})


def _grid(limit: int, ratio: float, attempts: int) -> list[tuple[int, int, BudgetDecision]]:
    out = []
    for reserve in _RESERVES:
        ceiling = _exact_ceiling(limit, ratio, reserve)
        for used in _usage_samples(limit, ceiling, attempts):
            decision = _decide(
                daily_limit=limit,
                safety_ratio=ratio,
                reserve_calls=reserve,
                used_today=used,
                max_attempts=attempts,
            )
            out.append((ceiling, used, decision))
    return out


@pytest.mark.parametrize(
    ("limit", "ratio", "attempts"), list(itertools.product(_LIMITS, _RATIOS, _ATTEMPTS))
)
def test_worst_case_usage_never_exceeds_the_ceiling(
    limit: int, ratio: float, attempts: int
) -> None:
    # GIVEN a limit/ratio/attempts combination, crossed with reserves and usage levels
    #       around every page and ceiling boundary
    # WHEN deciding for each
    grid = _grid(limit, ratio, attempts)

    # THEN even if every page uses every attempt, no run goes past the ceiling or the limit
    for ceiling, used, d in grid:
        ctx = f"ceiling={ceiling} used={used} decision={d}"
        if d.run:
            assert d.max_pages >= 1, ctx
            assert used + d.max_pages * attempts <= ceiling, ctx
            assert used + d.max_pages * attempts <= limit, ctx
        else:
            assert d.max_pages == 0, ctx
        assert d.usable_calls >= 0, ctx
        assert d.max_pages * attempts <= d.usable_calls, ctx


@pytest.mark.parametrize(
    ("limit", "ratio", "attempts"), list(itertools.product(_LIMITS, _RATIOS, _ATTEMPTS))
)
def test_page_cap_is_not_needlessly_conservative(limit: int, ratio: float, attempts: int) -> None:
    # GIVEN the same input grid
    # WHEN deciding for each
    grid = _grid(limit, ratio, attempts)

    # THEN one more page would not fit in the usable calls, and run matches pages >= 1
    for ceiling, used, d in grid:
        ctx = f"ceiling={ceiling} used={used} decision={d}"
        assert (d.max_pages + 1) * attempts > d.usable_calls, ctx
        assert d.run is (d.max_pages >= 1), ctx


# --- quota_day_bounds ---


def test_last_second_of_a_kst_day_belongs_to_that_day() -> None:
    # GIVEN 23:59:59 KST on 2026-10-27
    now = datetime(2026, 10, 27, 23, 59, 59, tzinfo=KST)

    # WHEN computing the quota day
    start, end = quota_day_bounds(now)

    # THEN it is [2026-10-27 00:00 KST, 2026-10-28 00:00 KST)
    assert start == datetime(2026, 10, 27, tzinfo=KST)
    assert end == datetime(2026, 10, 28, tzinfo=KST)


def test_kst_midnight_starts_a_new_quota_day() -> None:
    # GIVEN exactly 00:00 KST on 2026-10-28
    now = datetime(2026, 10, 28, 0, 0, tzinfo=KST)

    # WHEN computing the quota day
    start, end = quota_day_bounds(now)

    # THEN the new day starts at that instant (start inclusive)
    assert start == now
    assert end == datetime(2026, 10, 29, tzinfo=KST)


@pytest.mark.parametrize(
    ("now_utc", "expected_kst_day"),
    [
        (datetime(2026, 10, 27, 14, 59, 59, tzinfo=UTC), 27),  # 23:59:59 KST
        (datetime(2026, 10, 27, 15, 0, 0, tzinfo=UTC), 28),  # 00:00 KST next day
        (datetime(2026, 10, 27, 0, 0, 0, tzinfo=UTC), 27),  # 09:00 KST
    ],
)
def test_utc_input_is_bucketed_by_the_kst_calendar_day(
    now_utc: datetime, expected_kst_day: int
) -> None:
    # GIVEN an instant expressed in UTC
    # WHEN computing the quota day
    start, end = quota_day_bounds(now_utc)

    # THEN the bounds follow the KST calendar day, not the UTC one
    assert start == datetime(2026, 10, expected_kst_day, tzinfo=KST)
    assert end - start == timedelta(days=1)


def test_bounds_are_kst_aware_and_contain_now() -> None:
    # GIVEN an instant in an unrelated offset (UTC-5)
    now = datetime(2026, 10, 27, 12, 0, tzinfo=timezone(timedelta(hours=-5)))

    # WHEN computing the quota day
    start, end = quota_day_bounds(now)

    # THEN both bounds carry +09:00 and now falls in [start, end)
    assert start.utcoffset() == timedelta(hours=9)
    assert end.utcoffset() == timedelta(hours=9)
    assert start <= now < end
    assert start == datetime(2026, 10, 28, tzinfo=KST)  # 17:00Z -> 02:00 KST on the 28th


def test_quota_day_bounds_are_stable_across_repeat_calls() -> None:
    # GIVEN a fixed instant
    now = datetime(2026, 10, 27, 5, 30, tzinfo=KST)

    # WHEN computing the bounds twice
    # THEN both results are equal
    assert quota_day_bounds(now) == quota_day_bounds(now)


def test_naive_now_is_rejected() -> None:
    # GIVEN a naive datetime
    now = datetime(2026, 10, 27, 23, 59, 59)

    # WHEN / THEN computing the quota day raises
    with pytest.raises(ValueError):
        quota_day_bounds(now)
