from datetime import datetime, timedelta

import pytest

from eonje.load.pending import pending_batches
from eonje.storage.postgres.watermark import Watermark
from eonje.storage.raw.run import MANIFEST_VERSION, RunManifest
from eonje.timeutil import KST

SETTLE = timedelta(minutes=15)
NOW = datetime.fromisoformat("2026-11-10T03:00:00+00:00")


def _at(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


def _manifest(collected_at: str, run_key: str | None = None) -> RunManifest:
    """A complete charger_status run. dt is the KST date of collected_at, as the writer sets it."""
    collected = _at(collected_at)
    return RunManifest(
        version=MANIFEST_VERSION,
        dataset="charger_status",
        operation="getChargerStatus",
        dt=collected.astimezone(KST).date().isoformat(),
        run_key=run_key or f"scheduled__{collected_at}",
        collected_at=collected_at,
        total_count=10,
        row_count=10,
        page_size=9999,
        expected_pages=1,
        pages=[1],
    )


def _keys(batches: list[list[RunManifest]]) -> list[list[str]]:
    return [[m.run_key for m in batch] for batch in batches]


# --- watermark ---


def test_without_a_watermark_every_settled_run_is_returned() -> None:
    # GIVEN three settled runs on one KST date and no watermark
    runs = [
        _manifest("2026-11-10T01:00:00+00:00"),
        _manifest("2026-11-10T01:10:00+00:00"),
        _manifest("2026-11-10T01:20:00+00:00"),
    ]

    # WHEN picking pending runs
    batches = pending_batches(runs, watermark=None, now=NOW, settle=SETTLE)

    # THEN all of them are returned
    assert _keys(batches) == [[m.run_key for m in runs]]


def test_runs_at_or_before_the_watermark_are_excluded() -> None:
    # GIVEN a watermark at the second of three runs
    runs = [
        _manifest("2026-11-10T01:00:00+00:00"),
        _manifest("2026-11-10T01:10:00+00:00"),
        _manifest("2026-11-10T01:20:00+00:00"),
    ]
    watermark = Watermark(runs[1].collected, runs[1].run_key)

    # WHEN picking pending runs
    batches = pending_batches(runs, watermark=watermark, now=NOW, settle=SETTLE)

    # THEN only the run after the watermark remains
    assert _keys(batches) == [[runs[2].run_key]]


def test_runs_with_the_watermark_collected_at_are_compared_by_run_key() -> None:
    # GIVEN three runs collected at the same instant as the watermark
    at = "2026-11-10T01:10:00+00:00"
    before = _manifest(at, run_key="manual__a")
    same = _manifest(at, run_key="manual__b")
    after = _manifest(at, run_key="manual__c")
    watermark = Watermark(_at(at), "manual__b")

    # WHEN picking pending runs
    batches = pending_batches([before, same, after], watermark=watermark, now=NOW, settle=SETTLE)

    # THEN only the run whose run_key sorts after the watermark's is returned
    assert _keys(batches) == [["manual__c"]]


# --- settle ---


def test_run_collected_exactly_at_now_minus_settle_is_included() -> None:
    # GIVEN a run collected exactly at the settle cutoff
    run = _manifest((NOW - SETTLE).isoformat())

    # WHEN picking pending runs
    batches = pending_batches([run], watermark=None, now=NOW, settle=SETTLE)

    # THEN it counts as settled
    assert _keys(batches) == [[run.run_key]]


def test_run_collected_after_now_minus_settle_is_excluded() -> None:
    # GIVEN a settled run and one collected a second after the cutoff
    settled = _manifest((NOW - SETTLE).isoformat())
    unsettled = _manifest((NOW - SETTLE + timedelta(seconds=1)).isoformat())

    # WHEN picking pending runs
    batches = pending_batches([settled, unsettled], watermark=None, now=NOW, settle=SETTLE)

    # THEN the unsettled run is left for a later pass
    assert _keys(batches) == [[settled.run_key]]


# --- batching by dt ---


def test_runs_on_the_same_kst_date_form_one_batch_across_utc_midnight() -> None:
    # GIVEN two runs on either side of UTC midnight, both on KST date 2026-11-10
    runs = [
        _manifest("2026-11-09T23:50:00+00:00"),
        _manifest("2026-11-10T00:10:00+00:00"),
    ]

    # WHEN picking pending runs
    batches = pending_batches(runs, watermark=None, now=NOW, settle=SETTLE)

    # THEN they form a single batch
    assert _keys(batches) == [[m.run_key for m in runs]]


def test_crossing_kst_midnight_starts_a_new_batch() -> None:
    # GIVEN runs at 23:50 and 23:59:30 KST on 2026-11-09 and at 00:00:30 KST on 2026-11-10
    runs = [
        _manifest("2026-11-09T14:50:00+00:00"),
        _manifest("2026-11-09T14:59:30+00:00"),
        _manifest("2026-11-09T15:00:30+00:00"),
    ]

    # WHEN picking pending runs
    batches = pending_batches(runs, watermark=None, now=NOW, settle=SETTLE)

    # THEN the date change splits them into two batches, each holding one dt
    assert _keys(batches) == [[runs[0].run_key, runs[1].run_key], [runs[2].run_key]]
    assert [{m.dt for m in batch} for batch in batches] == [{"2026-11-09"}, {"2026-11-10"}]


# --- ordering ---


def test_batches_and_runs_follow_collected_at_then_run_key_for_shuffled_input() -> None:
    # GIVEN runs over two KST dates, two of them sharing a collected_at, given in shuffled order
    d1_early = _manifest("2026-11-09T14:00:00+00:00", run_key="scheduled__d1_early")
    d1_tie_a = _manifest("2026-11-09T14:30:00+00:00", run_key="manual__a")
    d1_tie_b = _manifest("2026-11-09T14:30:00+00:00", run_key="manual__b")
    d2_early = _manifest("2026-11-09T15:10:00+00:00", run_key="scheduled__d2_early")
    d2_late = _manifest("2026-11-09T15:20:00+00:00", run_key="scheduled__d2_late")
    shuffled = [d2_late, d1_tie_b, d2_early, d1_early, d1_tie_a]

    # WHEN picking pending runs
    batches = pending_batches(shuffled, watermark=None, now=NOW, settle=SETTLE)

    # THEN batches go oldest date first and runs within a batch by (collected_at, run_key)
    assert _keys(batches) == [
        ["scheduled__d1_early", "manual__a", "manual__b"],
        ["scheduled__d2_early", "scheduled__d2_late"],
    ]


# --- nothing to do ---


def test_no_manifests_yield_an_empty_list() -> None:
    # GIVEN no runs at all
    # WHEN picking pending runs
    batches = pending_batches([], watermark=None, now=NOW, settle=SETTLE)

    # THEN there is nothing to process
    assert batches == []


def test_only_processed_or_unsettled_runs_yield_an_empty_list() -> None:
    # GIVEN one run at the watermark and one not yet settled
    processed = _manifest("2026-11-10T01:00:00+00:00")
    unsettled = _manifest("2026-11-10T02:50:00+00:00")
    watermark = Watermark(processed.collected, processed.run_key)

    # WHEN picking pending runs
    batches = pending_batches([processed, unsettled], watermark=watermark, now=NOW, settle=SETTLE)

    # THEN there is nothing to process
    assert batches == []


# --- contract ---


def test_naive_now_is_rejected() -> None:
    # GIVEN a now without a timezone
    naive_now = datetime(2026, 11, 10, 3, 0)

    # WHEN / THEN picking pending runs raises ValueError
    with pytest.raises(ValueError, match="now"):
        pending_batches([], watermark=None, now=naive_now, settle=SETTLE)
