from datetime import UTC, datetime, timedelta, timezone, tzinfo

import pytest

from eonje.timeutil import KST, require_aware


class _OffsetlessTz(tzinfo):
    """A tzinfo that is attached but has no offset, so the datetime is effectively naive."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        return None

    def tzname(self, dt: datetime | None) -> str | None:
        return None


def test_kst_is_fixed_utc_plus_nine() -> None:
    # GIVEN an instant in KST
    value = datetime(2026, 10, 27, 0, 0, tzinfo=KST)

    # WHEN converting it to UTC
    as_utc = value.astimezone(UTC)

    # THEN it is 9 hours earlier, on the previous UTC day
    assert KST.utcoffset(None) == timedelta(hours=9)
    assert as_utc == datetime(2026, 10, 26, 15, 0, tzinfo=UTC)


def test_kst_has_kst_name() -> None:
    # GIVEN / WHEN asking the zone for its name
    name = datetime(2026, 10, 27, tzinfo=KST).tzname()

    # THEN it is "KST"
    assert name == "KST"


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 10, 26, 10, 10, tzinfo=UTC),
        datetime(2026, 10, 26, 19, 10, tzinfo=KST),
        datetime(2026, 10, 26, 19, 10, tzinfo=timezone(timedelta(hours=-5))),
    ],
    ids=["utc", "kst", "negative-offset"],
)
def test_require_aware_accepts_aware_datetimes(value: datetime) -> None:
    # GIVEN a timezone-aware datetime
    # WHEN / THEN validation passes and returns nothing
    assert require_aware("collected_at", value) is None


def test_require_aware_rejects_naive_datetime() -> None:
    # GIVEN a naive datetime (e.g. KST local time without tzinfo)
    value = datetime(2026, 10, 26, 19, 10)

    # WHEN / THEN it is rejected
    with pytest.raises(ValueError, match="must be timezone-aware"):
        require_aware("collected_at", value)


def test_require_aware_error_names_the_offending_field() -> None:
    # GIVEN a naive datetime passed under a specific field name
    value = datetime(2026, 10, 26, 19, 10)

    # WHEN validation fails
    with pytest.raises(ValueError) as exc_info:
        require_aware("page.fetched_at", value)

    # THEN the message names the field and shows the value
    message = str(exc_info.value)
    assert message.startswith("page.fetched_at ")
    assert repr(value) in message


def test_require_aware_rejects_tzinfo_without_offset() -> None:
    # GIVEN a datetime with a tzinfo whose utcoffset() is None
    value = datetime(2026, 10, 26, 19, 10, tzinfo=_OffsetlessTz())

    # WHEN / THEN it is treated as naive and rejected
    with pytest.raises(ValueError, match="must be timezone-aware"):
        require_aware("called_at", value)


def test_require_aware_is_repeatable_on_same_input() -> None:
    # GIVEN an aware datetime
    value = datetime(2026, 10, 26, 10, 10, tzinfo=UTC)

    # WHEN validating it twice
    first = require_aware("start", value)
    second = require_aware("start", value)

    # THEN both calls pass and the value is not modified
    assert first is None and second is None
    assert value == datetime(2026, 10, 26, 10, 10, tzinfo=UTC)
