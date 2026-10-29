"""fetch-delta job: flow outcome -> process exit code. The flow itself is tested in tests/db."""

import pytest

from eonje.cli import exit_codes
from eonje.cli.jobs.fetch_delta import exit_code
from eonje.collect.delta import DeltaResult, Outcome


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (Outcome.COLLECTED, exit_codes.OK),
        (Outcome.SKIPPED_BUDGET, exit_codes.SKIPPED),
    ],
)
def test_outcome_maps_to_exit_code(outcome: Outcome, expected: int) -> None:
    # GIVEN a flow result with the outcome
    result = DeltaResult(outcome=outcome, budget=None, manifest=None)
    # WHEN it is turned into an exit code
    # THEN the code tells Airflow to succeed or skip
    assert exit_code(result) == expected


def test_every_outcome_has_an_exit_code() -> None:
    # GIVEN every outcome the flow can return
    # WHEN / THEN each maps to a known exit code (a new outcome must be mapped deliberately)
    known = {exit_codes.OK, exit_codes.SKIPPED}
    for outcome in Outcome:
        assert exit_code(DeltaResult(outcome, None, None)) in known
