import pytest


def test_tests_under_tests_db_get_the_db_marker_automatically(
    request: pytest.FixtureRequest,
) -> None:
    # GIVEN a test module under tests/db that sets no marker itself
    # WHEN it is collected
    # THEN the conftest hook has attached the db marker
    assert request.node.get_closest_marker("db") is not None
