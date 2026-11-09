"""Unit tests for eonje.collect.master.master_spec: what a master run asks the shared flow for."""

from datetime import timedelta
from typing import Any

import pytest

from eonje.collect.master import master_spec
from eonje.config import Settings
from eonje.storage.raw.paths import Dataset


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {"api_service_key": "test"}
    return Settings(**{**base, **overrides})


def test_master_spec_calls_get_charger_info() -> None:
    # GIVEN default settings
    settings = _settings()

    # WHEN the master spec is built
    spec = master_spec(settings)

    # THEN it targets the charger master operation, not the status delta one
    assert spec.operation == "getChargerInfo"


def test_master_spec_stores_into_the_charger_info_dataset() -> None:
    # GIVEN default settings
    settings = _settings()

    # WHEN the master spec is built
    spec = master_spec(settings)

    # THEN the raw run lands in the charger_info dataset
    assert spec.dataset is Dataset.CHARGER_INFO


@pytest.mark.parametrize("period", [1, 10])
def test_master_spec_sends_no_extra_params_whatever_the_status_period(period: int) -> None:
    # GIVEN a status period setting, which only the delta collection sends
    settings = _settings(status_period_min=period)

    # WHEN the master spec is built
    spec = master_spec(settings)

    # THEN it asks for the whole master with no filter or period param
    assert dict(spec.params) == {}


def test_master_spec_uses_the_configured_master_max_lag() -> None:
    # GIVEN a master lag allowance that differs from the delta fetch lag
    settings = _settings(master_max_lag_min=90, status_period_min=10, fetch_max_lag_min=5)

    # WHEN the master spec is built
    spec = master_spec(settings)

    # THEN its max lag is the master one, not the delta one
    assert spec.max_lag == timedelta(minutes=90)
    assert spec.max_lag == settings.master_max_lag
    assert spec.max_lag != settings.fetch_max_lag


def test_master_spec_max_lag_defaults_to_sixty_minutes() -> None:
    # GIVEN no master lag override (delta fetch lag defaults to the 10-minute status period)
    settings = _settings()

    # WHEN the master spec is built
    spec = master_spec(settings)

    # THEN the master default of 60 minutes applies
    assert spec.max_lag == timedelta(minutes=60)


def test_master_spec_is_the_same_on_every_call() -> None:
    # GIVEN one settings object
    settings = _settings(master_max_lag_min=45)

    # WHEN the master spec is built twice
    first = master_spec(settings)
    second = master_spec(settings)

    # THEN both specs are equal
    assert first == second
