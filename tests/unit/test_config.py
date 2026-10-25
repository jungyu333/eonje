from pathlib import Path

import pytest
from pydantic import ValidationError

from eonje.config import Settings, get_settings


def test_missing_api_server_key_is_rejected() -> None:
    # GIVEN no EONJE_API_SERVER_KEY in the environment or .env
    # WHEN settings are loaded
    # THEN validation fails on the key field
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "api_server_key" in str(exc.value)


def test_defaults_apply_when_only_the_key_is_set(api_key_env: str) -> None:
    # GIVEN only the required key is set
    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN every other field takes its documented default
    assert settings.api_server_key.get_secret_value() == api_key_env
    assert settings.api_base_url == "https://apis.data.go.kr/B552584/EvCharger"
    assert settings.api_daily_limit == 1000
    assert settings.page_size == 9999
    assert settings.status_period_min == 10
    assert settings.status_interval_min == 10
    assert settings.serve_zcodes == []


def test_values_are_read_from_prefixed_env_vars(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN overrides under the EONJE_ prefix
    monkeypatch.setenv("EONJE_API_DAILY_LIMIT", "500")
    monkeypatch.setenv("EONJE_PAGE_SIZE", "100")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN the overrides are applied and typed
    assert settings.api_daily_limit == 500
    assert settings.page_size == 100


def test_unprefixed_env_vars_are_ignored(api_key_env: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # GIVEN a limit set without the EONJE_ prefix
    monkeypatch.setenv("API_DAILY_LIMIT", "5")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN the default is kept
    assert settings.api_daily_limit == 1000


def test_serve_zcodes_are_parsed_from_a_json_list(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN served regions as a JSON list of zcode strings
    monkeypatch.setenv("EONJE_SERVE_ZCODES", '["11", "26"]')

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN zcodes stay strings (leading digits preserved)
    assert settings.serve_zcodes == ["11", "26"]


def test_unknown_prefixed_env_var_is_ignored(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN an EONJE_ variable that is not a field
    monkeypatch.setenv("EONJE_NOT_A_FIELD", "x")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN loading succeeds and the value is not attached
    assert not hasattr(settings, "not_a_field")


def test_values_are_read_from_dotenv_in_cwd(tmp_path: Path) -> None:
    # GIVEN a .env file in the working directory (cwd is tmp_path via conftest)
    (tmp_path / ".env").write_text(
        "EONJE_API_SERVER_KEY=from-dotenv\nEONJE_API_DAILY_LIMIT=42\n", encoding="utf-8"
    )

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN values come from the file
    assert settings.api_server_key.get_secret_value() == "from-dotenv"
    assert settings.api_daily_limit == 42


def test_api_server_key_is_masked_in_repr(api_key_env: str) -> None:
    # GIVEN loaded settings
    settings = Settings()  # type: ignore[call-arg]

    # WHEN the settings object is rendered (e.g. in a log line)
    rendered = repr(settings) + str(settings)

    # THEN the key does not leak
    assert api_key_env not in rendered


@pytest.mark.parametrize("value", ["0", "-1"])
def test_non_positive_daily_limit_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a daily limit that would allow no calls or be meaningless
    monkeypatch.setenv("EONJE_API_DAILY_LIMIT", value)

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


@pytest.mark.parametrize("value", ["10", "10000"])
def test_page_size_outside_bounds_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a page size at or below 10, or above the API max 9999
    monkeypatch.setenv("EONJE_PAGE_SIZE", value)

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


@pytest.mark.parametrize("value", ["11", "9999"])
def test_page_size_within_bounds_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a page size at the inclusive edges of the valid range
    monkeypatch.setenv("EONJE_PAGE_SIZE", value)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is accepted as-is
    assert settings.page_size == int(value)


@pytest.mark.parametrize("value", ["1", "11"])
def test_status_period_outside_bounds_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a status lookback period outside (1, 10]
    monkeypatch.setenv("EONJE_STATUS_PERIOD_MIN", value)

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_status_interval_of_zero_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a zero collection interval
    monkeypatch.setenv("EONJE_STATUS_INTERVAL_MIN", "0")

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_get_settings_returns_the_same_instance_on_repeat_calls(api_key_env: str) -> None:
    # GIVEN valid environment
    # WHEN get_settings is called twice
    first = get_settings()
    second = get_settings()

    # THEN the cached instance is reused
    assert first is second
