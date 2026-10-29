from pathlib import Path

import pytest
from pydantic import ValidationError

from eonje.config import DatabaseSettings, Settings, get_database_settings, get_settings


def test_missing_api_service_key_is_rejected() -> None:
    # GIVEN no EONJE_API_SERVICE_KEY in the environment or .env
    # WHEN settings are loaded
    # THEN validation fails on the key field
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "api_service_key" in str(exc.value)


def test_defaults_apply_when_only_the_key_is_set(api_key_env: str) -> None:
    # GIVEN only the required key is set
    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN every other field takes its documented default
    assert settings.api_service_key.get_secret_value() == api_key_env
    assert settings.api_base_url == "https://apis.data.go.kr/B552584/EvCharger"
    assert settings.api_daily_limit == 1000
    assert settings.page_size == 9999
    assert settings.api_timeout_sec == 30.0
    assert settings.api_max_attempts == 3
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
        "EONJE_API_SERVICE_KEY=from-dotenv\nEONJE_API_DAILY_LIMIT=42\n", encoding="utf-8"
    )

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN values come from the file
    assert settings.api_service_key.get_secret_value() == "from-dotenv"
    assert settings.api_daily_limit == 42


def test_api_service_key_is_masked_in_repr(api_key_env: str) -> None:
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


@pytest.mark.parametrize("value", ["0", "11"])
def test_status_period_outside_bounds_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a status lookback period outside the API range [1, 10]
    monkeypatch.setenv("EONJE_STATUS_PERIOD_MIN", value)

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


@pytest.mark.parametrize("value", ["1", "10"])
def test_status_period_within_bounds_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a status lookback period at the inclusive edges of the API range
    monkeypatch.setenv("EONJE_STATUS_PERIOD_MIN", value)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is accepted as-is
    assert settings.status_period_min == int(value)


def test_status_interval_of_zero_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a zero collection interval
    monkeypatch.setenv("EONJE_STATUS_INTERVAL_MIN", "0")

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_api_timeout_and_attempts_are_read_from_prefixed_env_vars(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN HTTP timeout and retry overrides under the EONJE_ prefix
    monkeypatch.setenv("EONJE_API_TIMEOUT_SEC", "2.5")
    monkeypatch.setenv("EONJE_API_MAX_ATTEMPTS", "5")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN the overrides are applied and typed
    assert settings.api_timeout_sec == 2.5
    assert settings.api_max_attempts == 5


def test_integer_api_timeout_from_env_example_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN the timeout written as an integer, as in infra/.env.example
    monkeypatch.setenv("EONJE_API_TIMEOUT_SEC", "30")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is coerced to a float
    assert settings.api_timeout_sec == 30.0
    assert isinstance(settings.api_timeout_sec, float)


@pytest.mark.parametrize("value", ["0", "-1", "-0.5"])
def test_non_positive_api_timeout_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN an HTTP timeout that would fail every request immediately or be meaningless
    monkeypatch.setenv("EONJE_API_TIMEOUT_SEC", value)

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_small_positive_api_timeout_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a fractional timeout just above zero
    monkeypatch.setenv("EONJE_API_TIMEOUT_SEC", "0.1")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is accepted as-is
    assert settings.api_timeout_sec == 0.1


@pytest.mark.parametrize("value", ["0", "6"])
def test_api_max_attempts_outside_bounds_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN zero attempts (no call at all) or more than 5 (budget burn on retries)
    monkeypatch.setenv("EONJE_API_MAX_ATTEMPTS", value)

    # WHEN / THEN loading fails
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


@pytest.mark.parametrize("value", ["1", "5"])
def test_api_max_attempts_within_bounds_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN an attempt count at the inclusive edges of [1, 5]
    monkeypatch.setenv("EONJE_API_MAX_ATTEMPTS", value)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is accepted as-is
    assert settings.api_max_attempts == int(value)


def test_fractional_api_max_attempts_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a non-integer attempt count
    monkeypatch.setenv("EONJE_API_MAX_ATTEMPTS", "2.5")

    # WHEN / THEN loading fails instead of silently truncating
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_budget_defaults_apply_when_only_the_key_is_set(api_key_env: str) -> None:
    # GIVEN only the required key is set
    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN the budget fields take their documented defaults
    assert settings.budget_safety_ratio == 0.95
    assert settings.budget_reserve_calls == 0


@pytest.mark.parametrize("value", ["0", "-0.1", "1.01", "2"])
def test_budget_safety_ratio_outside_bounds_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a ratio that allows no calls or permits exceeding the daily limit
    monkeypatch.setenv("EONJE_BUDGET_SAFETY_RATIO", value)

    # WHEN / THEN loading fails on the ratio field
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "budget_safety_ratio" in str(exc.value)


@pytest.mark.parametrize("value", ["0.01", "0.95", "1", "1.0"])
def test_budget_safety_ratio_within_bounds_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a ratio in (0, 1], including the inclusive upper edge
    monkeypatch.setenv("EONJE_BUDGET_SAFETY_RATIO", value)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is accepted as a float
    assert settings.budget_safety_ratio == float(value)


@pytest.mark.parametrize("value", ["-1", "-160"])
def test_negative_budget_reserve_calls_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a negative reserve, which would raise the ceiling above limit x ratio
    monkeypatch.setenv("EONJE_BUDGET_RESERVE_CALLS", value)

    # WHEN / THEN loading fails on the reserve field
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "budget_reserve_calls" in str(exc.value)


@pytest.mark.parametrize("value", ["0", "160"])
def test_non_negative_budget_reserve_calls_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN no reserve (the default and the .env.example value) or a positive reserve
    monkeypatch.setenv("EONJE_BUDGET_RESERVE_CALLS", value)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is accepted as-is
    assert settings.budget_reserve_calls == int(value)


def test_fractional_budget_reserve_calls_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a non-integer reserve
    monkeypatch.setenv("EONJE_BUDGET_RESERVE_CALLS", "1.5")

    # WHEN / THEN loading fails instead of silently truncating
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_get_settings_returns_the_same_instance_on_repeat_calls(api_key_env: str) -> None:
    # GIVEN valid environment
    # WHEN get_settings is called twice
    first = get_settings()
    second = get_settings()

    # THEN the cached instance is reused
    assert first is second


ENV_EXAMPLE = Path(__file__).resolve().parents[2] / "infra" / ".env.example"


def test_storage_defaults_apply_when_only_the_key_is_set(api_key_env: str) -> None:
    # GIVEN only the required API key is set
    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN storage fields take their documented local-dev defaults
    assert settings.raw_bucket == "eonje-raw"
    assert settings.s3_endpoint_url == "http://127.0.0.1:8333"
    assert settings.s3_access_key is None
    assert settings.s3_secret_key is None
    assert (
        settings.database_url.get_secret_value()
        == "postgresql+psycopg://eonje:eonje@localhost:5432/eonje"
    )


def test_storage_values_are_read_from_prefixed_env_vars(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN storage overrides under the EONJE_ prefix
    monkeypatch.setenv("EONJE_RAW_BUCKET", "eonje-raw-test")
    monkeypatch.setenv("EONJE_S3_ENDPOINT_URL", "http://seaweedfs:8333")
    monkeypatch.setenv("EONJE_S3_ACCESS_KEY", "access-id")
    monkeypatch.setenv("EONJE_S3_SECRET_KEY", "secret-value")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN the overrides are applied, with credentials wrapped as secrets
    assert settings.raw_bucket == "eonje-raw-test"
    assert settings.s3_endpoint_url == "http://seaweedfs:8333"
    assert settings.s3_access_key is not None
    assert settings.s3_access_key.get_secret_value() == "access-id"
    assert settings.s3_secret_key is not None
    assert settings.s3_secret_key.get_secret_value() == "secret-value"


@pytest.mark.parametrize(
    "value",
    [
        "",
        "ab",
        "s3a://eonje-raw",
        "s3://eonje-raw",
        "Eonje-raw",
        "eonje_raw",
        "-eonje-raw",
        "eonje-raw-",
        "a" * 64,
    ],
)
def test_invalid_raw_bucket_name_is_rejected(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a value that is not an S3 bucket name (too short/long, URL scheme, case, symbols)
    monkeypatch.setenv("EONJE_RAW_BUCKET", value)

    # WHEN / THEN loading fails on the bucket field
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "raw_bucket" in str(exc.value)


@pytest.mark.parametrize("value", ["abc", "eonje-raw", "eonje.raw.2026", "a" * 63])
def test_valid_raw_bucket_name_is_accepted(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN a valid S3 bucket name, including the 3- and 63-char edges
    monkeypatch.setenv("EONJE_RAW_BUCKET", value)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN it is accepted as-is
    assert settings.raw_bucket == value


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_s3_credentials_are_treated_as_unset(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # GIVEN S3 credential variables present but blank, as in an unfilled .env
    monkeypatch.setenv("EONJE_S3_ACCESS_KEY", value)
    monkeypatch.setenv("EONJE_S3_SECRET_KEY", value)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN both are None, not an empty secret
    assert settings.s3_access_key is None
    assert settings.s3_secret_key is None


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_api_service_key_is_rejected(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    # GIVEN the API key variable present but empty
    monkeypatch.setenv("EONJE_API_SERVICE_KEY", value)

    # WHEN / THEN loading fails at startup, not at the first API call
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "api_service_key" in str(exc.value)


def test_s3_credentials_are_masked_in_repr(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN S3 credentials in the environment
    monkeypatch.setenv("EONJE_S3_ACCESS_KEY", "access-id-should-not-leak")
    monkeypatch.setenv("EONJE_S3_SECRET_KEY", "secret-should-not-leak")
    settings = Settings()  # type: ignore[call-arg]

    # WHEN the settings object is rendered (e.g. in a log line)
    rendered = repr(settings) + str(settings)

    # THEN neither credential leaks
    assert "access-id-should-not-leak" not in rendered
    assert "secret-should-not-leak" not in rendered


def test_legacy_raw_root_env_var_does_not_break_loading(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a node .env still carrying the removed EONJE_RAW_ROOT
    monkeypatch.setenv("EONJE_RAW_ROOT", "s3a://eonje-raw")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN loading succeeds, the stale value is not attached, and the bucket default holds
    assert not hasattr(settings, "raw_root")
    assert settings.raw_bucket == "eonje-raw"


def test_env_example_with_only_the_api_key_filled_loads(api_key_env: str, tmp_path: Path) -> None:
    # GIVEN infra/.env.example copied verbatim as the .env in cwd, with the API key supplied
    (tmp_path / ".env").write_text(ENV_EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN every example value validates, the quoted endpoint is unquoted,
    # and the unfilled S3 credentials count as unset
    assert settings.raw_bucket == "eonje-raw"
    assert settings.s3_endpoint_url == "http://seaweedfs:8333"
    assert settings.serve_zcodes == ["11"]
    assert settings.s3_access_key is None
    assert settings.s3_secret_key is None


def test_env_example_without_filling_the_api_key_is_rejected(tmp_path: Path) -> None:
    # GIVEN infra/.env.example copied verbatim, API key left blank
    (tmp_path / ".env").write_text(ENV_EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8")

    # WHEN / THEN loading fails, pointing at the key that must be filled in
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "api_service_key" in str(exc.value)


def test_loading_storage_settings_twice_yields_equal_values(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a fixed storage environment
    monkeypatch.setenv("EONJE_RAW_BUCKET", "eonje-raw-test")
    monkeypatch.setenv("EONJE_S3_SECRET_KEY", "secret-value")

    # WHEN settings are loaded twice, independently
    first = Settings()  # type: ignore[call-arg]
    second = Settings()  # type: ignore[call-arg]

    # THEN both loads agree
    assert first == second


def test_database_url_is_read_from_prefixed_env_var(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a database URL under the EONJE_ prefix
    url = "postgresql+psycopg://eonje:pw@postgres:5432/eonje"
    monkeypatch.setenv("EONJE_DATABASE_URL", url)

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN the URL is wrapped as a secret holding the exact value
    assert settings.database_url.get_secret_value() == url


def test_database_url_password_is_masked_in_repr(
    api_key_env: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # GIVEN a database URL carrying a password
    monkeypatch.setenv(
        "EONJE_DATABASE_URL", "postgresql+psycopg://eonje:db-pw-should-not-leak@postgres/eonje"
    )
    settings = Settings()  # type: ignore[call-arg]

    # WHEN the settings object is rendered (e.g. in a log line)
    rendered = repr(settings) + str(settings)

    # THEN the password does not leak
    assert "db-pw-should-not-leak" not in rendered


def test_env_example_database_url_is_loaded(api_key_env: str, tmp_path: Path) -> None:
    # GIVEN infra/.env.example copied verbatim as the .env in cwd
    (tmp_path / ".env").write_text(ENV_EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8")

    # WHEN settings are loaded
    settings = Settings()  # type: ignore[call-arg]

    # THEN the example database URL points at the compose postgres service
    assert (
        settings.database_url.get_secret_value()
        == "postgresql+psycopg://eonje:CHANGE_ME@postgres:5432/eonje"
    )


def test_database_settings_load_without_an_api_key() -> None:
    # GIVEN no EONJE_API_SERVICE_KEY and no EONJE_DATABASE_URL
    # WHEN database-only settings are loaded
    settings = DatabaseSettings()

    # THEN loading succeeds with the local-dev default URL
    assert (
        settings.database_url.get_secret_value()
        == "postgresql+psycopg://eonje:eonje@localhost:5432/eonje"
    )


def test_database_url_env_var_is_seen_identically_by_both_settings_classes(
    api_key_env: str, database_url_env: str
) -> None:
    # GIVEN EONJE_DATABASE_URL set (and the API key, so Settings can load)
    # WHEN both settings classes are loaded
    db_settings = DatabaseSettings()
    settings = Settings()  # type: ignore[call-arg]

    # THEN both hold the same URL
    assert db_settings.database_url.get_secret_value() == database_url_env
    assert settings.database_url.get_secret_value() == database_url_env


def test_database_settings_repr_does_not_leak_the_password(database_url_env: str) -> None:
    # GIVEN database settings whose URL carries a password
    settings = DatabaseSettings()

    # WHEN the settings object is rendered (e.g. in a log line)
    rendered = repr(settings) + str(settings)

    # THEN the password does not leak
    assert "db-pw-should-not-leak" not in rendered


def test_get_database_settings_returns_the_same_instance_on_repeat_calls() -> None:
    # GIVEN no API key in the environment
    # WHEN get_database_settings is called twice
    first = get_database_settings()
    second = get_database_settings()

    # THEN the cached instance is reused
    assert first is second


def test_settings_still_require_the_api_key_after_the_database_split(
    database_url_env: str,
) -> None:
    # GIVEN a database URL but no API key
    # WHEN the full settings are loaded
    # THEN validation still fails on the key field
    with pytest.raises(ValidationError) as exc:
        Settings()  # type: ignore[call-arg]
    assert "api_service_key" in str(exc.value)
