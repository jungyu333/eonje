from typing import Any

import pytest

from eonje.config import Settings
from eonje.storage.fs import raw_filesystem

ACCESS_KEY = "test-access-key"
SECRET_KEY = "test-secret-key"
ENDPOINT = "http://seaweedfs.test:8333"


def _settings(access_key: str | None = ACCESS_KEY, secret_key: str | None = SECRET_KEY) -> Settings:
    return Settings(
        api_service_key="test-service-key",  # type: ignore[arg-type]
        s3_endpoint_url=ENDPOINT,
        s3_access_key=access_key,  # type: ignore[arg-type]
        s3_secret_key=secret_key,  # type: ignore[arg-type]
    )


def test_credentials_are_passed_as_plain_strings(captured: dict[str, Any]) -> None:
    # GIVEN settings with both S3 credentials
    settings = _settings()

    # WHEN the filesystem is built
    raw_filesystem(settings)

    # THEN s3fs receives the unwrapped secret values, not SecretStr
    assert captured["key"] == ACCESS_KEY
    assert captured["secret"] == SECRET_KEY


def test_endpoint_comes_from_settings(captured: dict[str, Any]) -> None:
    # GIVEN settings pointing at an S3 endpoint
    settings = _settings()

    # WHEN the filesystem is built
    raw_filesystem(settings)

    # THEN s3fs is pointed at that endpoint
    assert captured["endpoint_url"] == ENDPOINT


def test_path_style_addressing_is_used(captured: dict[str, Any]) -> None:
    # GIVEN valid settings
    # WHEN the filesystem is built
    raw_filesystem(_settings())

    # THEN path-style addressing is requested (SeaweedFS serves buckets by path, not virtual host)
    assert captured["config_kwargs"] == {"s3": {"addressing_style": "path"}}


def test_listing_and_instance_caches_are_disabled(captured: dict[str, Any]) -> None:
    # GIVEN valid settings
    # WHEN the filesystem is built
    raw_filesystem(_settings())

    # THEN no stale listing or shared instance can mask a rerun's overwrite
    assert captured["use_listings_cache"] is False
    assert captured["skip_instance_cache"] is True


def test_building_twice_passes_identical_arguments(captured: dict[str, Any]) -> None:
    # GIVEN the same settings
    settings = _settings()

    # WHEN the filesystem is built twice
    raw_filesystem(settings)
    first = dict(captured)
    captured.clear()
    raw_filesystem(settings)

    # THEN both calls configure s3fs identically
    assert captured == first


def test_missing_access_key_is_rejected_by_env_name(captured: dict[str, Any]) -> None:
    # GIVEN no access key
    settings = _settings(access_key=None)

    # WHEN / THEN building fails naming the env var to set
    with pytest.raises(ValueError, match="EONJE_S3_ACCESS_KEY") as exc:
        raw_filesystem(settings)
    assert "EONJE_S3_SECRET_KEY" not in str(exc.value)


def test_missing_secret_key_is_rejected_by_env_name(captured: dict[str, Any]) -> None:
    # GIVEN no secret key
    settings = _settings(secret_key=None)

    # WHEN / THEN building fails naming the env var to set
    with pytest.raises(ValueError, match="EONJE_S3_SECRET_KEY") as exc:
        raw_filesystem(settings)
    assert "EONJE_S3_ACCESS_KEY" not in str(exc.value)


def test_both_missing_keys_are_reported_together(captured: dict[str, Any]) -> None:
    # GIVEN neither credential
    settings = _settings(access_key=None, secret_key=None)

    # WHEN / THEN one error lists both env vars
    with pytest.raises(ValueError, match="EONJE_S3_ACCESS_KEY, EONJE_S3_SECRET_KEY"):
        raw_filesystem(settings)


def test_blank_credentials_count_as_missing(captured: dict[str, Any]) -> None:
    # GIVEN credentials left blank as in .env.example
    settings = _settings(access_key="", secret_key="  ")

    # WHEN / THEN building fails instead of connecting with empty keys
    with pytest.raises(ValueError, match="EONJE_S3_ACCESS_KEY, EONJE_S3_SECRET_KEY"):
        raw_filesystem(settings)


def test_missing_credentials_never_construct_a_filesystem(captured: dict[str, Any]) -> None:
    # GIVEN no secret key
    settings = _settings(secret_key=None)

    # WHEN building fails
    with pytest.raises(ValueError):
        raw_filesystem(settings)

    # THEN s3fs was never called
    assert captured == {}


def test_error_message_does_not_leak_the_present_credential(captured: dict[str, Any]) -> None:
    # GIVEN the access key is set but the secret is missing
    settings = _settings(secret_key=None)

    # WHEN / THEN the error does not echo the configured key value
    with pytest.raises(ValueError) as exc:
        raw_filesystem(settings)
    assert ACCESS_KEY not in str(exc.value)
