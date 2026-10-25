import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from eonje.config import get_settings


@pytest.fixture(autouse=True)
def isolated_settings_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Keep unit tests independent of the developer's shell env and local .env file."""
    for name in list(os.environ):
        if name.upper().startswith("EONJE_"):
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)  # Settings reads ".env" relative to cwd
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def api_key_env(monkeypatch: pytest.MonkeyPatch) -> str:
    key = "test-service-key"
    monkeypatch.setenv("EONJE_API_SERVICE_KEY", key)
    return key
