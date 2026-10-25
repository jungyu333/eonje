from collections.abc import Callable
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "evcharger"


@pytest.fixture
def fixture_text() -> Callable[[str], str]:
    """Load an EvCharger response fixture as raw text (cwd-independent)."""

    def load(name: str) -> str:
        return (FIXTURES / name).read_text(encoding="utf-8")

    return load
