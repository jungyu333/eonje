from collections.abc import Iterator
from datetime import date

import pytest
from fsspec.implementations.memory import MemoryFileSystem

from eonje.storage.raw_paths import Dataset, RawRun


@pytest.fixture
def mem_fs() -> Iterator[MemoryFileSystem]:
    fs = MemoryFileSystem()
    _reset(fs)
    yield fs
    _reset(fs)


def _reset(fs: MemoryFileSystem) -> None:
    """The store and pseudo dirs are shared at class level; reset both around each test."""
    fs.store.clear()
    fs.pseudo_dirs.clear()
    fs.pseudo_dirs.append("")


@pytest.fixture
def run() -> RawRun:
    return RawRun(dataset=Dataset.CHARGER_STATUS, dt=date(2026, 10, 26), run_key="20261026T101000")
