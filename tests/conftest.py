from collections.abc import Iterator

import pytest
from fsspec.implementations.memory import MemoryFileSystem


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
