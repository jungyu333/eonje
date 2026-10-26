"""원본 run 쓰기: start → write × N → finish. manifest가 있어야 완료된 run이다.

- start: run 폴더를 비운다 (재실행 때 지난 페이지가 남지 않게)
- write: 페이지를 쓴다. 쓰기가 성공한 뒤에만 run 상태에 반영한다
- finish: 모든 페이지를 쓴 뒤 _manifest.json을 마지막으로 쓴다
중간에 실패하면 manifest가 없어서 읽는 쪽(Spark 워터마크)이 그 run을 건너뛴다.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime

from fsspec import AbstractFileSystem

from eonje.client.types import Page
from eonje.storage.raw import require_aware, write_page
from eonje.storage.raw_paths import RawRun, manifest_path, run_prefix

MANIFEST_VERSION = 1


@dataclass(frozen=True)
class RunManifest:
    version: int
    dataset: str
    operation: str
    dt: str  # YYYY-MM-DD
    run_key: str
    collected_at: str  # ISO 8601, UTC
    total_count: int  # 1페이지의 totalCount
    row_count: int  # 실제로 쓴 행 수. 예산 상한으로 잘리면 total_count보다 작다
    pages: list[int]  # 쓴 페이지 번호 (정렬)


class RawRunWriter:
    def __init__(
        self, fs: AbstractFileSystem, bucket: str, run: RawRun, collected_at: datetime
    ) -> None:
        require_aware("collected_at", collected_at)
        self._fs = fs
        self._bucket = bucket
        self._run = run
        self._collected_at = collected_at
        self._started = False
        self._operation: str | None = None
        self._total_count = 0
        self._row_count = 0
        self._pages: list[int] = []

    def start(self) -> None:
        prefix = run_prefix(self._bucket, self._run)
        if self._fs.exists(prefix):
            self._fs.rm(prefix, recursive=True)
        self._started = True

    def write(self, page: Page) -> str:
        if not self._started:
            raise RuntimeError("start() must be called before write()")
        if page.page_no in self._pages:
            raise ValueError(f"page {page.page_no} already written in this run")
        if self._operation is not None and page.operation != self._operation:
            raise ValueError(f"mixed operations in one run: {self._operation}, {page.operation}")

        path = write_page(self._fs, self._bucket, self._run, page, self._collected_at)

        # 쓰기가 성공한 뒤에만 run 상태를 바꾼다
        self._operation = page.operation
        if page.page_no == 1:
            self._total_count = page.total_count
        self._pages.append(page.page_no)
        self._row_count += len(page.items)
        return path

    def finish(self) -> RunManifest:
        if not self._pages or self._operation is None:
            raise RuntimeError("finish() called with no pages written")
        if 1 not in self._pages:
            raise RuntimeError("finish() requires page 1 (it carries totalCount)")
        manifest = RunManifest(
            version=MANIFEST_VERSION,
            dataset=str(self._run.dataset),
            operation=self._operation,
            dt=f"{self._run.dt:%Y-%m-%d}",
            run_key=self._run.run_key,
            collected_at=self._collected_at.astimezone(UTC).isoformat(),
            total_count=self._total_count,
            row_count=self._row_count,
            pages=sorted(self._pages),
        )
        body = json.dumps(asdict(manifest), ensure_ascii=False, indent=2, sort_keys=True)
        self._fs.pipe_file(manifest_path(self._bucket, self._run), body.encode("utf-8"))
        return manifest


def read_manifest(fs: AbstractFileSystem, bucket: str, run: RawRun) -> RunManifest | None:
    """완료된 run이면 manifest, 아니면 None. 모르는 버전이면 ValueError."""
    path = manifest_path(bucket, run)
    if not fs.exists(path):
        return None
    data = json.loads(fs.cat_file(path))
    if data.get("version") != MANIFEST_VERSION:
        raise ValueError(f"unsupported manifest version: {data.get('version')!r} at {path}")
    return RunManifest(**{f.name: data[f.name] for f in fields(RunManifest)})
