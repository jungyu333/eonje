from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

MAX_PAGE_NO = 999
_RUN_KEY = re.compile(r"^[0-9A-Za-z][0-9A-Za-z_.-]*$")


class Dataset(StrEnum):
    CHARGER_STATUS = "charger_status"
    CHARGER_INFO = "charger_info"


DATASET_BY_OPERATION: dict[str, Dataset] = {
    "getChargerStatus": Dataset.CHARGER_STATUS,
    "getChargerInfo": Dataset.CHARGER_INFO,
}


@dataclass(frozen=True)
class RawRun:
    dataset: Dataset
    dt: date
    run_key: str

    def __post_init__(self) -> None:
        if not _RUN_KEY.fullmatch(self.run_key):
            raise ValueError(f"run_key must be path-safe [0-9A-Za-z_.-]: {self.run_key!r}")


def run_prefix(bucket: str, run: RawRun) -> str:
    return f"{bucket}/{run.dataset}/dt={run.dt:%Y-%m-%d}/run={run.run_key}"


def page_path(bucket: str, run: RawRun, page_no: int) -> str:
    if not 1 <= page_no <= MAX_PAGE_NO:
        raise ValueError(f"page_no out of range 1..{MAX_PAGE_NO}: {page_no}")
    return f"{run_prefix(bucket, run)}/page={page_no:03d}.parquet"


def manifest_path(bucket: str, run: RawRun) -> str:
    return f"{run_prefix(bucket, run)}/_manifest.json"
