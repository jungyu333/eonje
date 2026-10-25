from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EONJE_", env_file=".env", extra="ignore")

    # API
    api_service_key: SecretStr
    api_base_url: str = "https://apis.data.go.kr/B552584/EvCharger"
    api_daily_limit: int = Field(default=1000, gt=0)
    page_size: int = Field(default=9999, gt=10, le=9999)

    # 수집
    status_period_min: int = Field(default=10, ge=1, le=10)
    status_interval_min: int = Field(default=10, gt=0)
    serve_zcodes: list[str] = Field(default_factory=list)

    # 저장
    raw_root: str = "s3a://eonje-raw"
    database_url: str = "postgresql+psycopg://eonje:eonje@localhost:5432/eonje"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
