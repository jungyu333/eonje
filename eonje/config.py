from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EONJE_", env_file=".env", extra="ignore")

    database_url: SecretStr = SecretStr("postgresql+psycopg://eonje:eonje@localhost:5432/eonje")


class Settings(DatabaseSettings):
    # API
    api_service_key: SecretStr = Field(min_length=1)
    api_base_url: str = "https://apis.data.go.kr/B552584/EvCharger"
    api_daily_limit: int = Field(default=1000, gt=0)
    page_size: int = Field(default=9999, gt=10, le=9999)
    api_timeout_sec: float = Field(default=30.0, gt=0)
    api_max_attempts: int = Field(default=3, ge=1, le=5)

    # 수집
    status_period_min: int = Field(default=10, ge=1, le=10)
    status_interval_min: int = Field(default=10, gt=0)
    serve_zcodes: list[str] = Field(default_factory=list)

    # 저장
    # S3 버킷 이름 규칙. 스킴(s3://, s3a://)은 쓰는 쪽이 붙인다
    raw_bucket: str = Field(default="eonje-raw", pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    s3_endpoint_url: str = "http://localhost:9000"  # MinIO. 컨테이너 안에서는 http://minio:9000
    s3_access_key: SecretStr | None = None
    s3_secret_key: SecretStr | None = None

    @field_validator("api_service_key", mode="before")
    @classmethod
    def _strip_api_service_key(cls, v: object) -> object:
        """앞뒤 공백 제거. 공백뿐인 값은 min_length로 거부된다."""
        return v.strip() if isinstance(v, str) else v

    @field_validator("s3_access_key", "s3_secret_key", mode="before")
    @classmethod
    def _blank_secret_is_unset(cls, v: object) -> object:
        """빈 값(.env.example 그대로)은 '설정 안 함'으로 본다. 빈 키로 접속을 시도하지 않게."""
        return None if isinstance(v, str) and not v.strip() else v


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


@lru_cache
def get_database_settings() -> DatabaseSettings:
    return DatabaseSettings()
