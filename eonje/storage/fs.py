"""원본 저장소(SeaweedFS, S3 호환) 파일시스템. 설정에서 만들고, 키가 없으면 여기서 실패한다."""

from __future__ import annotations

import s3fs
from fsspec import AbstractFileSystem

from eonje.config import Settings


def s3_credentials(settings: Settings) -> tuple[str, str]:
    """(access key, secret key). 비어 있으면 어느 변수가 없는지 알려 주고 실패한다.

    s3fs와 Spark 모두 키가 없으면 익명 접근을 조용히 시도해 원인 찾기가 어려우므로 여기서 막는다.
    """
    access_key = settings.s3_access_key
    secret_key = settings.s3_secret_key
    if access_key is None or secret_key is None:
        missing = [
            name
            for name, value in (
                ("EONJE_S3_ACCESS_KEY", access_key),
                ("EONJE_S3_SECRET_KEY", secret_key),
            )
            if value is None
        ]
        raise ValueError(f"S3 credentials are not set: {', '.join(missing)}")
    return access_key.get_secret_value(), secret_key.get_secret_value()


def raw_filesystem(settings: Settings) -> AbstractFileSystem:
    access_key, secret_key = s3_credentials(settings)
    return s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        endpoint_url=settings.s3_endpoint_url,
        config_kwargs={"s3": {"addressing_style": "path"}},
        use_listings_cache=False,
        skip_instance_cache=True,
    )
