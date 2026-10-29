"""원본 저장소(SeaweedFS, S3 호환) 파일시스템. 설정에서 만들고, 키가 없으면 여기서 실패한다."""

from __future__ import annotations

import s3fs
from fsspec import AbstractFileSystem

from eonje.config import Settings


def raw_filesystem(settings: Settings) -> AbstractFileSystem:
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

    return s3fs.S3FileSystem(
        key=access_key.get_secret_value(),
        secret=secret_key.get_secret_value(),
        endpoint_url=settings.s3_endpoint_url,
        config_kwargs={"s3": {"addressing_style": "path"}},
        use_listings_cache=False,
        skip_instance_cache=True,
    )
