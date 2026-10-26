from __future__ import annotations

import s3fs
from fsspec import AbstractFileSystem

from eonje.config import Settings


def raw_filesystem(settings: Settings) -> AbstractFileSystem:

    missing = [
        name
        for name, value in (
            ("EONJE_S3_ACCESS_KEY", settings.s3_access_key),
            ("EONJE_S3_SECRET_KEY", settings.s3_secret_key),
        )
        if value is None
    ]

    if missing:
        raise ValueError(f"S3 credentials are not set: {', '.join(missing)}")

    assert settings.s3_access_key is not None and settings.s3_secret_key is not None

    return s3fs.S3FileSystem(
        key=settings.s3_access_key.get_secret_value(),
        secret=settings.s3_secret_key.get_secret_value(),
        endpoint_url=settings.s3_endpoint_url,
        config_kwargs={"s3": {"addressing_style": "path"}},
        use_listings_cache=False,
        skip_instance_cache=True,
    )
