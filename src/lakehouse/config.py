"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from pyiceberg.catalog import Catalog, load_catalog


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    catalog_name: str = field(default_factory=lambda: _env("ICEBERG_CATALOG_NAME", "clinic"))
    catalog_uri: str = field(default_factory=lambda: _env("ICEBERG_CATALOG_URI", "sqlite:///.lakehouse/catalog.db"))
    warehouse: str = field(default_factory=lambda: _env("ICEBERG_WAREHOUSE", "s3://clinic-lakehouse-dev/warehouse"))
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:8333"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "lakehouse"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "lakehouse-secret"))
    s3_region: str = field(default_factory=lambda: _env("S3_REGION", "us-east-1"))
    namespace: str = field(default_factory=lambda: _env("ICEBERG_NAMESPACE", "bronze"))
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_group: str = field(default_factory=lambda: _env("KAFKA_GROUP", "lakehouse-bronze"))
    kafka_topic_pattern: str = field(default_factory=lambda: _env("KAFKA_TOPIC_PATTERN", r"^clinic\.public\..*"))

    def catalog_properties(self) -> dict[str, str]:
        props = {"type": "sql", "uri": self.catalog_uri, "warehouse": self.warehouse}
        if self.warehouse.startswith("s3://"):
            props.update(
                {
                    "s3.endpoint": self.s3_endpoint,
                    "s3.access-key-id": self.s3_access_key,
                    "s3.secret-access-key": self.s3_secret_key,
                    "s3.region": self.s3_region,
                }
            )
        return props

    def load_catalog(self) -> Catalog:
        if self.catalog_uri.startswith("sqlite:///"):
            path = self.catalog_uri.removeprefix("sqlite:///")
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)
        return load_catalog(self.catalog_name, **self.catalog_properties())
