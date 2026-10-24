from __future__ import annotations

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from lakehouse.events import RawMessage
from lakehouse.sink import IcebergSink


@pytest.fixture
def catalog(tmp_path):
    """A throwaway Iceberg catalog on the local filesystem."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{warehouse}")


@pytest.fixture
def sink(catalog):
    s = IcebergSink(catalog, "bronze")
    s.ensure_tables()
    return s


def appointment(**overrides) -> dict:
    value = {
        "appointment_id": 1,
        "clinic_id": 1,
        "practitioner_id": 1,
        "patient_id": 1,
        "service_type": "initial_assessment",
        "starts_at": "2026-08-03T17:00:00.000000Z",
        "ends_at": "2026-08-03T17:50:00.000000Z",
        "status": "booked",
        "price": "120.00",
        "updated_at": "2026-08-01T10:00:00.000000Z",
        "__op": "c",
        "__source_ts_ms": 1754042400000,
        "__source_lsn": 1000,
        "__table": "appointments",
        "__deleted": "false",
    }
    value.update(overrides)
    return value


def msg(value, topic="clinic.public.appointments", offset=0) -> RawMessage:
    return RawMessage(topic=topic, partition=0, offset=offset, value=value)
