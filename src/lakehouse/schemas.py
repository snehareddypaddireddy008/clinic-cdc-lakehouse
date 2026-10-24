"""Bronze table definitions.

Each source table lands in Iceberg as an append-only change log: the business
columns plus CDC metadata. Current state and SCD Type 2 history are derived
downstream in dbt, so bronze never loses a change.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pyarrow as pa

DECIMAL_TYPE = pa.decimal128(10, 2)
TS_TYPE = pa.timestamp("us", tz="UTC")

ARROW_TYPES: dict[str, pa.DataType] = {
    "long": pa.int64(),
    "int": pa.int32(),
    "string": pa.string(),
    "bool": pa.bool_(),
    "decimal": DECIMAL_TYPE,
    "timestamptz": TS_TYPE,
}

# Metadata columns appended to every bronze table.
CDC_COLUMNS: list[tuple[str, str]] = [
    ("_op", "string"),  # r = snapshot read, c = insert, u = update, d = delete
    ("_is_deleted", "bool"),
    ("_source_ts", "timestamptz"),  # commit time in Postgres
    ("_lsn", "long"),  # Postgres log sequence number, orders changes
    ("_kafka_partition", "int"),
    ("_kafka_offset", "long"),
    ("_ingested_at", "timestamptz"),
]


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_key: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]

    def arrow_schema(self) -> pa.Schema:
        fields = []
        for col, typ in list(self.columns) + CDC_COLUMNS:
            # Primary keys and CDC metadata are required; business columns may be null.
            required = col in self.primary_key or col in {"_op", "_is_deleted", "_ingested_at"}
            fields.append(pa.field(col, ARROW_TYPES[typ], nullable=not required))
        return pa.schema(fields)


TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in [
        TableSpec(
            "clinics",
            ("clinic_id",),
            (
                ("clinic_id", "long"),
                ("clinic_name", "string"),
                ("city", "string"),
                ("province", "string"),
                ("updated_at", "timestamptz"),
            ),
        ),
        TableSpec(
            "practitioners",
            ("practitioner_id",),
            (
                ("practitioner_id", "long"),
                ("clinic_id", "long"),
                ("full_name", "string"),
                ("discipline", "string"),
                ("is_active", "bool"),
                ("updated_at", "timestamptz"),
            ),
        ),
        TableSpec(
            "patients",
            ("patient_id",),
            (
                ("patient_id", "long"),
                ("full_name", "string"),
                ("email", "string"),
                ("phone", "string"),
                ("city", "string"),
                ("birth_year", "int"),
                ("updated_at", "timestamptz"),
            ),
        ),
        TableSpec(
            "appointments",
            ("appointment_id",),
            (
                ("appointment_id", "long"),
                ("clinic_id", "long"),
                ("practitioner_id", "long"),
                ("patient_id", "long"),
                ("service_type", "string"),
                ("starts_at", "timestamptz"),
                ("ends_at", "timestamptz"),
                ("status", "string"),
                ("price", "decimal"),
                ("updated_at", "timestamptz"),
            ),
        ),
        TableSpec(
            "payments",
            ("payment_id",),
            (
                ("payment_id", "long"),
                ("appointment_id", "long"),
                ("amount", "decimal"),
                ("method", "string"),
                ("status", "string"),
                ("paid_at", "timestamptz"),
                ("updated_at", "timestamptz"),
            ),
        ),
    ]
}

QUARANTINE_TABLE = "quarantine"

QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("source_table", pa.string(), nullable=False),
        pa.field("record_key", pa.string()),
        pa.field("rule_name", pa.string(), nullable=False),
        pa.field("severity", pa.string(), nullable=False),  # warn = loaded and flagged, error = held back
        pa.field("owner", pa.string(), nullable=False),  # team that should fix it
        pa.field("reason", pa.string(), nullable=False),
        pa.field("payload", pa.string()),  # original message, for replay after a fix
        pa.field("_lsn", pa.int64()),
        pa.field("_kafka_partition", pa.int32()),
        pa.field("_kafka_offset", pa.int64()),
        pa.field("detected_at", TS_TYPE, nullable=False),
    ]
)


def to_decimal(value: str | float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))
