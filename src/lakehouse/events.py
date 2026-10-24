"""Parse Debezium change events (after the ExtractNewRecordState transform).

A message value looks like:
    {"appointment_id": 7, "status": "completed", ...,
     "__op": "u", "__source_ts_ms": 1760730000000, "__source_lsn": 26843816,
     "__table": "appointments", "__deleted": "false"}

Parsing never raises on bad data. Type problems are collected in
`parse_errors`, so the quality gate can quarantine the row instead of
stopping the pipeline.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .schemas import TABLES, TableSpec, to_decimal

VALID_OPS = {"r", "c", "u", "d"}


@dataclass(frozen=True)
class RawMessage:
    """A message as read from Kafka (or from a replay file)."""

    topic: str
    partition: int
    offset: int
    value: bytes | str | dict | None


@dataclass
class ChangeEvent:
    table: str
    op: str
    is_deleted: bool
    source_ts: datetime | None
    lsn: int | None
    row: dict[str, Any]
    partition: int
    offset: int
    raw: str
    parse_errors: list[str] = field(default_factory=list)

    @property
    def spec(self) -> TableSpec | None:
        return TABLES.get(self.table)

    @property
    def record_key(self) -> str | None:
        spec = self.spec
        if spec is None:
            return None
        parts = [self.row.get(k) for k in spec.primary_key]
        if any(p is None for p in parts):
            return None
        return "|".join(str(p) for p in parts)


def table_from_topic(topic: str) -> str:
    # clinic.public.appointments -> appointments
    return topic.rsplit(".", 1)[-1]


def parse_timestamp(value: Any) -> datetime:
    """Debezium sends TIMESTAMPTZ as an ISO-8601 string (ZonedTimestamp)."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coerce(value: Any, typ: str) -> Any:
    if value is None:
        return None
    if typ in ("long", "int"):
        if isinstance(value, bool):
            raise ValueError("boolean is not an integer")
        return int(value)
    if typ == "string":
        return str(value)
    if typ == "bool":
        if isinstance(value, bool):
            return value
        if str(value).lower() in ("true", "false"):
            return str(value).lower() == "true"
        raise ValueError(f"not a boolean: {value!r}")
    if typ == "decimal":
        return to_decimal(value)
    if typ == "timestamptz":
        return parse_timestamp(value)
    raise ValueError(f"unknown type {typ}")


def _decode(value: bytes | str | dict | None) -> tuple[dict | None, str, str | None]:
    """Return (payload, raw_text, error)."""
    if value is None:
        return None, "", "empty message (tombstone)"
    if isinstance(value, dict):
        return value, json.dumps(value, default=str), None
    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, text, f"invalid JSON: {exc.msg}"
    if not isinstance(payload, dict):
        return None, text, "message is not a JSON object"
    return payload, text, None


def parse_message(msg: RawMessage) -> ChangeEvent:
    payload, raw, error = _decode(msg.value)
    table = (payload or {}).get("__table") or table_from_topic(msg.topic)
    event = ChangeEvent(
        table=table,
        op="?",
        is_deleted=False,
        source_ts=None,
        lsn=None,
        row={},
        partition=msg.partition,
        offset=msg.offset,
        raw=raw,
    )
    if error:
        event.parse_errors.append(error)
        return event

    spec = TABLES.get(table)
    if spec is None:
        event.parse_errors.append(f"unknown table '{table}'")
        return event

    op = str(payload.get("__op", "?"))
    event.op = op
    if op not in VALID_OPS:
        event.parse_errors.append(f"unknown operation '{op}'")
    event.is_deleted = str(payload.get("__deleted", "false")).lower() == "true" or op == "d"

    try:
        if payload.get("__source_ts_ms") is not None:
            event.source_ts = parse_timestamp(payload["__source_ts_ms"])
        if payload.get("__source_lsn") is not None:
            event.lsn = int(payload["__source_lsn"])
    except (TypeError, ValueError) as exc:
        event.parse_errors.append(f"bad CDC metadata: {exc}")

    for col, typ in spec.columns:
        try:
            event.row[col] = _coerce(payload.get(col), typ)
        except (TypeError, ValueError, ArithmeticError) as exc:
            event.row[col] = None
            event.parse_errors.append(f"{col}: {exc}")
    return event
