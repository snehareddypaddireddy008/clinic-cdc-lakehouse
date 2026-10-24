import json
from datetime import UTC, datetime
from decimal import Decimal

from conftest import appointment, msg

from lakehouse.events import parse_message, parse_timestamp


def test_parses_typed_columns_and_cdc_metadata():
    event = parse_message(msg(json.dumps(appointment()).encode()))
    assert event.parse_errors == []
    assert event.table == "appointments"
    assert event.op == "c"
    assert event.is_deleted is False
    assert event.lsn == 1000
    assert event.row["price"] == Decimal("120.00")
    assert event.row["starts_at"] == datetime(2026, 8, 3, 17, 0, tzinfo=UTC)
    assert event.record_key == "1"


def test_delete_rewrite_marks_row_deleted():
    event = parse_message(msg(appointment(__op="d", __deleted="true")))
    assert event.is_deleted is True
    assert event.op == "d"


def test_bad_values_are_collected_not_raised():
    event = parse_message(msg(appointment(price="twelve", starts_at="not a date")))
    assert len(event.parse_errors) == 2
    assert event.row["price"] is None


def test_malformed_json_keeps_raw_payload():
    event = parse_message(msg(b'{"appointment_id": 1,', topic="clinic.public.payments"))
    assert event.table == "payments"
    assert event.parse_errors and "invalid JSON" in event.parse_errors[0]
    assert event.raw.startswith('{"appointment_id"')


def test_unknown_table_is_an_error():
    event = parse_message(msg({"__table": "invoices", "__op": "c"}))
    assert "unknown table" in event.parse_errors[0]


def test_timestamp_formats():
    expected = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
    assert parse_timestamp("2026-08-01T00:00:00Z") == expected
    assert parse_timestamp("2026-07-31T17:00:00-07:00") == expected
    assert parse_timestamp(int(expected.timestamp() * 1000)) == expected
