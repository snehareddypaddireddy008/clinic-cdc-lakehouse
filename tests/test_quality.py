from conftest import appointment, msg

from lakehouse.events import parse_message
from lakehouse.quality import QualityGate, Severity


def _patient(email):
    return {
        "patient_id": 5,
        "full_name": "Mei Lee",
        "email": email,
        "phone": None,
        "city": "Surrey",
        "birth_year": 1990,
        "updated_at": "2026-08-01T10:00:00Z",
        "__op": "c",
        "__source_ts_ms": 1754042400000,
        "__source_lsn": 10,
        "__table": "patients",
        "__deleted": "false",
    }


def test_clean_event_passes():
    result = QualityGate().route([parse_message(msg(appointment()))])
    assert len(result.clean) == 1
    assert result.flagged == []


def test_end_before_start_is_held_back_and_routed_to_owner():
    bad = parse_message(msg(appointment(ends_at="2026-08-03T16:50:00Z")))
    result = QualityGate().route([bad])
    assert result.clean == []
    [(_event, issue)] = result.flagged
    assert issue.rule_name == "appointment_ends_after_start"
    assert issue.severity is Severity.ERROR
    assert issue.owner == "scheduling-app"


def test_warning_is_loaded_and_flagged():
    event = parse_message(msg(_patient("mei at example.com"), topic="clinic.public.patients"))
    result = QualityGate().route([event])
    assert len(result.clean) == 1
    assert [i.severity for _, i in result.flagged] == [Severity.WARN]


def test_one_bad_row_does_not_block_the_batch():
    events = [
        parse_message(msg(appointment(appointment_id=1))),
        parse_message(msg(appointment(appointment_id=2, status="pending_review"))),
        parse_message(msg(b"{broken")),
    ]
    result = QualityGate().route(events)
    assert [e.row["appointment_id"] for e in result.clean] == [1]
    assert result.held_back == 2


def test_deletes_skip_business_rules():
    # A deleted row with bad times is still a valid delete to record.
    event = parse_message(msg(appointment(__op="d", __deleted="true", ends_at="2026-08-03T16:00:00Z")))
    assert QualityGate().evaluate(event) == []
