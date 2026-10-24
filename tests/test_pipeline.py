from pathlib import Path

from conftest import appointment, msg
from generate_sample_events import generate

from lakehouse.pipeline import Pipeline
from lakehouse.replay import replay


def test_batch_lands_in_iceberg_with_quarantine(sink):
    stats = Pipeline(sink).process(
        [
            msg(appointment(appointment_id=1), offset=0),
            msg(appointment(appointment_id=1, __op="u", status="completed", __source_lsn=1100), offset=1),
            msg(appointment(appointment_id=2, price="-5"), offset=2),  # passes: price rule not defined
            msg(appointment(appointment_id=3, ends_at="2026-08-03T16:00:00Z"), offset=3),  # held back
        ]
    )
    assert stats.loaded == {"appointments": 3}
    assert stats.held_back == 1

    bronze = sink.table("appointments").scan().to_arrow()
    assert bronze.num_rows == 3
    assert sorted(bronze.column("_op").to_pylist()) == ["c", "c", "u"]

    quarantine = sink.table("quarantine").scan().to_arrow().to_pylist()
    assert len(quarantine) == 1
    assert quarantine[0]["record_key"] == "3"
    assert quarantine[0]["owner"] == "scheduling-app"
    assert '"appointment_id": 3' in quarantine[0]["payload"]


def test_bronze_is_partitioned_by_day(sink):
    spec = sink.table("appointments").spec()
    assert [f.name for f in spec.fields] == ["_source_ts_day"]


def test_sample_file_replays_end_to_end(sink, tmp_path):
    import json

    path = Path(tmp_path) / "events.jsonl"
    with path.open("w") as fh:
        for rec in generate(seed=1, n_patients=30, days=20):
            fh.write(json.dumps(rec) + "\n")

    total = replay(path, Pipeline(sink), batch_size=100)
    assert total.messages > 100
    assert total.loaded["clinics"] >= 3
    assert total.held_back >= 1  # the malformed message at least
    assert sink.table("quarantine").scan().to_arrow().num_rows >= 1
