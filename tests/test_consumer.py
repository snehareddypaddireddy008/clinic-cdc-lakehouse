"""Consumer loop tested with a fake Kafka client: offsets are committed only after Iceberg writes."""

import json

from conftest import appointment

import lakehouse.consumer as consumer_module
from lakehouse.config import Settings
from lakehouse.pipeline import Pipeline


class FakeMessage:
    def __init__(self, value, offset):
        self._value, self._offset = value, offset

    def error(self):
        return None

    def topic(self):
        return "clinic.public.appointments"

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def value(self):
        return self._value


class FakeConsumer:
    def __init__(self, config, messages, log):
        self.messages, self.log = list(messages), log

    def subscribe(self, topics):
        self.log.append(("subscribe", topics))

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, asynchronous):
        self.log.append(("commit", None))

    def close(self):
        self.log.append(("close", None))


def test_commits_after_each_batch_is_written(sink, monkeypatch):
    log = []
    messages = [FakeMessage(json.dumps(appointment(appointment_id=i)).encode(), i) for i in range(1, 6)]
    monkeypatch.setattr(consumer_module, "Consumer", lambda cfg: FakeConsumer(cfg, messages, log))

    writes_before_commit = []
    original = sink.write_changes

    def tracking_write(events):
        writes_before_commit.append(len([e for e in log if e[0] == "commit"]))
        return original(events)

    monkeypatch.setattr(sink, "write_changes", tracking_write)

    runner = consumer_module.KafkaToIceberg(Settings(), Pipeline(sink), batch_size=2, flush_seconds=0.2)
    total = runner.run(max_idle_seconds=0.5)

    assert total.loaded == {"appointments": 5}
    assert sink.table("appointments").scan().to_arrow().num_rows == 5
    commits = [e for e in log if e[0] == "commit"]
    assert len(commits) == 3  # batches of 2, 2, 1
    # Each write happened before the commit for its batch.
    assert writes_before_commit == [0, 1, 2]
    assert log[-1] == ("close", None)
