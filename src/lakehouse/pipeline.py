"""One micro-batch: parse -> quality gate -> Iceberg. Shared by the Kafka consumer and replay."""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field

from .events import RawMessage, parse_message
from .quality import QualityGate
from .sink import IcebergSink

log = logging.getLogger(__name__)


@dataclass
class BatchStats:
    messages: int = 0
    loaded: dict[str, int] = field(default_factory=dict)
    held_back: int = 0
    flagged_by_rule: Counter = field(default_factory=Counter)

    def merge(self, other: BatchStats) -> None:
        self.messages += other.messages
        for k, v in other.loaded.items():
            self.loaded[k] = self.loaded.get(k, 0) + v
        self.held_back += other.held_back
        self.flagged_by_rule.update(other.flagged_by_rule)

    def summary(self) -> str:
        loaded = ", ".join(f"{k}={v}" for k, v in sorted(self.loaded.items())) or "none"
        flags = ", ".join(f"{k}={v}" for k, v in self.flagged_by_rule.most_common()) or "none"
        return f"messages={self.messages} loaded[{loaded}] held_back={self.held_back} flagged[{flags}]"


class Pipeline:
    def __init__(self, sink: IcebergSink, gate: QualityGate | None = None):
        self.sink = sink
        self.gate = gate or QualityGate()

    def process(self, messages: list[RawMessage]) -> BatchStats:
        stats = BatchStats(messages=len(messages))
        if not messages:
            return stats
        events = [parse_message(m) for m in messages]
        result = self.gate.route(events)
        # Quarantine first: if the bronze write then fails, the batch is retried
        # and quarantine may get a duplicate, which is safer than losing a flag.
        self.sink.write_quarantine(result.flagged)
        stats.loaded = self.sink.write_changes(result.clean)
        stats.held_back = result.held_back
        stats.flagged_by_rule = Counter(issue.rule_name for _, issue in result.flagged)
        return stats
