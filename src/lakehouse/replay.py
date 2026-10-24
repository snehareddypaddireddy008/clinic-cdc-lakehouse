"""Load Debezium-format events from a JSON Lines file instead of Kafka.

Used by CI and for working offline. Each line:
    {"topic": "clinic.public.appointments", "partition": 0, "offset": 12, "value": {...}}

Run:  python -m lakehouse.replay sample_data/cdc_events.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .config import Settings
from .events import RawMessage
from .pipeline import BatchStats, Pipeline
from .sink import IcebergSink

log = logging.getLogger("lakehouse.replay")


def read_messages(path: Path) -> list[RawMessage]:
    messages = []
    with path.open(encoding="utf-8") as fh:
        for line_no, line in enumerate(fh):
            if not line.strip():
                continue
            rec = json.loads(line)
            messages.append(
                RawMessage(rec["topic"], rec.get("partition", 0), rec.get("offset", line_no), rec.get("value"))
            )
    return messages


def replay(path: Path, pipeline: Pipeline, batch_size: int = 500) -> BatchStats:
    messages = read_messages(path)
    total = BatchStats()
    for start in range(0, len(messages), batch_size):
        stats = pipeline.process(messages[start : start + batch_size])
        log.info("batch: %s", stats.summary())
        total.merge(stats)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a JSONL file of CDC events into Iceberg.")
    parser.add_argument("path", type=Path)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = Settings()
    sink = IcebergSink(settings.load_catalog(), settings.namespace)
    sink.ensure_tables()
    total = replay(args.path, Pipeline(sink), args.batch_size)
    print(total.summary())


if __name__ == "__main__":
    main()
