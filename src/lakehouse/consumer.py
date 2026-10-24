"""Kafka -> Iceberg consumer.

Delivery guarantee: at-least-once. Offsets are committed only after the batch
is safely in Iceberg. If the process dies between the Iceberg commit and the
offset commit, the batch is re-read; dbt removes those duplicates using the
(primary key, _lsn) of each change.

Run:  python -m lakehouse.consumer --batch-size 500 --flush-seconds 5
"""

from __future__ import annotations

import argparse
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from .config import Settings
from .events import RawMessage
from .pipeline import BatchStats, Pipeline
from .sink import IcebergSink

log = logging.getLogger("lakehouse.consumer")


class KafkaToIceberg:
    def __init__(self, settings: Settings, pipeline: Pipeline, batch_size: int, flush_seconds: float):
        self.settings = settings
        self.pipeline = pipeline
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self._running = True
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": settings.kafka_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )

    def stop(self, *_args) -> None:
        log.info("stopping after the current batch")
        self._running = False

    def _poll_batch(self) -> list[RawMessage]:
        batch: list[RawMessage] = []
        deadline = time.monotonic() + self.flush_seconds
        while self._running and len(batch) < self.batch_size and time.monotonic() < deadline:
            msg = self.consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            batch.append(RawMessage(msg.topic(), msg.partition(), msg.offset(), msg.value()))
        return batch

    def run(self, max_idle_seconds: float | None = None) -> BatchStats:
        total = BatchStats()
        idle_since = time.monotonic()
        self.consumer.subscribe([self.settings.kafka_topic_pattern])
        log.info("subscribed to %s", self.settings.kafka_topic_pattern)
        try:
            while self._running:
                batch = self._poll_batch()
                if not batch:
                    if max_idle_seconds is not None and time.monotonic() - idle_since > max_idle_seconds:
                        log.info("no new messages for %ss, exiting", max_idle_seconds)
                        break
                    continue
                idle_since = time.monotonic()
                stats = self.pipeline.process(batch)
                self.consumer.commit(asynchronous=False)
                log.info("batch committed: %s", stats.summary())
                total.merge(stats)
        finally:
            self.consumer.close()
        log.info("total: %s", total.summary())
        return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Debezium CDC events from Kafka into Iceberg.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--flush-seconds", type=float, default=5.0)
    parser.add_argument(
        "--max-idle-seconds",
        type=float,
        default=None,
        help="exit after this many seconds without messages (handy for demos)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = Settings()
    sink = IcebergSink(settings.load_catalog(), settings.namespace)
    sink.ensure_tables()
    runner = KafkaToIceberg(settings, Pipeline(sink), args.batch_size, args.flush_seconds)
    signal.signal(signal.SIGINT, runner.stop)
    signal.signal(signal.SIGTERM, runner.stop)
    runner.run(args.max_idle_seconds)


if __name__ == "__main__":
    main()
