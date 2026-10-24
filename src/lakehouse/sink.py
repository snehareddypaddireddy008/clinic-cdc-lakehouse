"""Write change events and quarantine records to Apache Iceberg tables."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, datetime

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from .events import ChangeEvent
from .quality import Issue
from .schemas import QUARANTINE_SCHEMA, QUARANTINE_TABLE, TABLES

log = logging.getLogger(__name__)


class IcebergSink:
    """Appends each batch to one bronze change-log table per source table.

    Bronze tables are partitioned by day of the source commit time, so
    downstream jobs can prune old partitions when they read incrementally.
    """

    def __init__(self, catalog: Catalog, namespace: str = "bronze"):
        self.catalog = catalog
        self.namespace = namespace
        self._tables: dict[str, Table] = {}

    def _identifier(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def ensure_tables(self) -> None:
        try:
            self.catalog.create_namespace(self.namespace)
        except NamespaceAlreadyExistsError:
            pass
        for name, spec in TABLES.items():
            self._tables[name] = self._create_if_missing(name, spec.arrow_schema(), partition_col="_source_ts")
        self._tables[QUARANTINE_TABLE] = self._create_if_missing(
            QUARANTINE_TABLE, QUARANTINE_SCHEMA, partition_col="detected_at"
        )

    def _create_if_missing(self, name: str, schema: pa.Schema, partition_col: str) -> Table:
        identifier = self._identifier(name)
        if self.catalog.table_exists(identifier):
            return self.catalog.load_table(identifier)
        table = self.catalog.create_table(identifier, schema=schema)
        with table.update_spec() as update:
            update.add_field(partition_col, DayTransform(), f"{partition_col}_day")
        log.info("created Iceberg table %s", identifier)
        return self.catalog.load_table(identifier)

    def table(self, name: str) -> Table:
        if name not in self._tables:
            self.ensure_tables()
        return self._tables[name]

    def write_changes(self, events: list[ChangeEvent]) -> dict[str, int]:
        """Append clean events, one Iceberg commit per source table. Returns rows per table."""
        ingested_at = datetime.now(UTC)
        by_table: dict[str, list[dict]] = defaultdict(list)
        for event in events:
            row = dict(event.row)
            row.update(
                _op=event.op,
                _is_deleted=event.is_deleted,
                _source_ts=event.source_ts,
                _lsn=event.lsn,
                _kafka_partition=event.partition,
                _kafka_offset=event.offset,
                _ingested_at=ingested_at,
            )
            by_table[event.table].append(row)

        written = {}
        for name, rows in by_table.items():
            arrow = pa.Table.from_pylist(rows, schema=TABLES[name].arrow_schema())
            self.table(name).append(arrow)
            written[name] = len(rows)
        return written

    def write_quarantine(self, flagged: list[tuple[ChangeEvent, Issue]]) -> int:
        if not flagged:
            return 0
        detected_at = datetime.now(UTC)
        rows = [
            {
                "source_table": event.table,
                "record_key": event.record_key,
                "rule_name": issue.rule_name,
                "severity": issue.severity.value,
                "owner": issue.owner,
                "reason": issue.reason,
                "payload": event.raw,
                "_lsn": event.lsn,
                "_kafka_partition": event.partition,
                "_kafka_offset": event.offset,
                "detected_at": detected_at,
            }
            for event, issue in flagged
        ]
        self.table(QUARANTINE_TABLE).append(pa.Table.from_pylist(rows, schema=QUARANTINE_SCHEMA))
        return len(rows)
