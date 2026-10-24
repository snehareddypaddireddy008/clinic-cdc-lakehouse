"""Non-blocking data quality gate.

Bad data never stops the pipeline and is never silently dropped:
  - ERROR issues: the change is held back from bronze and written to the
    quarantine table, with the owning team and the original payload so it
    can be replayed once fixed.
  - WARN issues: the change is loaded to bronze AND recorded in quarantine,
    so the owning team can follow up.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from .events import ChangeEvent


class Severity(str, Enum):
    WARN = "warn"
    ERROR = "error"


@dataclass(frozen=True)
class Issue:
    rule_name: str
    severity: Severity
    owner: str
    reason: str


class Rule(ABC):
    """A single check. Subclasses set the class attributes and implement check()."""

    name: str
    severity: Severity
    owner: str
    tables: frozenset[str] | None = None  # None = every table
    check_deletes: bool = False  # most rules only make sense for live rows

    def applies_to(self, event: ChangeEvent) -> bool:
        if self.tables is not None and event.table not in self.tables:
            return False
        return self.check_deletes or not event.is_deleted

    @abstractmethod
    def check(self, event: ChangeEvent) -> str | None:
        """Return a reason string if the event fails, otherwise None."""

    def evaluate(self, event: ChangeEvent) -> Issue | None:
        if not self.applies_to(event):
            return None
        reason = self.check(event)
        if reason is None:
            return None
        return Issue(self.name, self.severity, self.owner, reason)


class ParseErrors(Rule):
    name = "parse_errors"
    severity = Severity.ERROR
    owner = "data-platform"
    check_deletes = True

    def check(self, event: ChangeEvent) -> str | None:
        return "; ".join(event.parse_errors) if event.parse_errors else None


class PrimaryKeyPresent(Rule):
    name = "primary_key_present"
    severity = Severity.ERROR
    owner = "data-platform"
    check_deletes = True

    def check(self, event: ChangeEvent) -> str | None:
        if event.spec is None or event.parse_errors:
            return None  # already reported by ParseErrors
        return None if event.record_key is not None else "primary key is null"


class AppointmentTimesValid(Rule):
    name = "appointment_ends_after_start"
    severity = Severity.ERROR
    owner = "scheduling-app"
    tables = frozenset({"appointments"})

    def check(self, event: ChangeEvent) -> str | None:
        start, end = event.row.get("starts_at"), event.row.get("ends_at")
        if start and end and end <= start:
            return f"ends_at {end.isoformat()} is not after starts_at {start.isoformat()}"
        return None


class AllowedValues(Rule):
    severity = Severity.ERROR
    owner = "scheduling-app"

    def __init__(self, table: str, column: str, allowed: set[str], owner: str | None = None):
        self.name = f"{table}_{column}_allowed"
        self.tables = frozenset({table})
        self.column = column
        self.allowed = allowed
        if owner:
            self.owner = owner

    def check(self, event: ChangeEvent) -> str | None:
        value = event.row.get(self.column)
        if value is not None and value not in self.allowed:
            return f"{self.column}='{value}' not in {sorted(self.allowed)}"
        return None


class PaymentAmountPositive(Rule):
    name = "payment_amount_positive"
    severity = Severity.ERROR
    owner = "billing"
    tables = frozenset({"payments"})

    def check(self, event: ChangeEvent) -> str | None:
        amount = event.row.get("amount")
        if amount is not None and amount <= 0:
            return f"amount {amount} must be greater than zero"
        return None


class PatientEmailFormat(Rule):
    name = "patient_email_format"
    severity = Severity.WARN
    owner = "scheduling-app"
    tables = frozenset({"patients"})
    _pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")

    def check(self, event: ChangeEvent) -> str | None:
        email = event.row.get("email")
        if email and not self._pattern.match(email):
            return "email is not a valid address"
        return None


def default_rules() -> list[Rule]:
    return [
        ParseErrors(),
        PrimaryKeyPresent(),
        AppointmentTimesValid(),
        AllowedValues("appointments", "status", {"booked", "completed", "cancelled", "no_show"}),
        AllowedValues("payments", "method", {"card", "insurance", "cash"}, owner="billing"),
        AllowedValues("payments", "status", {"captured", "refunded"}, owner="billing"),
        PaymentAmountPositive(),
        PatientEmailFormat(),
    ]


@dataclass
class GateResult:
    clean: list[ChangeEvent]
    flagged: list[tuple[ChangeEvent, Issue]]

    @property
    def held_back(self) -> int:
        return len({id(e) for e, i in self.flagged if i.severity is Severity.ERROR})


class QualityGate:
    def __init__(self, rules: list[Rule] | None = None):
        self.rules = rules if rules is not None else default_rules()

    def evaluate(self, event: ChangeEvent) -> list[Issue]:
        return [issue for rule in self.rules if (issue := rule.evaluate(event)) is not None]

    def route(self, events: list[ChangeEvent]) -> GateResult:
        clean: list[ChangeEvent] = []
        flagged: list[tuple[ChangeEvent, Issue]] = []
        for event in events:
            issues = self.evaluate(event)
            flagged.extend((event, issue) for issue in issues)
            if not any(i.severity is Severity.ERROR for i in issues):
                clean.append(event)
        return GateResult(clean=clean, flagged=flagged)
