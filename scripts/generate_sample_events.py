"""Generate a deterministic file of Debezium-shaped CDC events (no Docker needed).

The output mimics what the Debezium Postgres connector emits after the
ExtractNewRecordState transform, including snapshot reads, inserts, updates,
deletes, a few deliberately bad rows and some redelivered (duplicate) messages.

    python scripts/generate_sample_events.py --out sample_data/cdc_events.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

START = datetime(2026, 8, 1, 14, 0, tzinfo=UTC)
SERVICES = {
    "physiotherapy": ("initial_assessment", "120.00"),
    "massage": ("massage_60", "110.00"),
    "chiropractic": ("adjustment", "85.00"),
    "counselling": ("session_50", "150.00"),
}
CITIES = ["Surrey", "Vancouver", "Burnaby", "Richmond", "Calgary", "Langley", "Coquitlam"]
FIRST = ["Maya", "Arjun", "Olivia", "Noah", "Sofia", "Ethan", "Aisha", "Lucas", "Mei", "Daniel", "Zara", "Ravi"]
LAST = ["Singh", "Wong", "Martin", "Patel", "Nguyen", "Brown", "Kaur", "Tremblay", "Lee", "Gill"]


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Emitter:
    def __init__(self) -> None:
        self.lsn = 24_000_000
        self.offsets: dict[tuple[str, int], int] = {}
        self.lines: list[dict] = []

    def emit(self, table: str, op: str, row: dict, ts: datetime, key: int, snapshot_lsn: int | None = None) -> dict:
        self.lsn += random.randint(40, 400)
        topic = f"clinic.public.{table}"
        partition = key % 3
        offset = self.offsets.get((topic, partition), 0)
        self.offsets[(topic, partition)] = offset + 1
        value = dict(row)
        value.update(
            __op=op,
            __source_ts_ms=int(ts.timestamp() * 1000),
            __source_lsn=snapshot_lsn or self.lsn,
            __table=table,
            __deleted="true" if op == "d" else "false",
        )
        rec = {"topic": topic, "partition": partition, "offset": offset, "value": value, "_ts": ts}
        self.lines.append(rec)
        return rec

    def emit_raw(self, table: str, raw_value: str, ts: datetime) -> None:
        topic = f"clinic.public.{table}"
        offset = self.offsets.get((topic, 0), 0)
        self.offsets[(topic, 0)] = offset + 1
        self.lines.append({"topic": topic, "partition": 0, "offset": offset, "value": raw_value, "_ts": ts})


def generate(seed: int = 42, n_patients: int = 120, days: int = 60) -> list[dict]:
    random.seed(seed)
    em = Emitter()

    # --- Snapshot of reference tables (op = r), all at one LSN, like Debezium's initial snapshot.
    snap_lsn = em.lsn
    clinics = {
        1: {"clinic_id": 1, "clinic_name": "Fraser Valley Physio", "city": "Surrey", "province": "BC"},
        2: {"clinic_id": 2, "clinic_name": "Kits Movement Clinic", "city": "Vancouver", "province": "BC"},
        3: {"clinic_id": 3, "clinic_name": "Bow River Wellness", "city": "Calgary", "province": "AB"},
    }
    practitioners = {
        1: {
            "practitioner_id": 1,
            "clinic_id": 1,
            "full_name": "Priya Sandhu",
            "discipline": "physiotherapy",
            "is_active": True,
        },
        2: {
            "practitioner_id": 2,
            "clinic_id": 1,
            "full_name": "Tom Becker",
            "discipline": "massage",
            "is_active": True,
        },
        3: {
            "practitioner_id": 3,
            "clinic_id": 2,
            "full_name": "Ana Ruiz",
            "discipline": "chiropractic",
            "is_active": True,
        },
        4: {
            "practitioner_id": 4,
            "clinic_id": 2,
            "full_name": "Liam Chen",
            "discipline": "physiotherapy",
            "is_active": True,
        },
        5: {
            "practitioner_id": 5,
            "clinic_id": 3,
            "full_name": "Grace Okafor",
            "discipline": "counselling",
            "is_active": True,
        },
    }
    for cid, c in clinics.items():
        c["updated_at"] = iso(START - timedelta(days=30))
        em.emit("clinics", "r", c, START, cid, snapshot_lsn=snap_lsn)
    for pid, p in practitioners.items():
        p["updated_at"] = iso(START - timedelta(days=30))
        em.emit("practitioners", "r", p, START, pid, snapshot_lsn=snap_lsn)

    patients: dict[int, dict] = {}
    appointments: dict[int, dict] = {}
    payment_id = 0
    bad_emails = {7, 33, 81}

    for pid in range(1, n_patients + 1):
        ts = START + timedelta(minutes=random.randint(1, days * 24 * 60 // 2))
        first, last = random.choice(FIRST), random.choice(LAST)
        email = f"{first}.{last}{pid}@example.com".lower()
        if pid in bad_emails:
            email = email.replace("@", " at ")  # WARN: loaded, but flagged
        patients[pid] = {
            "patient_id": pid,
            "full_name": f"{first} {last}",
            "email": email,
            "phone": f"604-555-{1000 + pid:04d}",
            "city": random.choice(CITIES),
            "birth_year": random.randint(1950, 2006),
            "updated_at": iso(ts),
        }
        em.emit("patients", "c", patients[pid], ts, pid)

        for _ in range(random.randint(1, 4)):
            appt_id = len(appointments) + 1
            prac = practitioners[random.randint(1, 5)]
            service, price = SERVICES[prac["discipline"]]
            booked_at = ts + timedelta(hours=random.randint(1, 24 * 20))
            starts = (booked_at + timedelta(days=random.randint(1, 14))).replace(minute=0, second=0, microsecond=0)
            ends = starts + timedelta(minutes=50)
            if appt_id in {15, 140, 260}:
                ends = starts - timedelta(minutes=10)  # ERROR: held back from bronze
            appt = {
                "appointment_id": appt_id,
                "clinic_id": prac["clinic_id"],
                "practitioner_id": prac["practitioner_id"],
                "patient_id": pid,
                "service_type": service,
                "starts_at": iso(starts),
                "ends_at": iso(ends),
                "status": "booked",
                "price": price,
                "updated_at": iso(booked_at),
            }
            appointments[appt_id] = appt
            em.emit("appointments", "c", appt, booked_at, appt_id)

            # Some bookings get rescheduled before they happen.
            if random.random() < 0.15:
                change_ts = booked_at + timedelta(hours=random.randint(1, 48))
                starts = starts + timedelta(days=random.randint(1, 5))
                appt.update(
                    starts_at=iso(starts), ends_at=iso(starts + timedelta(minutes=50)), updated_at=iso(change_ts)
                )
                em.emit("appointments", "u", appt, change_ts, appt_id)

            outcome = random.choices(["completed", "cancelled", "no_show", "booked"], weights=[70, 12, 8, 10])[0]
            if outcome == "booked":
                continue
            done_ts = starts + timedelta(hours=1)
            appt.update(status=outcome, updated_at=iso(done_ts))
            em.emit("appointments", "u", appt, done_ts, appt_id)

            if outcome == "completed":
                payment_id += 1
                amount = appt["price"]
                if payment_id in {20, 95}:
                    amount = "-" + amount  # ERROR: negative payment
                pay = {
                    "payment_id": payment_id,
                    "appointment_id": appt_id,
                    "amount": amount,
                    "method": random.choices(["card", "insurance", "cash"], weights=[60, 35, 5])[0],
                    "status": "captured",
                    "paid_at": iso(done_ts),
                    "updated_at": iso(done_ts),
                }
                em.emit("payments", "c", pay, done_ts, payment_id)
                if random.random() < 0.05:
                    refund_ts = done_ts + timedelta(days=2)
                    pay.update(status="refunded", updated_at=iso(refund_ts))
                    em.emit("payments", "u", pay, refund_ts, payment_id)

        # Some patients move city later -> SCD Type 2 history in dbt.
        if random.random() < 0.2:
            move_ts = ts + timedelta(days=random.randint(10, 40))
            patients[pid].update(
                city=random.choice([c for c in CITIES if c != patients[pid]["city"]]), updated_at=iso(move_ts)
            )
            em.emit("patients", "u", patients[pid], move_ts, pid)

    # A practitioner leaves and a clinic is renamed (dimension changes).
    leave_ts = START + timedelta(days=45)
    practitioners[2].update(is_active=False, updated_at=iso(leave_ts))
    em.emit("practitioners", "u", practitioners[2], leave_ts, 2)
    rename_ts = START + timedelta(days=50)
    clinics[2].update(clinic_name="Kitsilano Movement Clinic", updated_at=iso(rename_ts))
    em.emit("clinics", "u", clinics[2], rename_ts, 2)

    # A booking created in error is hard-deleted in the source.
    booked_only = [a for a in appointments.values() if a["status"] == "booked"]
    victim = booked_only[0]
    del_ts = START + timedelta(days=days - 1)
    em.emit("appointments", "d", victim, del_ts, victim["appointment_id"])

    # An unknown status value and a malformed message.
    weird = dict(appointments[2], status="pending_review", updated_at=iso(del_ts))
    em.emit("appointments", "u", weird, del_ts, 2)
    em.emit_raw("payments", '{"payment_id": 999, "amount": ', del_ts)

    # Events were generated per patient, so put them in commit-time order and
    # renumber LSNs and offsets to increase in that order, as they would in Postgres/Kafka.
    em.lines.sort(key=lambda r: r["_ts"])
    lsn = snap_lsn
    offsets: dict[tuple[str, int], int] = {}
    for rec in em.lines:
        value = rec["value"]
        if isinstance(value, dict) and value["__op"] != "r":
            lsn += random.randint(40, 400)
            value["__source_lsn"] = lsn
        key = (rec["topic"], rec["partition"])
        rec["offset"] = offsets.get(key, 0)
        offsets[key] = rec["offset"] + 1

    # At-least-once redelivery: the same five messages arrive twice.
    for rec in em.lines[100:105]:
        em.lines.append(dict(rec))
    for rec in em.lines:
        rec.pop("_ts", None)
    return em.lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("sample_data/cdc_events.jsonl"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    lines = generate(args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fh:
        for rec in lines:
            fh.write(json.dumps(rec) + "\n")
    print(f"wrote {len(lines)} events to {args.out}")


if __name__ == "__main__":
    main()
