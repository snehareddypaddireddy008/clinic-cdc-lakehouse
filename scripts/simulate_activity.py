"""Write realistic booking activity into the source Postgres so Debezium has changes to capture.

    python scripts/simulate_activity.py --actions 300            # clean activity
    python scripts/simulate_activity.py --actions 50 --dirty     # also inject bad rows

Each action is its own transaction, like a real app: new patients, bookings,
reschedules, completions with payments, cancellations, no-shows, refunds,
patients moving city, and occasionally a booking deleted in error.
"""

from __future__ import annotations

import argparse
import random
import time
from datetime import UTC, datetime, timedelta

import psycopg

DSN = "postgresql://clinic:clinic@localhost:5432/clinic"
CITIES = ["Surrey", "Vancouver", "Burnaby", "Richmond", "Calgary", "Langley", "Coquitlam"]
FIRST = ["Maya", "Arjun", "Olivia", "Noah", "Sofia", "Ethan", "Aisha", "Lucas", "Mei", "Daniel"]
LAST = ["Singh", "Wong", "Martin", "Patel", "Nguyen", "Brown", "Kaur", "Tremblay", "Lee", "Gill"]
PRICES = {
    "physiotherapy": ("initial_assessment", 120),
    "massage": ("massage_60", 110),
    "chiropractic": ("adjustment", 85),
    "counselling": ("session_50", 150),
}


def one(cur, sql, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def new_patient(cur, dirty: bool) -> None:
    first, last = random.choice(FIRST), random.choice(LAST)
    email = f"{first}.{last}{random.randint(1, 99999)}@example.com".lower()
    if dirty:
        email = email.replace("@", " at ")
    cur.execute(
        "INSERT INTO patients (full_name, email, phone, city, birth_year) VALUES (%s, %s, %s, %s, %s)",
        (
            f"{first} {last}",
            email,
            f"604-555-{random.randint(1000, 9999)}",
            random.choice(CITIES),
            random.randint(1950, 2006),
        ),
    )


def book(cur, dirty: bool) -> None:
    patient = one(cur, "SELECT patient_id FROM patients ORDER BY random() LIMIT 1")
    if patient is None:
        return new_patient(cur, False)
    cur.execute(
        "SELECT practitioner_id, clinic_id, discipline FROM practitioners WHERE is_active ORDER BY random() LIMIT 1"
    )
    prac, clinic, discipline = cur.fetchone()
    service, price = PRICES[discipline]
    starts = (datetime.now(UTC) + timedelta(days=random.randint(1, 14))).replace(minute=0, second=0, microsecond=0)
    ends = starts - timedelta(minutes=10) if dirty else starts + timedelta(minutes=50)
    cur.execute(
        """INSERT INTO appointments (clinic_id, practitioner_id, patient_id, service_type, starts_at, ends_at, status, price)
           VALUES (%s, %s, %s, %s, %s, %s, 'booked', %s)""",
        (clinic, prac, patient, service, starts, ends, price),
    )


def reschedule(cur) -> None:
    cur.execute(
        """UPDATE appointments SET starts_at = starts_at + interval '2 days', ends_at = ends_at + interval '2 days',
                  updated_at = now()
           WHERE appointment_id = (SELECT appointment_id FROM appointments WHERE status = 'booked' ORDER BY random() LIMIT 1)"""
    )


def finish(cur, dirty: bool) -> None:
    appt = one(cur, "SELECT appointment_id FROM appointments WHERE status = 'booked' ORDER BY random() LIMIT 1")
    if appt is None:
        return
    outcome = random.choices(["completed", "cancelled", "no_show"], weights=[78, 13, 9])[0]
    cur.execute("UPDATE appointments SET status = %s, updated_at = now() WHERE appointment_id = %s", (outcome, appt))
    if outcome == "completed":
        price = one(cur, "SELECT price FROM appointments WHERE appointment_id = %s", (appt,))
        cur.execute(
            "INSERT INTO payments (appointment_id, amount, method, status) VALUES (%s, %s, %s, 'captured')",
            (appt, -price if dirty else price, random.choices(["card", "insurance", "cash"], weights=[60, 35, 5])[0]),
        )


def refund(cur) -> None:
    cur.execute(
        """UPDATE payments SET status = 'refunded', updated_at = now()
           WHERE payment_id = (SELECT payment_id FROM payments WHERE status = 'captured' ORDER BY random() LIMIT 1)"""
    )


def move_city(cur) -> None:
    cur.execute(
        """UPDATE patients SET city = %s, updated_at = now()
           WHERE patient_id = (SELECT patient_id FROM patients ORDER BY random() LIMIT 1)""",
        (random.choice(CITIES),),
    )


def delete_booking(cur) -> None:
    cur.execute(
        """DELETE FROM appointments WHERE appointment_id = (
               SELECT a.appointment_id FROM appointments a
               WHERE a.status = 'booked' AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.appointment_id = a.appointment_id)
               ORDER BY random() LIMIT 1)"""
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--actions", type=int, default=300)
    parser.add_argument("--dirty", action="store_true", help="about 1 in 10 actions writes a bad row")
    parser.add_argument("--sleep", type=float, default=0.02, help="seconds between transactions")
    args = parser.parse_args()

    actions = [
        (new_patient, 15),
        (book, 35),
        (reschedule, 5),
        (finish, 30),
        (refund, 2),
        (move_city, 8),
        (delete_booking, 1),
    ]
    funcs, weights = zip(*actions)
    # autocommit=True so each conn.transaction() block is its own real transaction.
    with psycopg.connect(DSN, autocommit=True) as conn:
        for i in range(args.actions):
            action = random.choices(funcs, weights=weights)[0]
            dirty = args.dirty and random.random() < 0.1
            with conn.transaction(), conn.cursor() as cur:
                if action in (new_patient, book, finish):
                    action(cur, dirty)
                else:
                    action(cur)
            time.sleep(args.sleep)
            if (i + 1) % 50 == 0:
                print(f"{i + 1} actions written")
    print("done")


if __name__ == "__main__":
    main()
