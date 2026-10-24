-- One row per distinct change to appointments (redelivered Kafka messages removed).
{{ dedupe_changes(source('bronze', 'appointments'), 'appointment_id') }}
