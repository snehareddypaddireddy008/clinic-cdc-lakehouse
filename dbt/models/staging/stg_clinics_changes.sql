-- One row per distinct change to clinics (redelivered Kafka messages removed).
{{ dedupe_changes(source('bronze', 'clinics'), 'clinic_id') }}
