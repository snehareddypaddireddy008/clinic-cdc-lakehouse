-- One row per distinct change to patients (redelivered Kafka messages removed).
{{ dedupe_changes(source('bronze', 'patients'), 'patient_id') }}
