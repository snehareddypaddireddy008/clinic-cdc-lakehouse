-- One row per distinct change to practitioners (redelivered Kafka messages removed).
{{ dedupe_changes(source('bronze', 'practitioners'), 'practitioner_id') }}
