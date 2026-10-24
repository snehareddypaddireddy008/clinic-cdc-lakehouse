-- One row per distinct change to payments (redelivered Kafka messages removed).
{{ dedupe_changes(source('bronze', 'payments'), 'payment_id') }}
