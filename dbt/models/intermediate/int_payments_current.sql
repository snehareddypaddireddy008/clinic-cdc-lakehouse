-- Current state of each payment (captured or refunded).
{{ latest_state(ref('stg_payments_changes'), 'payment_id') }}
