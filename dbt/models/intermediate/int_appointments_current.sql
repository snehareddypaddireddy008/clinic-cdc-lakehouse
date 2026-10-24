-- Current state of each appointment. Appointments deleted in the source are excluded.
{{ latest_state(ref('stg_appointments_changes'), 'appointment_id') }}
