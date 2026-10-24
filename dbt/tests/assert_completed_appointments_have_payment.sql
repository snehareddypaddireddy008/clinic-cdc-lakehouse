-- Warns (does not fail the build) when a completed appointment has no payment.
-- Expected cause: the payment was held in quarantine by the ingestion quality gate,
-- so this test and mart_data_quality_issues should point at the same records.
{{ config(severity='warn') }}

select a.appointment_id, a.clinic_id, a.starts_at
from {{ ref('fct_appointments') }} as a
left join {{ ref('fct_payments') }} as p
    on p.appointment_id = a.appointment_id and not p.is_deleted
where a.is_completed
  and p.payment_id is null
