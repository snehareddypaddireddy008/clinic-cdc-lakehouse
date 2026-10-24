-- Grain: one row per appointment, current status.
-- Dimension keys are joined point-in-time: the patient, practitioner and clinic
-- versions that were valid when the booking was made.
with appts as (
    select a.*, l.booked_at, l.reschedule_count
    from {{ ref('int_appointments_current') }} as a
    join {{ ref('int_appointment_lifecycle') }} as l using (appointment_id)
)

select
    a.appointment_id,
    p.patient_sk,
    pr.practitioner_sk,
    c.clinic_sk,
    a.patient_id,
    a.practitioner_id,
    a.clinic_id,
    a.service_type,
    a.status,
    a.status = 'completed' as is_completed,
    a.status = 'cancelled' as is_cancelled,
    a.status = 'no_show'   as is_no_show,
    a.booked_at,
    a.starts_at,
    a.ends_at,
    date_diff('minute', a.starts_at, a.ends_at) as duration_minutes,
    date_diff('day', a.booked_at, a.starts_at)  as lead_time_days,
    a.reschedule_count,
    a.price as list_price,
    a._source_ts as last_changed_at
from appts as a
left join {{ ref('dim_patient') }} as p
    on p.patient_id = a.patient_id
   and a.booked_at >= p.valid_from
   and (p.valid_to is null or a.booked_at < p.valid_to)
left join {{ ref('dim_practitioner') }} as pr
    on pr.practitioner_id = a.practitioner_id
   and a.booked_at >= pr.valid_from
   and (pr.valid_to is null or a.booked_at < pr.valid_to)
left join {{ ref('dim_clinic') }} as c
    on c.clinic_id = a.clinic_id
   and a.booked_at >= c.valid_from
   and (c.valid_to is null or a.booked_at < c.valid_to)
