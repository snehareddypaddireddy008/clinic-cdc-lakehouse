-- Monthly operating metrics per clinic, using the clinic's current name.
with appts as (
    select
        clinic_id,
        date_trunc('month', starts_at) as month,
        count(*)                              as appointments,
        count(*) filter (where is_completed)  as completed,
        count(*) filter (where is_cancelled)  as cancelled,
        count(*) filter (where is_no_show)    as no_shows,
        sum(reschedule_count)                 as reschedules
    from {{ ref('fct_appointments') }}
    group by all
),

revenue as (
    select
        a.clinic_id,
        date_trunc('month', a.starts_at) as month,
        sum(p.net_amount) as net_revenue
    from {{ ref('fct_payments') }} as p
    join {{ ref('fct_appointments') }} as a using (appointment_id)
    where not p.is_deleted
    group by all
)

select
    a.clinic_id,
    c.clinic_name,
    a.month,
    a.appointments,
    a.completed,
    a.cancelled,
    a.no_shows,
    a.reschedules,
    round(a.no_shows / nullif(a.completed + a.no_shows, 0), 4) as no_show_rate,
    round(a.cancelled / nullif(a.appointments, 0), 4)          as cancellation_rate,
    coalesce(r.net_revenue, 0)                                 as net_revenue
from appts as a
left join revenue as r using (clinic_id, month)
left join {{ ref('dim_clinic') }} as c
    on c.clinic_id = a.clinic_id and c.is_current
