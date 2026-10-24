-- Facts that only the change history can tell us: when the booking was made
-- and how many times it was rescheduled. A current-state table loses both.
with changes as (
    select
        appointment_id,
        _op,
        _source_ts,
        starts_at,
        lag(starts_at) over (partition by appointment_id order by _lsn, _kafka_offset) as prev_starts_at
    from {{ ref('stg_appointments_changes') }}
    where not _is_deleted
)

select
    appointment_id,
    min(_source_ts) as booked_at,
    count(*) filter (where prev_starts_at is not null and starts_at <> prev_starts_at) as reschedule_count
from changes
group by appointment_id
