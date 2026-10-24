{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='delete+insert'
    )
}}
-- Grain: one row per payment. Incremental: each run reads only changes with an
-- LSN above the highest one already loaded, then replaces those payments.
-- Source deletes are kept as soft deletes (is_deleted) so an incremental run can apply them.
with changes as (
    select *
    from {{ ref('stg_payments_changes') }}
    {% if is_incremental() %}
    where _lsn > (select coalesce(max(_last_lsn), 0) from {{ this }})
    {% endif %}
),

latest as (
    select *
    from changes
    qualify row_number() over (partition by payment_id order by _lsn desc, _kafka_offset desc) = 1
)

select
    payment_id,
    appointment_id,
    amount,
    case when status = 'refunded' then -amount else amount end as net_amount,
    method,
    status,
    paid_at,
    _is_deleted as is_deleted,
    _lsn as _last_lsn,
    _source_ts as last_changed_at
from latest
