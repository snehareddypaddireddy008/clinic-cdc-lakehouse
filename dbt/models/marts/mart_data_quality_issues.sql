-- What each owning team needs to fix, from the ingestion quality gate.
-- severity = error: change was held back from bronze; warn: loaded but flagged.
select
    owner,
    source_table,
    rule_name,
    severity,
    count(*)                          as issue_count,
    count(distinct record_key)        as records_affected,
    min(detected_at)                  as first_detected_at,
    max(detected_at)                  as last_detected_at,
    any_value(reason)                 as example_reason
from {{ ref('stg_quarantine') }}
group by all
