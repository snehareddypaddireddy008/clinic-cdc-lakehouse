-- Quality issues raised by the ingestion gate. A batch retry can write the same issue twice; keep one.
select *
from {{ source('bronze', 'quarantine') }}
qualify row_number() over (
    partition by source_table, _kafka_partition, _kafka_offset, rule_name
    order by detected_at
) = 1
