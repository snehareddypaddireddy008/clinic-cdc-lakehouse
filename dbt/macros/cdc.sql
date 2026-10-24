{#
  Helpers for turning an append-only CDC change log into usable tables.
  Ordering uses the Postgres LSN, with the Kafka offset as a tie-breaker.
#}

{% macro dedupe_changes(relation, key) %}
    {#- The consumer is at-least-once, so a change can arrive twice.
        A change is identified by (key, _lsn, _op); keep one copy. -#}
    select *
    from {{ relation }}
    qualify row_number() over (
        partition by {{ key }}, _lsn, _op
        order by _kafka_offset
    ) = 1
{% endmacro %}


{% macro latest_state(relation, key) %}
    {#- Current state: the latest change per key, minus keys whose latest change is a delete. -#}
    select * exclude (_change_rank)
    from (
        select
            *,
            row_number() over (partition by {{ key }} order by _lsn desc, _kafka_offset desc) as _change_rank
        from {{ relation }}
    )
    where _change_rank = 1
      and not _is_deleted
{% endmacro %}


{% macro scd2_from_changes(relation, key, tracked_columns) %}
    {#- SCD Type 2 built straight from the change log.
        A new version starts only when a tracked column changes (updated_at-only changes are ignored).
        A delete closes the open version. -#}
    with ordered as (
        select
            *,
            md5(concat_ws('|', {% for c in tracked_columns %}coalesce(cast({{ c }} as varchar), '~'){% if not loop.last %}, {% endif %}{% endfor %})) as _attr_hash
        from {{ relation }}
    ),

    flagged as (
        select
            *,
            lag(_attr_hash) over (partition by {{ key }} order by _lsn, _kafka_offset) as _prev_hash,
            lag(_is_deleted) over (partition by {{ key }} order by _lsn, _kafka_offset) as _prev_deleted
        from ordered
    ),

    version_starts as (
        -- Keep the first change, every tracked-attribute change, every delete, and re-inserts after a delete.
        select *
        from flagged
        where _prev_hash is null
           or _is_deleted
           or _prev_deleted
           or _attr_hash <> _prev_hash
    ),

    bounded as (
        select
            *,
            _source_ts as valid_from,
            lead(_source_ts) over (partition by {{ key }} order by _lsn, _kafka_offset) as valid_to
        from version_starts
    )

    select
        md5(concat_ws('|', cast({{ key }} as varchar), cast(_lsn as varchar))) as {{ key | replace('_id', '') }}_sk,
        {{ key }},
        {% for c in tracked_columns %}{{ c }},
        {% endfor %}valid_from,
        valid_to,
        valid_to is null as is_current
    from bounded
    where not _is_deleted   -- the delete row itself only serves to close the previous version
{% endmacro %}
