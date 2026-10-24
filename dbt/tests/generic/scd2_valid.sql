{#- An SCD Type 2 table is valid when, per business key:
    - at most one version is current,
    - versions never overlap (a gap is fine: the record was deleted for a while),
    - no version ends before it starts.
    Returns the offending keys. -#}
{% test scd2_valid(model, key) %}
    with ordered as (
        select
            {{ key }},
            valid_from,
            valid_to,
            lead(valid_from) over (partition by {{ key }} order by valid_from) as next_valid_from
        from {{ model }}
    ),

    multiple_current as (
        select {{ key }}, 'more than one current version' as problem
        from {{ model }}
        where is_current
        group by {{ key }}
        having count(*) > 1
    ),

    overlapping as (
        select {{ key }}, 'versions overlap' as problem
        from ordered
        where next_valid_from is not null
          and (valid_to is null or valid_to > next_valid_from)
    ),

    inverted as (
        select {{ key }}, 'valid_to before valid_from' as problem
        from {{ model }}
        where valid_to is not null and valid_to < valid_from
    )

    select * from multiple_current
    union all select * from overlapping
    union all select * from inverted
{% endtest %}
