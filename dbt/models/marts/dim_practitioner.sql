-- SCD Type 2 practitioner dimension (practitioners change clinic or leave).
{{ scd2_from_changes(ref('stg_practitioners_changes'), 'practitioner_id',
                     ['clinic_id', 'full_name', 'discipline', 'is_active']) }}
