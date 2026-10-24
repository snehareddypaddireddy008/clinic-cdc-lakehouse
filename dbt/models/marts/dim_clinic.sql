-- SCD Type 2 clinic dimension (clinics get renamed and move).
{{ scd2_from_changes(ref('stg_clinics_changes'), 'clinic_id', ['clinic_name', 'city', 'province']) }}
