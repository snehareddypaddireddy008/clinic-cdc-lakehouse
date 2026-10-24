-- SCD Type 2 patient dimension, built from the CDC change log.
-- A new version starts when name, contact details, city or birth year change.
-- Contact details are hashed or masked: the marts never hold them in clear text.
with versions as (
    {{ scd2_from_changes(ref('stg_patients_changes'), 'patient_id',
                         ['full_name', 'email', 'phone', 'city', 'birth_year']) }}
)

select
    patient_sk,
    patient_id,
    full_name,
    {{ hash_email('email') }} as email_hash,
    {{ mask_phone('phone') }} as phone_masked,
    city,
    birth_year,
    valid_from,
    valid_to,
    is_current
from versions
