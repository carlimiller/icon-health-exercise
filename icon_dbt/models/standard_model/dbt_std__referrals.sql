{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['patient_id','procedure_code','employer_id'],
        tags=['standard_model']
    ) 
}}

select
    model_name,
    cast(patient_id as text) as patient_id,
    gender,
    strftime('%Y-%m-%d', birth_date) as birth_date,
    case
        when strftime('%m-%d', 'now') < strftime('%m-%d', birth_date) then strftime('%y', 'now') - strftime('%y', birth_date) - 1
        else strftime('%y', 'now') - strftime('%y', birth_date)
    end as age,
    case
        when (strftime('%y', 'now') - strftime('%y', birth_date)) between 0 and 17 then '0-17'
        when (strftime('%y', 'now') - strftime('%y', birth_date)) between 18 and 34 then '18-34'
        when (strftime('%y', 'now') - strftime('%y', birth_date)) between 35 and 49 then '35-49'
        when (strftime('%y', 'now') - strftime('%y', birth_date)) between 50 and 64 then '50-64'
        when (strftime('%y', 'now') - strftime('%y', birth_date)) >= 65 then '65+'
        else 'unknown'
    end as age_bracket,
    cast(procedure_code as text) as procedure_code,
    substr(patient_zip_code, 1, 5) || '-' || substr(patient_zip_code, 6) as patient_zip_code,
    cast(substr(patient_zip_code, 1, 5) as text) AS patient_zip_code_trunc,
    cast(employer_id as text) as employer_id,
    cast(updated_at as timestamp) as updated_at,
    current_timestamp as create_date
from 
    {{ ref('mapped__referrals') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}