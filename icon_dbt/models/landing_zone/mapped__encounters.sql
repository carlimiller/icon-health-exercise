{{ 
    config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['encounter_id','npi','patient_id','cpt'],
    tags=['mapped_model']
)
}}

select
    '{{ this }}' as model_name,
    encounter_id,
    npi as provider_npi,
    patient_id,
    cpt as procedure_code,
    paid_amount,
    updated_at,
    current_timestamp as create_date
from
    {{ source('icon_health', 'encounters') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}