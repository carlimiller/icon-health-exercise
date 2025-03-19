{{ 
    config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'patient_id',
    tags=['mapped_model']
)
}}

select
    '{{ this }}' as model_name,
    patient_id,
    gender,
    birth_date,
    updated_at,
    current_timestamp as create_date
from
    {{ source('icon_health', 'patients') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}
