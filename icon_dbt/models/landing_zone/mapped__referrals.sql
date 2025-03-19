{{ 
    config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['patient_id','procedure_code','employer_id'],
    tags=['mapped_model']
)
}}

select
    '{{ this }}' as model_name,
    patient_id,
    gender,
    birth_date,
    procedure as procedure_code,
    zip as patient_zip_code,
    employer_id,
    updated_at,
    current_timestamp as create_date
from
    {{ source('icon_health', 'referrals') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}
