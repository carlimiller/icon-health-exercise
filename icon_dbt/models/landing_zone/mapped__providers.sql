{{ 
    config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'npi',
    tags=['mapped_model']
)
}}

select
    '{{ this }}' as model_name,
    npi as provider_npi,
    provider_name,
    zip as provider_zip_code,
    updated_at,
    current_timestamp as create_date
from
    {{ source('icon_health', 'providers') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}