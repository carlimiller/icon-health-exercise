{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'npi',
        tags=['standard_model']
    ) 
}}

select
    model_name,
    cast(provider_npi as text) as provider_npi,
    provider_name,
    substr(provider_zip_code, 1, 5) || '-' || substr(provider_zip_code, 6) as provider_zip_code,
    cast(substr(provider_zip_code, 1, 5) as text) AS provider_zip_code_trunc,
    cast(updated_at as timestamp) as updated_at,
    current_timestamp as create_date
from 
    {{ ref('mapped__providers') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}