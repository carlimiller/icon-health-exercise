{{ 
    config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['employer_id','npi'],
    tags=['mapped_model']
)
}}

select
    '{{ this }}' as model_name,
    employer_id,
    npi,
    updated_at,
    current_timestamp as create_date
from
    {{ source('icon_health', 'employer_network_unpivot') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}