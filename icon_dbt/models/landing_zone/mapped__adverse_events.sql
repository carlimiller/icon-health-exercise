{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'encounter_id',
        tags=['mapped_model']
    ) 
}}

select
    '{{ this }}' AS model_name,
    encounter_id,
    updated_at,
    current_timestamp as create_date
from
    {{ source('icon_health', 'adverse_events') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}