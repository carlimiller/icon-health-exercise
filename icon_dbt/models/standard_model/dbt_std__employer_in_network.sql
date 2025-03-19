{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['employer_id', 'npi'],
        tags=['standard_model']
    ) 
}}

select
    model_name,
    cast(employer_id as text) as employer_id,
    cast(npi as text) as npi,
    cast(updated_at as timestamp) as updated_at,
    current_timestamp as create_date
from 
    {{ ref('mapped__employer_in_network') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}