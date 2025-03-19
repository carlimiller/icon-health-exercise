{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['encounter_id','npi','patient_id','cpt'],
        tags=['standard_model']
    ) 
}}

select
    model_name,
    cast(encounter_id as text) as encounter_id,
    cast(provider_npi as text) as provider_npi,
    cast(patient_id as text) as patient_id,
    cast(procedure_code as text) as procedure_code,
    cast(paid_amount as int) as paid_amount, 
    cast(updated_at as timestamp) as updated_at,
    current_timestamp as create_date
from 
    {{ ref('mapped__encounters') }}
{% if is_incremental() %}
where
    updated_at > (select max(updated_at) from {{ this }})
{% endif %}