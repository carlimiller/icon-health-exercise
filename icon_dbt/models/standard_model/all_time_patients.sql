{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = [''],
        tags = ['transformed_model']
    ) 
}}

with historical_patients as (
    select
        distinct patient_id
    from 
       {{ ref('dbt_std__encounters') }} en
)
select
    *
from
    historical_patients hp
union
    select distinct patient_id
from 
    {{ ref('dbt_std__referrals') }} rp
    on hp.patient_id = rp.patient_id
{% if is_incremental() %}
    where rp.updated_at > (select max(rp.updated_at) from {{ this }})
{% endif %}
