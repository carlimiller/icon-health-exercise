{{ 
    config(
        materialized = 'view',
        tags = ['enriched_view']
    ) 
}}

select
    {{ dbt_utils.star(from=ref('aggregate_provider_metrics')) }},
    cpt.cpt_description as procedure_name
from
    {{ ref('aggregate_provider_metrics') }} apm
    left join {{ ref('cpt_codes') }} cpt
    on apm.procedure_code = cpt.cpt_code