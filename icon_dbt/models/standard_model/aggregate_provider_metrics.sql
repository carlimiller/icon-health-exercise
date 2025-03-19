{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['provider_npi', 'gender', 'age_bracket','procedure_code'],
        tags = ['transformed_model']
    ) 
}}

with granular_aggregates as (
    select
        en.provider_npi,
        en.procedure_code,
        pt.gender as patient_gender,
        pt.age_bracket as patient_age_bracket,
        count(distinct en.encounter_id) as total_procedures,
        count(distinct ae.encounter_id) as total_adverse_events,
        count(distinct en.patient_id) as total_patients,
        sum(en.paid_amount) as total_paid_amount,
        sum(en.paid_amount)/count(distinct en.encounter_id) as avg_cost_per_procedure,
        sum(en.paid_amount)/count(distinct en.patient_id) as avg_cost_per_patient,
        sum(distinct en.encounter_id)/count(distinct en.patient_id) as avg_procedures_per_pt
    from
        {{ ref('dbt_std__encounters') }} en
        left join {{ ref('dbt_std__patients') }} pt
        on en.patient_id = pt.patient_id
        left join {{ ref('dbt_std__adverse_events') }} ae
        on en.encounter_id = ae.encounter_id
    {% if is_incremental() %}
        where en.updated_at > (select max(en.updated_at) from {{ this }})
    {% endif %}
    group by
        en.provider_npi,
        en.procedure_code,
        pt.gender,
        pt.age_bracket
)
select
    ga.provider_npi,
    ga.procedure_code,
    ga.patient_gender,
    ga.patient_age_bracket,
    ga.total_procedures,
    ga.total_adverse_events,
    ga.total_patients,
    ga.total_paid_amount,
    ga.avg_cost_per_procedure,
    ga.avg_cost_per_patient,
    ga.avg_procedures_per_pt,
    --npi metrics
    sum(ga.total_procedures) over (partition by ga.provider_npi) as npi_total_procedures,
    sum(ga.total_adverse_events) over (partition by ga.provider_npi) as npi_total_adverse_events,
    sum(ga.total_patients) over (partition by ga.provider_npi) as npi_total_patients,
    sum(ga.total_paid_amount) over (partition by ga.provider_npi) as npi_total_paid_amount,
    sum(ga.total_paid_amount)/sum(ga.total_procedures) over (partition by ga.provider_npi) as npi_avg_cost_per_procedure,
    sum(ga.total_paid_amount)/sum(ga.total_patients) over (partition by ga.provider_npi) as npi_avg_cost_per_patient,
    sum(ga.total_procedures)/sum(ga.total_patients) over (partition by ga.provider_npi) as npi_avg_procedures_per_pt,
    --age metrics
    sum(ga.total_procedures) over (partition by ga.patient_age_bracket) as age_total_procedures,
    sum(ga.total_adverse_events) over (partition by ga.patient_age_bracket) as age_total_adverse_events,
    sum(ga.total_patients) over (partition by ga.patient_age_bracket) as age_total_patients,
    sum(ga.total_paid_amount) over (partition by ga.patient_age_bracket) as age_total_paid_amount,
    sum(ga.total_paid_amount)/sum(ga.total_procedures) over (partition by ga.patient_age_bracket) as age_avg_cost_per_procedure,
    sum(ga.total_paid_amount)/sum(ga.total_patients) over (partition by ga.patient_age_bracket) as age_avg_cost_per_patient,
    sum(ga.total_procedures)/sum(ga.total_patients) over (partition by ga.patient_age_bracket) as age_avg_procedures_per_pt,
    --gender metrics
    sum(ga.total_procedures) over (partition by ga.patient_gender) as gender_total_procedures,
    sum(ga.total_adverse_events) over (partition by ga.patient_gender) as gender_total_adverse_events,
    sum(ga.total_patients) over (partition by ga.patient_gender) as gender_total_patients,
    sum(ga.total_paid_amount) over (partition by ga.patient_gender) as gender_total_paid_amount,
    sum(ga.total_paid_amount)/sum(ga.total_procedures) over (partition by ga.patient_gender) as gender_avg_cost_per_procedure,
    sum(ga.total_paid_amount)/sum(ga.total_patients) over (partition by ga.patient_gender) as gender_avg_cost_per_patient,
    sum(ga.total_procedures)/sum(ga.total_patients) over (partition by ga.patient_gender) as gender_avg_procedures_per_pt,
    --procedure metrics
    sum(ga.total_procedures) over (partition by ga.procedure_code) as cpt_total_procedures,
    sum(ga.total_adverse_events) over (partition by ga.procedure_code) as cpt_total_adverse_events,
    sum(ga.total_patients) over (partition by ga.procedure_code) as cpt_total_patients,
    sum(ga.total_paid_amount) over (partition by ga.procedure_code) as cpt_total_paid_amount,
    sum(ga.total_paid_amount)/sum(ga.total_procedures) over (partition by ga.procedure_code) as cpt_avg_cost_per_procedure,
    sum(ga.total_paid_amount)/sum(ga.total_patients) over (partition by ga.procedure_code) as cpt_avg_cost_per_patient,
    sum(ga.total_procedures)/sum(ga.total_patients) over (partition by ga.procedure_code) as cpt_avg_procedures_per_pt
from
    granular_aggregates ga
