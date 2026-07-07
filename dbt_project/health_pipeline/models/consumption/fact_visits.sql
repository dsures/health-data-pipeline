{{ config(
    materialized='incremental',
    unique_key='visit_id'
) }}

/*
    Fact_Visits: one row per patient visit.
    Grain: visit_id. FK to dim_patient.patient_id.
*/

WITH uat AS (
    SELECT * FROM {{ ref('uat_visit') }}
)

SELECT
    visit_id,
    patient_id,
    visit_date,
    visit_type,
    provider_name,
    diagnosis_code,
    cost,
    duration_minutes
FROM uat

{% if is_incremental() %}
WHERE visit_date > (SELECT MAX(visit_date) FROM {{ this }})
{% endif %}