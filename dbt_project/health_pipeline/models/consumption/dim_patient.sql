{{ config(
    materialized='incremental',
    unique_key='patient_id',
    incremental_strategy='merge'
) }}

WITH uat AS (
    SELECT * FROM {{ ref('uat_patient') }}
),

dim AS (
    SELECT
        patient_id::VARCHAR(255)              AS patient_id,
        full_name_hash::VARCHAR(200)           AS full_name,
        birth_date::DATE                       AS birth_date,
        gender::VARCHAR(20)                    AS gender,
        address_hash::VARCHAR(255)             AS address,
        marital_status_code::VARCHAR(20)       AS marital_status,
        insurance_number_hash::VARCHAR(255)    AS insurance_number,
        insurance_provider::VARCHAR(255)       AS insurance_provider,
        nationality::VARCHAR(20)               AS nationality,
        telecom_phone:value::VARCHAR(255)      AS telecom_phone,
        telecom_email:value::VARCHAR(255)      AS telecom_email
    FROM uat
)

SELECT * FROM dim