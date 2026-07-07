{{ config(materialized='table') }}

/*
    refers to RAW visit table, extracts JSON and applies type/validity checks
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'visit') }}
),

extracted AS (
    SELECT
        raw_data:visit_id::VARCHAR(255) AS visit_id,
        raw_data:patient_id::VARCHAR(255) AS patient_id,

        CASE
            WHEN try_to_date(raw_data:visit_date::STRING) > current_date()
                THEN NULL
            ELSE try_to_date(raw_data:visit_date::STRING)
        END AS visit_date,

        raw_data:visit_type::STRING AS visit_type,
        raw_data:provider_name::STRING AS provider_name,
        raw_data:diagnosis_code::STRING AS diagnosis_code,

        CASE
            WHEN raw_data:cost::FLOAT < 0 THEN NULL
            ELSE raw_data:cost::FLOAT
        END AS cost,

        raw_data:duration_minutes::NUMBER AS duration_minutes

    FROM source
)

SELECT * FROM extracted