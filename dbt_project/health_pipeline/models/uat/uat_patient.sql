{{ config(materialized='table') }}

/*
    refers to RAW table, does transformation (hashing, deterministic ID creation) and mapping
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'patient') }}   -- was: 'raw_raw_patient'
),

extracted AS (
    SELECT
        loaded_at,
        source_sheet_id,

        raw_data:city::STRING AS city,

        raw_data:state::STRING AS state,
        raw_data:nationality::STRING AS nationality,

        raw_data:blood_type::STRING AS blood_type,

        raw_data:allergies::STRING AS allergies,

        raw_data:preferred_language::STRING AS preferred_language,

        raw_data:address::STRING                AS address,
        raw_data:zip_code::STRING               AS zip_code,
        raw_data:emergency_contact_name::STRING AS emergency_contact_name,
        raw_data:emergency_contact_phone::STRING AS emergency_contact_phone,
        raw_data:insurance_provider::STRING     AS insurance_provider,

        md5(
            coalesce(raw_data:first_name::STRING, '')
            || coalesce(raw_data:last_name::STRING, '')
            || coalesce(raw_data:birth_date::STRING, '')
        ) AS patient_id,

        md5(
            coalesce(raw_data:first_name::STRING, '') || ' '
            || coalesce(raw_data:last_name::STRING, '')
        ) AS full_name_hash,

        CASE
            WHEN lower(trim(raw_data:gender::STRING)) IN ('male', 'm')
                THEN 'male'
            WHEN lower(trim(raw_data:gender::STRING)) IN ('female', 'f')
                THEN 'female'
            WHEN lower(trim(raw_data:gender::STRING)) IN ('other', 'non-binary', 'nonbinary', 'nb')
                THEN 'other'
            WHEN
                lower(trim(raw_data:gender::STRING)) IN ('unknown', '')
                OR raw_data:gender::STRING IS NULL
                THEN 'unknown'
        END AS gender,

        CASE
            WHEN try_to_date(raw_data:birth_date::STRING) > current_date()
                THEN NULL
            ELSE try_to_date(raw_data:birth_date::STRING)
        END AS birth_date,

        date_trunc('year', try_to_date(raw_data:birth_date::STRING)) AS birth_year,

        CASE
            WHEN lower(trim(raw_data:marital_status::STRING)) IN ('married', 'm')
                THEN 'M'
            WHEN lower(trim(raw_data:marital_status::STRING)) IN ('single', 's')
                THEN 'S'
            WHEN lower(trim(raw_data:marital_status::STRING)) IN ('divorced', 'd')
                THEN 'D'
            WHEN lower(trim(raw_data:marital_status::STRING)) IN ('widowed', 'w')
                THEN 'W'
            ELSE 'UNK'
        END AS marital_status_code,

        md5(coalesce(raw_data:address::STRING, '')) AS address_hash,

        object_construct(
            'system', 'phone',
            'value', md5(coalesce(raw_data:phone_number::STRING, '')),
            'use', 'home'
        ) AS telecom_phone,
        object_construct(
            'system', 'email',
            'value', md5(coalesce(raw_data:email::STRING, '')),
            'use', 'home'
        ) AS telecom_email,
        md5(coalesce(raw_data:insurance_number::STRING, '')) AS insurance_number_hash,
        try_to_date(raw_data:last_visit_date::STRING) AS last_visit_date

    FROM source
)

SELECT * FROM extracted
