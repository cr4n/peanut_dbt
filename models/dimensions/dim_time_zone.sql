{{ config(materialized="view") }}

SELECT 
    timezone_id,
    TRIM(SPLIT_PART(name, '/', 1)) AS area,
    REPLACE(TRIM(SPLIT_PART(name, '/', 2)), '_', ' ')  AS location
FROM {{ source('INTERVIEW', 'TIME_ZONE') }}
