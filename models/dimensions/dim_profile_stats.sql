{{ config(materialized="view") }}

SELECT 
    uid AS user_id,
    timezone_id
FROM {{ source('INTERVIEW', 'PROFILE_STATS') }}
