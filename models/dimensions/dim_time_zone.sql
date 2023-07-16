{{ config(
    materialized="view"
 ) }}

SELECT 
    timezone_id,
    name
FROM {{ source('INTERVIEW', 'TIME_ZONE') }}
