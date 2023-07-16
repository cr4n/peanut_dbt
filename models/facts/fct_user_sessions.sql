{{ config(materialized="view") }}

SELECT 
    session_id,
    uid AS user_id,
    TO_TIMESTAMP(created_at) AS created_at,
    TO_TIMESTAMP(updated_at) AS updated_at
FROM {{ source('INTERVIEW', 'USER_SESSIONS') }}
