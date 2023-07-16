{{ config(materialized="view") }}

SELECT DISTINCT
    user_id,
    DATE_TRUNC('DAY', created_at) AS created_at
FROM 
    {{ ref('fct_user_sessions') }} 
WHERE 
    DATEDIFF(second, created_at, updated_at) > 5
