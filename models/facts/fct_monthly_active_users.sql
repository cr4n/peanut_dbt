{{ config(
    materialized="view"
 ) }}

SELECT 
    user_id,
    DATE_TRUNC('month', created_at) AS created_at,
    COUNT(*) AS active_days
FROM 
    {{ ref('fct_daily_active_users') }}
WHERE 
    DATEADD(day, -28, CURRENT_DATE) <= created_at
GROUP BY 1, 2
