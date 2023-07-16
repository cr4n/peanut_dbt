{{ config(materialized="view") }}

WITH user_activity AS (
    SELECT DISTINCT
        user_id,
        DATE_TRUNC('DAY', created_at) AS day
    FROM 
        {{ ref('fct_daily_active_users') }} 
),

user_activity_28days AS (
    SELECT
        user_id,
        day,
        COUNT(*) OVER (
            PARTITION BY user_id 
            ORDER BY day
            ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
        ) AS active_days_in_last_28
    FROM
        user_activity
)

SELECT 
    DATE_TRUNC('MONTH', day) AS created_at,
    user_id
FROM user_activity_28days
WHERE active_days_in_last_28 > 0
