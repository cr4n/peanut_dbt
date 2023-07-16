{{ config(materialized="table") }}

SELECT
    created_at,
    COUNT(DISTINCT user_id) AS dau
FROM
    {{ ref('fct_daily_active_users') }}
GROUP BY 1