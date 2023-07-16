{{ config(materialized="table") }}

SELECT
    DATE_TRUNC('MONTH', created_at) created_at,
    COUNT(DISTINCT user_id) AS mau
FROM
    {{ ref('fct_monthly_active_users') }}
GROUP BY 1