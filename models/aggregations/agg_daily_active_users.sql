{{ config(
    materialized="incremental",
    unique_key="created_at || '-' || user_id"
) }}

SELECT
    created_at,
    COUNT(DISTINCT user_id) AS dau
FROM
    {{ ref('fct_daily_active_users') }}
{% if is_incremental() %} -- reduces data read up to last sync 
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
GROUP BY 1