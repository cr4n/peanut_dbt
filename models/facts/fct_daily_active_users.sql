{{ config(
    materialized="incremental",
    unique_key="user_id || '-' || created_at"
) }}

SELECT DISTINCT
    user_id,
    DATE_TRUNC('DAY', created_at) AS created_at
FROM 
    {{ ref('fct_user_sessions') }}
WHERE 
    DATEDIFF(second, created_at, updated_at) > 5
{% if is_incremental() %}
    AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
