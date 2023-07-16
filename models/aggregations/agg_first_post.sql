{{ config(
    materialized="table"
 ) }}

WITH posts AS (
    SELECT
        user_id,
        MIN(created_at) AS first_post_at
    FROM {{ ref('fct_posts') }}
    GROUP BY user_id
),
users AS (
    SELECT 
        user_id,
        onboarding_completed_at
    FROM {{ ref('dim_users') }} as users 
)

SELECT
    u.user_id,
    first_post_at,
    AVG(DATEDIFF('minute', u.onboarding_completed_at, fp.first_post_at)) AS avg_time_to_first_post
FROM users AS u
LEFT JOIN posts AS p ON u.user_id = p.user_id
GROUP BY 1, 2
HAVING avg_time_to_first_post > 0
