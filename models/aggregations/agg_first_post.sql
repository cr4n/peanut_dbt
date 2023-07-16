{{ config(materialized="table") }}

WITH posts AS (
    SELECT
        user_id,
        MIN(created_at) AS first_post_at
    FROM {{ ref('fct_posts') }}
    GROUP BY user_id
),
users AS (
    SELECT 
        du.user_id,
        du.onboarding_completed_at,
        dtz.area,
        dtz.location
    FROM {{ ref('dim_users') }} as du
    LEFT JOIN {{ ref('dim_profile_stats') }} AS dps ON du.user_id = dps.user_id
    LEFT JOIN {{ ref('dim_time_zone') }} AS dtz ON dps.timezone_id = dtz.timezone_id
    WHERE onboarding_completed_at IS NOT NULL
)

SELECT
    u.user_id,
    p.first_post_at,
    u.onboarding_completed_at,
    u.area,
    u.location,
    AVG(DATEDIFF('minute', u.onboarding_completed_at, p.first_post_at)) AS avg_minutes_to_first_post
FROM users AS u
INNER JOIN posts AS p ON u.user_id = p.user_id -- filters only UGC users
GROUP BY 1, 2, 3, 4, 5 
