{{ config(materialized="view") }}

SELECT dtz.name AS country,
       AVG(first_post_at - onboarding_completed_at) AS avg_time_to_first_post
FROM  {{ ref('agg_first_post') }} AS afp
LEFT JOIN {{ ref('dim_users') }} AS du ON du.user_id = afp.user_id
LEFT JOIN {{ ref('dim_profile_stats') }} AS dps ON du.user_id = dps.user_id
LEFT JOIN {{ ref('dim_time_zone') }} AS dtz ON du.timezone_id = dtz.timezone_id
GROUP BY 1