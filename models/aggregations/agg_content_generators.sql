{{ config(materialized="table") }}

-- Other contents can be added easily to this model
WITH dau AS (
    SELECT 
        created_at,
        user_id
    FROM {{ ref('fct_daily_active_users') }}
),

post_counts AS (
    SELECT
        DATE_TRUNC('Day', created_at) AS created_at,
        COUNT(DISTINCT user_id) AS num_posts
    FROM {{ ref('fct_posts')}}
    GROUP BY 1
)

SELECT dau.created_at,
       COALESCE(num_posts, 0) AS num_posts,
       COUNT(DISTINCT user_id) AS num_users,
       (COALESCE(num_posts, 0) / COUNT(DISTINCT user_id)) * 100 AS percent_posting_users
FROM dau
LEFT JOIN post_counts
ON dau.created_at = post_counts.created_at
GROUP BY 1, 2
