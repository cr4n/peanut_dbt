{{ config(materialized="table") }}

SELECT 
       DATE_TRUNC('Day', created_at) AS created_at,
       EXTRACT(HOUR FROM created_at) AS hour, 
       COUNT(*) AS num_posts
FROM {{ ref('fct_posts') }}
GROUP BY 1, 2
