{{ config(
    materialized="view"
) }}

SELECT 
    uid as user_id,
    TO_TIMESTAMP(created_at) AS created_at,
    comment_id,
    post_id,
    posted_as
FROM {{ source('INTERVIEW', 'COMMENTS') }}
