{{ config(
    materialized="view"
 ) }}

select 
    uid AS user_id,
    TO_TIMESTAMP(created_at) AS created_at,
    TO_TIMESTAMP(completed_reg_at) AS onboarding_completed_at
from {{ source('INTERVIEW', 'USERS') }}
