{{ config(
    tags=["six_hourly"]
) }}

with base as (

    SELECT * 
    FROM {{ source('salesforce', 'user_role') }}

)

SELECT *
FROM base