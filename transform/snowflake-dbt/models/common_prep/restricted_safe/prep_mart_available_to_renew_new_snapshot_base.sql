WITH base AS (

    SELECT *
    FROM {{ ref('mart_available_to_renew_new_snapshot') }} 

)

SELECT *
FROM base
