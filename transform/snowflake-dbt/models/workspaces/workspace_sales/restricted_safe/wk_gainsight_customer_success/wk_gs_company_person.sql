WITH source AS (

    SELECT *
    FROM {{ ref('company_person') }}

)

SELECT *
FROM source