WITH source AS (

    SELECT *
    FROM {{ ref('user_unsubscribed_message_type_source') }}

)

SELECT *
FROM source