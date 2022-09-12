WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_license_version') }}

), renamed AS (

    SELECT
      item_id::NUMBER                                     AS item_id,
      created_at::TIMESTAMP                               AS created_at,
      item_type::VARCHAR                                  AS item_type,
      event::VARCHAR                                      AS event,
      whodounnit::VARCHAR                                 AS whodounnit,
      object::VARCHAR                                     AS object,
      object_changes::VARCHAR                             AS object_changes,
      DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP AS _uploaded_at
    FROM source

)

SELECT *
FROM renamed