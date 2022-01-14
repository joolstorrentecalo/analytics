WITH source AS (

    SELECT
      id                            AS id,
      cookie_id                     AS cookie_id,
      visitor_id                    AS visitor_id,
      session_id                    AS session_id,
      event_date                    AS event_date,
      modified_date                 AS modified_date,
      current_page                  AS current_page,
      current_page_raw              AS current_page_raw,
      ip_address                    AS ip_address,
      type                          AS type,
      user_agent_string             AS user_agent_string,
      client_sequence               AS client_sequence,
      client_random                 AS client_random,
      is_duplicated                 AS is_duplicated,
      is_processed                  AS is_processed,
      mapping_type                  AS mapping_type,
      mapping_org_id                AS mapping_org_id,
      mapping_cookie_id             AS mapping_cookie_id,
      _created_date                 AS _created_date,
      _modified_date                AS _modified_date,
      _deleted_date                 AS _deleted_date
    FROM {{ source('bizible', 'biz_web_host_mappings') }}
    ORDER BY uploaded_at DESC

)

SELECT *
FROM source