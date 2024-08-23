{{ config(
    materialized='incremental',
    unique_key='id'
) }}

{{ level_up_incremental('email_captures') }}

parsed AS (
  SELECT
    value['notifiableType']::VARCHAR                        AS notifiable_type,
    value['courseSku']::VARCHAR                             AS course_sku,
    value['courseId']::VARCHAR                              AS course_id,
    value['companyId']::VARCHAR                             AS company_id,
    value['companyHost']::VARCHAR                           AS company_host,
    value['companySubdomain']::VARCHAR                      AS company_subdomain,
    value['courseTitle']::VARCHAR                           AS course_title,
    value['anonymousId']::VARCHAR                           AS anonymous_id,
    value['source']::VARCHAR                                AS source,
    value['notifiableId']::VARCHAR                          AS notifiable_id,
    value['timestamp']::TIMESTAMP                           AS event_timestamp,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,
    value['event']::VARCHAR                                 AS event, -- noqa: RF04
    SHA2(CONCAT(course_id, value['user'], event_timestamp)) AS id,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
