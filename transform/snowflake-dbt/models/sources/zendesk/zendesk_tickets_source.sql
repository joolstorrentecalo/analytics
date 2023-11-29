WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_tickets_dedupe_source') }}
),

renamed AS (

    SELECT
      --ids
      id                                      AS ticket_id,
      organization_id,
      assignee_id,
      brand_id,
      group_id,
      requester_id,
      submitter_id,
      ticket_form_id::NUMBER                  AS ticket_form_id,

      --fields
      status                                  AS ticket_status,
      lower(priority)                         AS ticket_priority,
      md5(subject)                            AS ticket_subject,
      md5(recipient)                          AS ticket_recipient,
      url                                     AS ticket_url,
      tags                                    AS ticket_tags,
      description                             AS ticket_description,
      type                                    AS ticket_type,
      -- added ':score'
      satisfaction_rating['id']::VARCHAR      AS satisfaction_rating_id,
      satisfaction_rating['score']::VARCHAR   AS satisfaction_rating_score,
      via['channel']::VARCHAR                 AS submission_channel,
      --IFF(custom_fields='[{}]','',custom_fields) AS ticket_custom_field_values,
      --custom_fields::VARCHAR                  AS ticket_custom_field_values,
      --dates
      updated_at::DATE                        AS date_updated,
      created_at                              AS ticket_created_at

    FROM source

)

SELECT *
FROM renamed
