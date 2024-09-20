{{ level_up_intermediate('course_completions') }}

parsed AS (
  SELECT
    value['id']::VARCHAR                        AS course_completion_id,
    value['companyId']::VARCHAR                 AS company_id,
    value['source']::VARCHAR                    AS course_action,
    value['event']::VARCHAR                     AS event, -- noqa: RF04
    value['timestamp']::TIMESTAMP               AS event_timestamp,
    value['license']::VARCHAR                   AS license,
    value['notifiableId']::VARCHAR              AS notifiable_id,
    value['title']::VARCHAR                     AS title,
    value['type']::VARCHAR                      AS transaction_type,
    value['updatedAt']::TIMESTAMP               AS updated_at,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,
    value['userDetail']['id']::VARCHAR          AS user_id,

    value['userDetail']['state']::VARCHAR       AS user_state,
    value['userDetail']['country']::VARCHAR     AS user_country,
    value['userDetail']['client']::VARCHAR      AS user_client,

    value['userDetail']['ref1']::VARCHAR        AS ref1_user_type,
    value['userDetail']['ref2']::VARCHAR        AS ref2_user_job,
    value['userDetail']['ref4']::VARCHAR        AS ref4_user_company,
    value['userDetail']['ref6']::VARCHAR        AS ref6_user_role_type,
    value['userDetail']['ref7']::VARCHAR        AS ref7_user_continent,
    value['userDetail']['ref8']::VARCHAR        AS ref8_user_country,
    value['userDetail']['ref9']::VARCHAR        AS ref9_user_sub_dept,
    value['userDetail']['ref10']::VARCHAR       AS ref10_user_dept,

    value['userDetail']['sfAccountId']::VARCHAR AS sf_account_id,
    value['userDetail']['sfContactId']::VARCHAR AS sf_contact_id,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        course_completion_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
