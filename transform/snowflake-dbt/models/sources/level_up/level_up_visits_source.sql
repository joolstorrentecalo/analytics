{{ level_up_intermediate('visits') }}

parsed AS (
  SELECT
    value['companyHost']::VARCHAR                AS company_host,
    value['companyId']::VARCHAR                  AS company_id,
    value['companySubdomain']::VARCHAR           AS company_subdomain,
    value['event']::VARCHAR                      AS event, -- noqa: RF04
    value['timestamp']::TIMESTAMP                AS event_timestamp,

    value['userDetail']['id']::VARCHAR           AS user_id,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,
    value['userDetail']['clientId']::VARCHAR     AS user_client_id,
    value['userDetail']['departmentId']::VARCHAR AS user_department_id,

    value['userDetail']['ref1']::VARCHAR         AS ref1_user_type,
    value['userDetail']['ref2']::VARCHAR         AS ref2_user_job,

    value['userDetail']['sfAccountId']::VARCHAR  AS sf_account_id,
    value['userDetail']['sfContactId']::VARCHAR  AS sf_contact_id,
    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        event_timestamp
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
