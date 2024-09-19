{{ level_up_incremental('code_redemptions') }}

parsed AS (
  SELECT
    value['companyHost']::varchar as company_host,
    value['companyId']::varchar as company_id,
    value['companySubdomain']::varchar as company_subdomain,
    value['event']::varchar as event,
    value['redemptionCode']::varchar as redemption_code,
    value['redemptionCodeGroupLabel']::varchar as redemption_code_group_label,
    value['timestamp']::timestamp as event_timestamp,
    {{ level_up_filter_gitlab_email("value['user']") }} as username,
    value['userDetail']['id']::varchar as user_id,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        redemption_code_group_label,
        redemption_code,
        event_timestamp
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
