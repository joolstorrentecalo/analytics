{{ config({
    "materialized": "incremental",
    "unique_key": "user_id"
    })
}}

WITH
source AS (
  SELECT
    value,
    uploaded_at
  FROM
    {{ source('clari', 'net_arr') }},
    LATERAL FLATTEN(input => jsontext:data:users)

  {% if is_incremental() %}
    WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
  {% endif %}
),

parsed AS (
  SELECT
    -- primary key
    uploaded_at,

    -- logical info
    value:userId::varchar AS user_id,
    value:crmId::varchar AS crm_user_id,
    value:email::varchar AS email,
    value:parentHierarchyId::varchar AS parent_role_id,
    value:parentHierarchyName::varchar AS parent_role,
    value:hierarchyId::varchar AS sales_team_role_id,
    value:hierarchyName::varchar AS sales_team_role,
    value:name::varchar AS user_full_name,

    value:scopeId::variant AS scope_id
  FROM
    source
  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT
  *
FROM
  parsed
