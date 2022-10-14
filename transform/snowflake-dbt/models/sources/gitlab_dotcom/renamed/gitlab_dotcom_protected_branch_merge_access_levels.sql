WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_protected_branch_merge_access_levels_dedupe_source') }}

),

renamed AS (

SELECT 
    id::NUMBER AS protected_branch_merge_access_levels_id,
    protected_branch_id::NUMBER as protected_branch_id,
    access_level::NUMBER AS access_level,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    user_id::NUMBER AS user_id,
    group_id::NUMBER AS group_id
    FROM source
)

SELECT *
FROM renamed
