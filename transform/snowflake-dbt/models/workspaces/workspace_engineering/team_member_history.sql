WITH dates AS (

  SELECT date_actual
  FROM {{ ref('dim_date') }}
  WHERE date_actual >= '2020-12-01'

),

category AS (

  SELECT
    SPLIT_PART(LOWER(group_name), ':', 1)     AS group_name,
    LOWER(stage_display_name)                 AS stage_display_name,
    stage_section,
    MIN(snapshot_date)                        AS valid_from,
    MAX(snapshot_date)                        AS valid_to,
    ROW_NUMBER() OVER (
      PARTITION BY SPLIT_PART(LOWER(group_name), ':', 1)
      ORDER BY valid_to
    )                                         AS rn,
    IFF(valid_to = CURRENT_DATE, TRUE, FALSE) AS is_current
  FROM {{ ref('stages_groups_yaml_historical') }}
  {{ dbt_utils.group_by(n=3) }}

),

gitlab_dotcom_users AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_users') }}

),

directory_mapping AS (

  SELECT
    a.date_actual,
    b.gitlab_username,
    d.user_id,
    b.employee_id,
    CONCAT(COALESCE(job_specialty_multi, ''), '; ', COALESCE(job_specialty_single, ''))                                                                                 AS job_specialty,
    --sometimes specialty is only specified in either single/multi, this way we can extract the group name properly

    b.division,
    b.department,
    b.position,
    b.management_level,
    CASE
      WHEN LOWER(b.position) LIKE '%backend%'
        THEN 'backend'
      WHEN LOWER(b.position) LIKE '%fullstack%'
        THEN 'fullstack'
      WHEN LOWER(b.position) LIKE '%frontend%'
        THEN 'frontend'
    END                                                                                                                                                                 AS technology_group,
    MAX(IFF(CONTAINS(LOWER(job_specialty), c.group_name), c.group_name, NULL))                                                                                          AS user_group,
    MAX(IFF(CONTAINS(LOWER(job_specialty), c.group_name), c.stage_display_name, IFF(CONTAINS(LOWER(job_specialty), c.stage_display_name), c.stage_display_name, NULL))) AS user_stage,
    MAX(IFF(CONTAINS(LOWER(job_specialty), c.group_name), c.stage_section, IFF(CONTAINS(LOWER(job_specialty), c.stage_display_name), c.stage_section, NULL)))           AS user_section
  FROM dates AS a
  INNER JOIN {{ ref('mart_team_member_directory') }} AS b
    ON b.is_current_team_member
      AND a.date_actual >= b.valid_from
      AND a.date_actual < b.valid_to
  INNER JOIN category AS c ON a.date_actual BETWEEN c.valid_from AND c.valid_to
  LEFT JOIN gitlab_dotcom_users AS d ON b.gitlab_username = d.user_name
  {{ dbt_utils.group_by(n=10) }}

)

SELECT *
FROM directory_mapping
