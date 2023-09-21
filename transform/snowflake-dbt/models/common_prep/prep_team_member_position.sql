
{{ simple_cte([
    ('job_info_source','blended_job_info_source'),
    ('team_member','dim_team_member'),
    ('employee_mapping','blended_employee_mapping_source')
]) }},


 job_profiles AS (

  SELECT 
    job_code,
    job_profile                                                                                                                AS position,
    job_family                                                                                                                 AS job_family,
    management_level                                                                                                           AS management_level, 
    job_level                                                                                                                  AS job_grade,
    is_job_profile_active                                                                                                      AS is_position_active
  FROM {{ref('job_profiles_snapshots_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY job_code ORDER BY valid_from DESC) = 1 

),

team_info AS (

  -- Get team, department, division info from the BambooHR data
  -- Solve for gaps and islands problem in data

  SELECT 
    {{ dbt_utils.surrogate_key(['employee_id', 'job_title', 'department', 'division', 'reports_to','entity']) }}               AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    job_title                                                                                                                  AS position,
    reports_to                                                                                                                 AS manager,
    entity                                                                                                                     AS entity, 
    department                                                                                                                 AS department,
    division                                                                                                                   AS division,
    effective_date                                                                                                             AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from)                               AS valid_to,
    LAG(unique_key, 1, NULL) OVER (PARTITION BY employee_id ORDER BY effective_date)                                           AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key) OVER ( PARTITION BY employee_id ORDER BY effective_date)              AS unique_key_group 
  FROM job_info_source

),

team_info_group AS (

  -- Combine the team_info and job_profiles data to get some data that didn't exist in BHR 
  -- Group by unique_key_group to clearly identify the different team change events

  SELECT 
    team_info.employee_id, 
    job_profiles.job_code,
    team_info.position,
    team_info.manager,
    team_info.entity, 
    team_info.department,
    team_info.division,
    job_profiles.is_position_active,
    job_profiles.job_family,
    team_info.unique_key_group,
    MIN(team_info.valid_from) AS valid_from,
    MAX(team_info.valid_to) AS valid_to
  FROM team_info
  LEFT JOIN job_profiles
    ON job_profiles.position = team_info.position
  {{ dbt_utils.group_by(n=10)}}

),

job_info_clean AS (

  -- There are a lot of data quality issues in the mapping data, this CTE cleans it up ahead of solving for gaps and islands
  -- Otherwise we end up two islands for the same time period since there are so many NULLs in the data

  SELECT
    employee_id                                                                                                                AS employee_id, 
    LAST_VALUE(job_role IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY uploaded_at DESC ROWS UNBOUNDED PRECEDING)       AS management_level,
    LAST_VALUE(job_grade IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY uploaded_at DESC ROWS UNBOUNDED PRECEDING)      AS job_grade,
    LAST_VALUE(jobtitle_speciality_single_select IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY uploaded_at DESC ROWS UNBOUNDED PRECEDING)           
                                                                                                                               AS job_specialty_single,
              
    LAST_VALUE(jobtitle_speciality_multi_select IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY uploaded_at ROWS UNBOUNDED PRECEDING) 
                                                                                                                               AS job_specialty_multi,
    uploaded_at                                                                                                                AS uploaded_at
  FROM employee_mapping

),

job_info AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['employee_id', 'management_level', 'job_grade', 'job_specialty_single', 'job_specialty_multi']) }}   
                                                                                                                               AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    management_level                                                                                                           AS management_level,
    job_grade                                                                                                                  AS job_grade, 
    job_specialty_single                                                                                                       AS job_specialty_single,
    job_specialty_multi                                                                                                        AS job_specialty_multi,
    uploaded_at                                                                                                                AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from)                               AS valid_to,
    LAG(unique_key, 1, NULL) OVER (PARTITION BY employee_id ORDER BY uploaded_at)                                              AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key) OVER ( PARTITION BY employee_id ORDER BY uploaded_at)                 AS unique_key_group 
  FROM job_info_clean

),

job_info_group AS (

  -- Group by unique_key_group to clearly identify the different job change events

  SELECT 
    employee_id, 
    management_level,
    job_grade,
    job_specialty_single, 
    job_specialty_multi,
    unique_key_group,
    MIN(valid_from) AS valid_from,
    MAX(valid_to) AS valid_to
  FROM job_info
  {{ dbt_utils.group_by(n=6)}}

),

legacy_history AS (

  -- Combine BHR tables to get the history of the team members before workday

  SELECT 
    job_info_group.employee_id                                                                                                 AS employee_id,
    team_info_group.manager                                                                                                    AS manager,
    team_info_group.job_code                                                                                                   AS job_code,
    team_info_group.position                                                                                                   AS position,
    team_info_group.job_family                                                                                                 AS job_family,
    job_info_group.job_specialty_single                                                                                        AS job_specialty_single,
    job_info_group.job_specialty_multi                                                                                         AS job_specialty_multi,
    job_info_group.management_level                                                                                            AS management_level,
    job_info_group.job_grade                                                                                                   AS job_grade,
    team_info_group.department                                                                                                 AS department,
    team_info_group.division                                                                                                   AS division,
    team_info_group.entity                                                                                                     AS entity,
    team_info_group.is_position_active                                                                                         AS is_position_active,
    team_info_group.valid_from                                                                                                 AS valid_from,
    team_info_group.valid_to                                                                                                   AS valid_to
  FROM job_info_group
  LEFT JOIN team_info_group
    ON job_info_group.employee_id = team_info_group.employee_id
      AND NOT (job_info_group.valid_to <= team_info_group.valid_from
          OR job_info_group.valid_from >= team_info_group.valid_to)

),

staffing_history AS (

  -- Combine workday tables to get an accurate picture of the team members info

  SELECT
    {{ dbt_utils.surrogate_key(['employee_id', 'team_id_current', 'manager_current','department_current','suporg_current','job_code_current', 'job_specialty_single_current', 'job_specialty_multi_current', 'entity_current']) }}
                                                                                                                               AS unique_key,
    employee_id                                                                                                                AS employee_id,
    team_id_current                                                                                                            AS team_id,
    manager_current                                                                                                            AS manager,
    department_current                                                                                                         AS department,
    suporg_current                                                                                                             AS suporg,

    /*
      We weren't capturing history of job codes and when they changed, we didn't capture it anywhere
      The following job codes from staffing_history don't exist in job_profiles so 
      we are capturing them through this case statement
    */

    CASE 
      WHEN job_code_current = 'SA.FSDN.P5' 
        THEN 'SA.FSDN.P5-SAE'                                                        
      WHEN job_code_current = 'SA.FSDN.P4' 
        THEN 'SA.FSDN.P4-SAE'
      WHEN job_code_current = 'MK.PMMF.M3-PM'
        THEN 'MK.PMMF.M4-PM'
      ELSE job_code_current
    END                                                                                                                        AS job_code,
    job_specialty_single_current                                                                                               AS job_specialty_single,
    job_specialty_multi_current                                                                                                AS job_specialty_multi,
    entity_current                                                                                                             AS entity,
    effective_date                                                                                                             AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from)                               AS valid_to
  FROM {{ref('staffing_history_approved_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY date_time_initiated DESC)  = 1 

),

staffing_history_combined AS (

  SELECT 
    staffing_history.employee_id                                                                                             AS employee_id,
    staffing_history.team_id                                                                                                 AS team_id,
    staffing_history.manager                                                                                                 AS manager,
    staffing_history.department                                                                                              AS department,
    staffing_history.suporg                                                                                                  AS suporg,
    staffing_history.job_code                                                                                                AS job_code,
    staffing_history.job_specialty_single                                                                                    AS job_specialty_single,
    staffing_history.job_specialty_multi                                                                                     AS job_specialty_multi,
    staffing_history.entity                                                                                                  AS entity,   
    team_info_group.division                                                                                                   AS division,
    job_profiles.position                                                                                                      AS position,
    job_profiles.job_family                                                                                                    AS job_family,
    job_profiles.management_level                                                                                              AS management_level,
    job_profiles.job_grade                                                                                                     AS job_grade,
    job_profiles.is_position_active                                                                                            AS is_position_active,
    staffing_history.valid_from                                                                                              AS valid_from,
    staffing_history.valid_to                                                                                                AS valid_to
  FROM staffing_history
  --Temporary join while we add a key to join cost_centers to staffing_history
  LEFT JOIN job_profiles
    ON job_profiles.job_code = staffing_history.job_code
  LEFT JOIN team_info_group
    ON staffing_history.employee_id = team_info_group.employee_id 
      AND NOT (staffing_history.valid_to <= team_info_group.valid_from
          OR staffing_history.valid_from >= team_info_group.valid_to)
  
),

final AS (

  SELECT 

    -- Surrogate keys
    {{ dbt_utils.surrogate_key(['staffing_history_combined.employee_id'])}}                                                             AS dim_team_member_sk,
    {{ dbt_utils.surrogate_key(['staffing_history_combined.team_id'])}}                                                                 AS dim_team_sk,

    -- Team member position attributes
    staffing_history_combined.employee_id                                                                                               AS employee_id,
    staffing_history_combined.team_id                                                                                                   AS team_id,
    staffing_history_combined.manager                                                                                                   AS manager,
    staffing_history_combined.suporg                                                                                                    AS suporg,
    staffing_history_combined.job_code                                                                                                  AS job_code,
    staffing_history_combined.position                                                                                                  AS position,
    staffing_history_combined.job_family                                                                                                AS job_family,
    staffing_history_combined.job_specialty_single                                                                                      AS job_specialty_single,
    staffing_history_combined.job_specialty_multi                                                                                       AS job_specialty_multi,
    staffing_history_combined.management_level                                                                                          AS management_level,
    staffing_history_combined.job_grade                                                                                                 AS job_grade,
    staffing_history_combined.department                                                                                                AS department,
    staffing_history_combined.division                                                                                                  AS division,
    staffing_history_combined.entity                                                                                                    AS entity,
    staffing_history_combined.is_position_active                                                                                        AS is_position_active,
    staffing_history_combined.valid_from                                                                                                AS valid_from,
    staffing_history_combined.valid_to                                                                                                  AS valid_to
  FROM staffing_history_combined
  WHERE staffing_history_combined.valid_from >= '2022-06-16'

  UNION

  SELECT 
    -- Surrogate keys
    {{ dbt_utils.surrogate_key(['legacy_history.employee_id'])}}                                                               AS dim_team_member_sk,                                                                                                              
    NULL                                                                                                                       AS dim_team_sk,

    -- Team member position attributes
    legacy_history.employee_id                                                                                                 AS employee_id,
    NULL                                                                                                                       AS team_id,
    legacy_history.manager                                                                                                    AS manager,
    NULL                                                                                                                       AS suporg,
    legacy_history.job_code                                                                                                   AS job_code,
    legacy_history.position                                                                                                   AS position,
    legacy_history.job_family                                                                                                 AS job_family,
    legacy_history.job_specialty_single                                                                                        AS job_specialty_single,
    legacy_history.job_specialty_multi                                                                                         AS job_specialty_multi,
    legacy_history.management_level                                                                                            AS management_level,
    legacy_history.job_grade                                                                                                   AS job_grade,
    legacy_history.department                                                                                                 AS department,
    legacy_history.division                                                                                                   AS division,
    legacy_history.entity                                                                                                     AS entity,
    legacy_history.is_position_active                                                                                         AS is_position_active,
    legacy_history.valid_from                                                                                                 AS valid_from,
    legacy_history.valid_to                                                                                                   AS valid_to
  FROM legacy_history
  WHERE legacy_history.valid_from < '2022-06-16'
  
)

SELECT 
  *,
  CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY valid_from DESC) = 1 
      THEN TRUE
    ELSE FALSE
  END                                                                                                                          AS is_current
FROM final
