
{{ simple_cte([
    ('job_info_source','blended_job_info_source'),
    ('team_member','dim_team_member'),
    ('employee_mapping','blended_employee_mapping_source'),
    ('staffing_history','staffing_history_approved_source')
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
    DATE(effective_date)                                                                                                       AS effective_date,
    LAG(unique_key, 1, NULL) OVER (PARTITION BY employee_id ORDER BY effective_date)                                           AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key) OVER ( PARTITION BY employee_id ORDER BY effective_date)              AS unique_key_group 
  FROM job_info_source
  WHERE source_system = 'bamboohr'
    AND DATE(effective_date) < '2022-06-16'

),

team_info_group AS (

  -- Combine the team_info and job_profiles data to get some data that didn't exist in BHR 
  -- Group by unique_key_group to clearly identify the different team change events

  SELECT 
    team_info.employee_id, 
    team_info.position,
    team_info.manager,
    team_info.entity, 
    team_info.department,
    team_info.division,
    team_info.unique_key_group,
    MIN(team_info.effective_date) AS effective_date
  FROM team_info
  {{ dbt_utils.group_by(n=7)}}

),

job_info AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['employee_id', 'job_role', 'job_grade', 'jobtitle_speciality_single_select', 'jobtitle_speciality_multi_select']) }}   
                                                                                                                               AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    job_role                                                                                                                   AS management_level,
    job_grade                                                                                                                  AS job_grade, 
    jobtitle_speciality_single_select                                                                                          AS job_specialty_single,
    jobtitle_speciality_multi_select                                                                                           AS job_specialty_multi,
    DATE(uploaded_at)                                                                                                          AS effective_date,
    LAG(unique_key, 1, NULL) OVER (PARTITION BY employee_id ORDER BY uploaded_at)                                              AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key) OVER (PARTITION BY employee_id ORDER BY uploaded_at)                  AS unique_key_group 
  FROM employee_mapping
  WHERE source_system = 'bamboohr'
    AND DATE(uploaded_at) < '2022-06-16'

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
    MIN(effective_date) AS effective_date
  FROM job_info
  {{ dbt_utils.group_by(n=6)}}
  
),

legacy_data AS (

SELECT
  
    team_info_group.employee_id                                                                                                AS employee_id,
    team_info_group.manager                                                                                                    AS manager,
    team_info_group.department                                                                                                 AS department,
    team_info_group.division                                                                                                   AS division,
    NULL                                                                                                                       AS job_specialty_single,
    NULL                                                                                                                       AS job_specialty_multi,
    team_info_group.entity                                                                                                     AS entity,
    team_info_group.position                                                                                                   AS position,
    NULL                                                                                                                       AS management_level,
    NULL                                                                                                                       AS job_grade,
    team_info_group.effective_date                                                                                             AS effective_date
  FROM team_info_group

UNION

  SELECT
    job_info_group.employee_id                                                                                                 AS employee_id,
    NULL                                                                                                                       AS manager,
    NULL                                                                                                                       AS department,
    NULL                                                                                                                       AS division,
    job_info_group.job_specialty_single                                                                                        AS job_specialty_single,
    job_info_group.job_specialty_multi                                                                                         AS job_specialty_multi,
    NULL                                                                                                                       AS entity,
    NULL                                                                                                                       AS position,
    job_info_group.management_level                                                                                            AS management_level,
    job_info_group.job_grade                                                                                                   AS job_grade,
    job_info_group.effective_date                                                                                              AS effective_date
  FROM job_info_group

),

legacy_clean AS (

  SELECT 
    legacy_data.employee_id                                                                                                   AS employee_id,
    LAST_VALUE(legacy_data.manager IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)              
                                                                                                                               AS manager,
    LAST_VALUE(legacy_data.position IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS position,
    LAST_VALUE(legacy_data.job_specialty_single IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS job_specialty_single,
    LAST_VALUE(legacy_data.job_specialty_multi IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS job_specialty_multi,
    LAST_VALUE(legacy_data.management_level IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS management_level,
    LAST_VALUE(legacy_data.job_grade IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS job_grade,
    LAST_VALUE(legacy_data.department IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS department,
    LAST_VALUE(legacy_data.division IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS division,
    LAST_VALUE(legacy_data.entity IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS entity,
    legacy_data.effective_date
  FROM legacy_data

),

position_history AS (

  -- Combine workday tables to get an accurate picture of the team members info

  SELECT
    staffing_history.employee_id                                                                                               AS employee_id,
    staffing_history.team_id_current                                                                                           AS team_id,
    staffing_history.manager_current                                                                                           AS manager,
    staffing_history.department_current                                                                                        AS department,
    NULL                                                                                                                       AS division,
    staffing_history.suporg_current                                                                                            AS suporg,

    /*
      We weren't capturing history of job codes and when they changed, we didn't capture it anywhere
      The following job codes from staffing_history don't exist in job_profiles so 
      we are capturing them through this case statement
    */

    CASE 
      WHEN staffing_history.job_code_current = 'SA.FSDN.P5' 
        THEN 'SA.FSDN.P5-SAE'                                                        
      WHEN staffing_history.job_code_current = 'SA.FSDN.P4' 
        THEN 'SA.FSDN.P4-SAE'
      WHEN staffing_history.job_code_current = 'MK.PMMF.M3-PM'
        THEN 'MK.PMMF.M4-PM'
      ELSE staffing_history.job_code_current
    END                                                                                                                        AS job_code,
    staffing_history.job_specialty_single_current                                                                              AS job_specialty_single,
    staffing_history.job_specialty_multi_current                                                                               AS job_specialty_multi,
    staffing_history.entity_current                                                                                            AS entity,
    job_profiles.position                                                                                                      AS position,
    job_profiles.job_family                                                                                                    AS job_family,
    job_profiles.management_level                                                                                              AS management_level,
    job_profiles.job_grade                                                                                                     AS job_grade,
    job_profiles.is_position_active                                                                                            AS is_position_active,
    staffing_history.effective_date                                                                                            AS effective_date
  FROM staffing_history
  LEFT JOIN job_profiles
    ON job_profiles.job_code = staffing_history.job_code_current
  WHERE effective_date >= '2022-06-16'

  UNION

  SELECT
    legacy_clean.employee_id                                                                                                AS employee_id,
    NULL                                                                                                                       AS team_id,
    legacy_clean.manager                                                                                                    AS manager,
    legacy_clean.department                                                                                                 AS department,
    legacy_clean.division                                                                                                   AS division,
    NULL                                                                                                                       AS suporg,
    NULL                                                                                                                       AS job_code,
    job_specialty_single                                                                                                       AS job_specialty_single,
    job_specialty_multi                                                                                                        AS job_specialty_multi,
    legacy_clean.entity                                                                                                     AS entity,
    legacy_clean.position                                                                                                   AS position,
    NULL                                                                                                                       AS job_family,
    management_level                                                                                                           AS management_level,
    job_grade                                                                                                                  AS job_grade,
    NULL                                                                                                                       AS is_position_active,
    legacy_clean.effective_date                                                                                             AS effective_date
  FROM legacy_clean
  
),

union_clean AS (

  SELECT 
    position_history.employee_id                                                                                               AS employee_id,
    position_history.team_id                                                                                                   AS team_id,
    manager,
    position_history.suporg                                                                                                    AS suporg,
    position_history.job_code                                                                                                  AS job_code,
    position,
    position_history.job_family                                                                                                AS job_family,
    job_specialty_single,
    job_specialty_multi,
    management_level,
    job_grade,
    department,
    division,
    entity,
    position_history.is_position_active                                                                                        AS is_position_active,
    MIN(position_history.effective_date)                                                                                       AS effective_date
  FROM position_history
  {{ dbt_utils.group_by(n=15)}}
  
)

SELECT *
FROM union_clean