WITH team_status_dup AS (
  SELECT 
    *
  FROM {{ref('fct_team_status')}}
),
pto_source AS (
  SELECT 
    *
  FROM {{ref('gitlab_pto')}}
),
pto AS (
  SELECT
    *,
    DAYOFWEEK(pto_date)                                    AS pto_day_of_week,
    ROW_NUMBER() OVER (
      PARTITION BY
        hr_employee_id,
        pto_date
      ORDER BY
        end_date DESC, --- In order to accurately populate the most recently submitted absence, would like to utlise the 'created_at' field.
        pto_uuid DESC -- Rakhi: Why did we order by pto_uuid, that are alphanumeric values? Shane: In order to remove duplication, we randomly choose one whichever comes first
    )                                                      AS pto_rank,
    'Y'                                                    AS is_pto_date
  FROM pto_source
  WHERE pto_status = 'AP' -- We would like to consider only approved absences. There infact is an absence approval process at Gitlab, although it's mandatory to approve pto requests by managers
--    AND pto_date <= CURRENT_DATE -- As it's a SSOT Absence data, we will include future absence data
    AND pto_day_of_week BETWEEN 1 AND 5 -- to include only absences occuring on working days of a week
  QUALIFY pto_rank = 1 -- to consider only unique rows of the data
),
team_status AS (
  SELECT 
    dim_team_member_sk,
    dim_team_sk,
    employee_id,
    team_id,
    job_code,
    position,
    job_family,
    job_specialty_single,
    job_specialty_multi,
    management_level,
    job_grade,
    entity,
    is_position_active,
    is_current
    FROM team_status_dup
    WHERE is_current=TRUE
),
combined_sources AS (
  SELECT 
    team_status.dim_team_member_sk,
    team_status.dim_team_sk,
    team_status.employee_id,
    team_status.team_id,
    team_status.job_code,
    team_status.position,
    team_status.job_family,
    team_status.job_specialty_single,
    team_status.job_specialty_multi,
    team_status.management_level,
    team_status.job_grade,
    team_status.entity,
    team_status.is_position_active,  
    team_status.is_current AS is_current_team_member_position,
    pto.start_date AS absence_start,
    pto.end_date AS absence_end,
    pto.pto_date AS absence_date,
    pto.is_pto AS is_pto,
    pto.is_holiday AS is_holiday,
    pto.pto_uuid AS pto_uuid,
    pto.pto_type_uuid AS pto_type_uuid,
    pto.pto_status AS pto_status,
    pto.pto_status_name AS pto_status_name,
    pto.total_hours AS total_hours,
    pto.recorded_hours AS recorded_hours,
     IFF(
      pto.pto_type_name = 'Out Sick'
      AND DATEDIFF('day', pto.start_date, pto.end_date) > 4, 'Out Sick-Extended', pto.pto_type_name
    ) AS absence_status,
    pto.employee_day_length

  FROM team_status
  INNER JOIN pto ON pto.hr_employee_id=team_status.employee_id
),
/* 
Skipping hire date and term date
*/
final AS (

  SELECT *

  FROM combined_sources

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@rakhireddy',
    updated_by='@rakhireddy',
    created_date='2024-04-12',
    updated_date='2024-04-12',
) }}


