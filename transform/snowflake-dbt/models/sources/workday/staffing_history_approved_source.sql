WITH source AS (

  SELECT *
  FROM {{ source('workday','staffing_history_approved') }}

), 

renamed AS (

  SELECT
    source.employee_id::NUMBER                          AS employee_id,
    d.value['WORKDAY_ID']::VARCHAR                      AS workday_id,
    d.value['BUSINESS_PROCESS_TYPE']::VARCHAR           AS business_process_type,
    d.value['HIRE_DATE']::DATE                          AS hire_date,
    d.value['TERMINATION_DATE']::DATE                   AS termination_date,
    d.value['COUNTRY_CURRENT']::VARCHAR                 AS country_past,
    d.value['COUNTRY_PROPOSED']::VARCHAR                AS country_current,
    d.value['REGION_CURRENT']::VARCHAR                  AS region_past,
    d.value['REGION_PROPOSED']::VARCHAR                 AS region_current,
    d.value['DEPARTMENT_CURRENT']::VARCHAR              AS department_past,
    d.value['DEPARTMENT_PROPOSED']::VARCHAR             AS department_current,
    d.value['EMPL_TYPE_CURRENT']::VARCHAR               AS employee_type_past,
    d.value['EMPL_TYPE_PROPOSED']::VARCHAR              AS employee_type_current,
    d.value['ENTITY_CURRENT']::VARCHAR                  AS entity_past,
    d.value['ENTITY_PROPOSED']::VARCHAR                 AS entity_current,
    d.value['JOB_CODE_CURRENT']::VARCHAR                AS job_code_past,
    d.value['JOB_CODE_PROPOSED']::VARCHAR               AS job_code_current,
    d.value['JOB_SPECIALTY_MULTI_CURRENT']::VARCHAR     AS job_specialty_multi_past,
    d.value['JOB_SPECIALTY_MULTI_PROPOSED']::VARCHAR    AS job_specialty_multi_current,
    d.value['JOB_SPECIALTY_SINGLE_CURRENT']::VARCHAR    AS job_specialty_single_past,
    d.value['JOB_SPECIALTY_SINGLE_PROPOSED']::VARCHAR   AS job_specialty_single_current,
    d.value['LOCALITY_CURRENT']::VARCHAR                AS locality_past,
    d.value['LOCALITY_PROPOSED']::VARCHAR               AS locality_current,
    d.value['MANAGER_CURRENT']::VARCHAR                 AS manager_past,
    d.value['MANAGER_PROPOSED']::VARCHAR                AS manager_current,
    d.value['SUPORG_CURRENT']::VARCHAR                  AS suporg_past,
    d.value['SUPORG_PROPOSED']::VARCHAR                 AS suporg_current,
    d.value['TEAM_ID_CURRENT']::VARCHAR                 AS team_id_past,
    d.value['TEAM_ID_PROPOSED']::VARCHAR                AS team_id_current,
    d.value['DATE_TIME_INITIATED']::TIMESTAMP           AS date_time_initiated,
    d.value['EFFECTIVE_DATE']::DATE                     AS effective_date
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(STAFFING_HISTORY_APPROVED)) AS d

)

SELECT *
FROM renamed
WHERE effective_date <= CURRENT_DATE()
