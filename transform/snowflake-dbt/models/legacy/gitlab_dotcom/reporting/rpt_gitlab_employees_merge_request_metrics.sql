WITH merge_requests AS (
    
    SELECT *
    FROM {{ ref('gitlab_employees_merge_requests_xf') }}
    WHERE merged_at IS NOT NULL
      {# AND is_part_of_product = TRUE #}
  
), employees AS (
  
    SELECT *
    FROM {{ref('gitlab_bamboohr_employee_base')}}

), intermediate AS (  

    SELECT
      employees.*,
      merge_requests.merge_request_id, 
      merge_requests.merge_request_data_source,
      merge_requests.merged_at,
      merge_requests.is_part_of_product,
      people_engineering_project
    FROM employees
    LEFT JOIN merge_requests
      ON merge_requests.bamboohr_employee_id = employees.employee_id
      AND DATE_TRUNC(day, merge_requests.merged_at) BETWEEN employees.valid_from AND COALESCE(employees.valid_to, '2020-02-28')

)

    SELECT 
      month_date,
      employee_id,
      gitlab_dotcom_user_id,
      division,
      department,
      job_role,
      jobtitle_speciality,
      reports_to,
      total_days,
      COUNT(IFF(is_part_of_product = TRUE, merge_request_id, NULL))                        AS total_merged_part_of_product,
      COUNT(IFF(is_part_of_product = TRUE AND 
                merge_request_data_source = 'gitlab_dotcom',merge_request_id,NULL))     AS total_gitlab_dotcom_product_merge_requests,
      COUNT(IFF(is_part_of_product = TRUE 
                AND merge_request_data_source = 'gitlab_ops',merge_request_id,NULL))    AS total_gitlab_ops_product_merge_requests,
      SUM(people_engineering_project)                                                   AS total_people_engineering_merge_requests
    FROM intermediate
    {{ dbt_utils.group_by(n=9) }}  

    --aggregate to division/department, and add in % of tm with mr



