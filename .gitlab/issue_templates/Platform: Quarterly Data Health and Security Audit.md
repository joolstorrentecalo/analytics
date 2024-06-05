# Quarterly Data Health and Security Audit

Quarterly audit is performed to validate security like right people with right access in environments (Example: Sisense, Snowflake.etc) and data feeds that are running are healthy (Example: Salesforce, GitLab.com..etc).

Please see the [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-management/#quarterly-data-health-and-security-audit) for more information. 

Below checklist of activities would be run once for quarter to validate security and system health.

## SNOWFLAKE
1. [ ] Validate terminated employees have been removed from Snowflake access.
    <details>

    Cross check between Employee Directory and Snowflake
    * [ ] If applicable, check if users set to disabled in Snowflake
    * [ ] If applicable, check if users in [roles.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/roles.yml):
        * [ ] isn't assigned to `warehouses`
        * [ ] isn't assigned to `roles`
        * [ ] can_login set to: `no`

    ```sql

      SELECT									 
        employee.employee_id,									 
        employee.first_name,									 
        employee.last_name,									 
        employee.hire_date,									 
        employee.rehire_date,									 
        snowflake.last_success_login,									 
        snowflake.created_on,									 
        employee.termination_date,									
        snowflake.is_disabled									 
      FROM prep.sensitive.employee_directory employee 									 
      INNER JOIN prod.legacy.snowflake_show_users  snowflake									 
      ON employee.first_name = snowflake.first_name									 
      AND employee.last_name = snowflake.last_name									 
      AND snowflake.is_disabled ='false'									 
      AND employee.termination_date IS NOT  NULL;									

    ```

2. [ ] De-activate any account that has not logged-in within the past 60 days from the moment of performing audit from Snowflake.
    <details>

   * [ ] Run below SQL script to perform the check.

     `NOTE: Exclude deactivating system accounts that show up in the list when below SQL script is executed.`
  

    ```sql
     SELECT	*																			
     FROM prod.legacy.snowflake_show_users 																			
     WHERE CASE WHEN last_success_login IS null THEN created_on ELSE last_success_login END <= dateadd('day', -60, CURRENT_DATE())
     AND is_disabled ='false';										
    ```


3. [ ] Validate all user accounts do not have password set.
    <details>

   * [ ] Check HAS_PASSWRD is set to ‘false’ in users table. If set to ‘false’ then there is not password set. Run below SQL script to perform the check.
   ```sql
    SELECT * 
   FROM "SNOWFLAKE"."ACCOUNT_USAGE"."USERS"
   WHERE has_password = 'true'
   AND disabled = 'false'
   AND deleted_on IS NULL
   AND name NOT IN ('PERMISSION_BOT','FIVETRAN','GITLAB_CI','AIRFLOW','STITCH','SISENSE_RESTRICTED_SAFE','PERISCOPE','MELTANO','TARGET_SNOWFLAKE','GRAFANA','SECURITYBOTSNOWFLAKEAPI', 'GAINSIGHT','MELTANO_DEV','BI_TOOL_EVAL','TABLEAU_RESTRICTED_SAFE','DATA_OBS_USER_1','TABLEAU', 'HIGHTOUCH_USER', 'DATA_SCIENCE_LOADER', 'TABLEAU_RESTRICTED_PEOPLE_DATA', 'TABLEAU_LOADER');

 
    ```

4. [ ] Drop orphaned tables.
    <details>

    * [ ] Using the current main branch of the [analytics repository](https://gitlab.com/gitlab-data/analytics/-/tree/master) run the dbt operation `orphaned_db_table_check`
    ```
    dbt run-operation orphaned_db_table_check
    ```
    * [ ] Using the list of output tables validate that the tables are no longer in use.
    * [ ] Send out Slack notification in `#data`
       * [ ] Slack notification
          <details>
          
          ```
          Hi Everyone.
          As part of our quarterly [data health and security audit](https://about.gitlab.com/handbook/business-technology/data-team/data-management/#quarterly-data-health-and-security-audit) we check for orphaned tables in our Snowflake instance (`PREP` and `PROD` database). Orphaned tables are tables with no dbt model attached to it. To keep our Data Platform clean we will drop each quarter orphaned tables in order to keep our Data Platform in a healthy shape. Please review this<link to issue> list of tables that we will drop on `xxxx-xx-xx` and let us know if there are concerns and (some) tables need to be kept in Snowflake.
          ```

          
    * [ ] Drop tables that are no longer in use.

## TRUSTED DATA
1.  [ ] Review Data Siren to confirm known existence of RED data.

    <details>
    
    * [ ] Run below SQL script to perform the check.

     ```sql

    SELECT DISTINCT 
       SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME				
    FROM "PREP"."DATASIREN"."DATASIREN_AUDIT_RESULTS"				
    UNION ALL				
    SELECT DISTINCT 
       SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME	
    FROM "PREP"."DATASIREN"."DATASIREN_CANARY_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_IP_ADDRESS_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_MAPPING_IP_ADDRESS_SENSOR"		
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_LEGACY_EMAIL_VALUE_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME		
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_LEGACY_IP_ADDRESS_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_SOURCE_DB_SOCIAL_SECURITY_NUMBER_SENSOR"		UNION ALL
    SELECT DISTINCT 
       SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME		
    FROM "PREP"."DATASIREN"."DATASIREN_TRANSFORM_DB_EMAIL_VALUE_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_TRANSFORM_DB_IP_ADDRESS_SENSOR"
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,				
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_BONEYARD_EMAIL_VALUE_SENSOR"
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_BONEYARD_IP_ADDRESS_SENSOR"
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_EMAIL_VALUE_SENSOR"
    UNION ALL
     SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_MAPPING_EMAIL_VALUE_SENSOR"
    ;					
				
     ```

## DBT Execution
1. [ ] Generate report on top 25 long running dbt models

    <details>

    * [ ] Run below SQL script (set manual the previous quarter)

     ```sql
        WITH RANGE_PREVIOUS_QUARTER AS
        (
          SELECT 
            MIN(date_day) AS first_day_fq,
            MAX(date_day) AS last_day_fq
          FROM "PROD"."COMMON"."DIM_DATE"
          --Check the year and quarter you want to audit
          WHERE fiscal_year = (CASE WHEN MONTH(CURRENT_DATE()) IN ('2','3','4') THEN year(CURRENT_DATE()) ELSE year(CURRENT_DATE())+1 END)
          AND fiscal_quarter = (CASE WHEN MONTH(CURRENT_DATE()) IN ('2','3','4') THEN '4' WHEN MONTH(CURRENT_DATE()) IN ('5','6','7') THEN '1' WHEN MONTH(CURRENT_DATE()) IN ('8','9','10') THEN '2' WHEN MONTH(CURRENT_DATE()) IN ('11','12','1') THEN '3' END)
        )

        , DISTINCT_SELECT AS
        ( 
          SELECT distinct
          model_name
        , compilation_started_at
        , model_execution_time
        FROM 
        "PROD"."WORKSPACE_DATA"."DBT_RUN_RESULTS"
        JOIN range_previous_quarter
        WHERE 1=1
        --AND model_name = 'bamboohr_budget_vs_actual'
        AND compilation_started_at >= first_day_fq
        AND compilation_started_at <= last_day_fq
        AND model_name in ( SELECT DISTINCT model_name FROM  "PROD"."WORKSPACE_DATA"."DBT_RUN_RESULTS" WHERE compilation_started_at BETWEEN dateadd('day', -7, CURRENT_DATE()) AND CURRENT_DATE AND RUN_STATUS = 'success' )
        )

        --select * from DISTINCT_SELECT

        , AVG_PER_MONTH AS
        (
          SELECT
          model_name 
        , YEAR(compilation_started_at) || LPAD(MONTH(compilation_started_at),2,0) AS compilation_started_at_month
        , AVG(model_execution_time) AS avg_execution_time
        FROM distinct_select
        GROUP BY 1,2
        )

        --select * from avg_per_month

        , MONTH_COMPARE AS
        (
          SELECT 
          model_name 
        , compilation_started_at_month 
        , LAG(avg_execution_time,2) OVER (PARTITION BY model_name ORDER BY compilation_started_at_month) AS avg_execution_time_first_month_of_quarter
        , LAG(avg_execution_time,1) OVER (PARTITION BY model_name ORDER BY compilation_started_at_month) AS avg_execution_time_month_month_of_quarter
        , avg_execution_time AS avg_execution_time_third_month_of_quarter
        FROM avg_per_month
        )

        --select * from month_compare

        SELECT 
          model_name  
        , avg_execution_time_first_month_of_quarter
        , avg_execution_time_month_month_of_quarter
        , avg_execution_time_third_month_of_quarter
        , (avg_execution_time_third_month_of_quarter / avg_execution_time_first_month_of_quarter) delta_first_last
        FROM month_compare
        --WHERE compilation_started_at_month = 202205
        QUALIFY ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY compilation_started_at_month DESC) = 1
        ORDER BY 4 desc
        LIMIT 25
    ```


## AIRFLOW
1. [ ] Validate off-boarded employees have been removed from Airflow access.
    <details>

    ```sql
      SELECT									 
        employee.employee_id,									 
        employee.first_name,									 
        employee.last_name,									 
        employee.hire_date,									 
        employee.rehire_date,									 
        employee.termination_date,	
        airflow.email,
        airflow.active									 
      FROM prep.sensitive.employee_directory employee 									 
      RIGHT OUTER JOIN raw.airflow_stitch.ab_user  airflow									 
        ON employee.last_work_email = airflow.email									   
      WHERE airflow.active ='TRUE'									 
      AND employee.termination_date IS NOT NULL
    ```
2. [ ] Clean up old log files, following [this runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/airflow_infrastructure/archival_pvc_volume/delete_pvc_volume.md). 

## Monte Carlo
1. [ ] Validate off-boarded employees have been removed from Monte Carlo access.
1. [ ] Deprovision access if an account has not logged-in within the past 90 days from the moment of performing audit.

## Tableau
1. [ ] Validate offboarded employess have been removed from Tableau Cloud
1. [ ] Deprovision access if a user has had access for >=90 days, but have not logged in during the past 90 days from the moment of performing audit. 
  - [Deprovision Check](https://10az.online.tableau.com/t/gitlab/views/UserDeprovisionCheck/UserDeprovisionCheck)

## Package version inventory

1. Python libraries inventory:
    * [ ] Go to [/package_inventory](https://gitlab.com/gitlab-data/package_inventory/-/blob/main/package_inventory/src/README.md) repo, run application and generate reports using guidelines from the `README.md` file  - this code is applicable for the `Python` libraries
    * [ ] Once you generate the reports, save them in the tabular format _(for better readability)_ in the comment of this issue 
1. Other tools/libraries inventory
    * [ ] Manually check the all tools we are using, except Python libraries, as they are already analyzed in the previous point. For instance, check the `Python` version itself `Airflow`, `permifrost`, `meltano`, `dbt` and `dbt_packages` versions. Save your findings in the comment, and use this template for the tabular format
        <details><summary>Template</summary>
    
        | Tool/Library                       | Current version | Current version release date | Latest version | Latest version release date | Note | Candidate for the upgrade (Yes/No) | 
        |--------------------------|-----------------|------------------------------|----------------|-----------------------------|--------|--------|
        | [airflow](https://about.gitlab.com/handbook/business-technology/data-team/platform/infrastructure/#airflow) | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | [permifrost](https://about.gitlab.com/handbook/business-technology/data-team/platform/permifrost/)          | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | [meltano](https://about.gitlab.com/handbook/business-technology/data-team/platform/Meltano-Gitlab/)         | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | [dbt](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/)                  | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | dbt package: [snowflake_spend](https://gitlab.com/gitlab-data/snowflake_spend)                              | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | dbt package: [data-tests](https://gitlab.com/gitlab-data/data-tests)                                        | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | dbt package: [dbt-labs/audit_helper](https://github.com/dbt-labs/dbt-audit-helper)                          | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | dbt package: [dbt-labs/dbt_utils](https://github.com/dbt-labs/dbt-utils)                                    | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | dbt package: [dbt-labs/snowplow](https://github.com/dbt-labs/snowplow/tree/0.15.1/)                         | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | dbt package: [dbt-labs/dbt_external_tables](https://hub.getdbt.com/dbt-labs/dbt_external_tables/latest/)    | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        | dbt package: [brooklyn-data/dbt_artifacts](https://github.com/brooklyn-data/dbt_artifacts)                  | ``      | `YYYY-MM-DD` | ``      | `YYYY-MM-DD` | | |
        
        </details>
1. [ ] Share your finding with the team `@gitlab-data/engineers` (tag them in the issue) and pick the good candidates for upgrading _(including both Python and other tools and libraries)_
1. [ ] If we agree on candidates for upgrading, create [**a new issue**](https://gitlab.com/gitlab-data/analytics/-/issues/new?issue%5Bassignee_id%5D=&issue%5Bmilestone_id%5D=) in the `/analytics` repo with the list of libraries we agreed that should upgrade and proceed further
1. [ ] Once when the packages are upgraded and the issue mentioned above is closed, put all updates in the handbook page [Python/Tools package management and inventory](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/python-tool-package-management/) to stay in sync with the current version we are using. 

<!-- DO NOT EDIT BELOW THIS LINE -->
/label ~"Team::Data Platform" ~Snowflake ~TDF ~"Data Team" ~"Priority::1-Ops" ~"workflow::4 - scheduled" ~"Quarterly Data Health and Security Audit" ~"Periscope / Sisense"
/confidential 
