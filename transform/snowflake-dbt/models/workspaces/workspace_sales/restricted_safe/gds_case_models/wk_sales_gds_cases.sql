/*
We pull all the Pooled cases, using the record type ID.

We have to manually parse the Subject field to get the Trigger Type, hopefully this will go away in future iterations.

-No spam filter
-Trigger Type logic only valid for FY25 onwards
*/

{{ simple_cte([
    ('prep_crm_case', 'prep_crm_case'),
    ('dim_crm_user', 'dim_crm_user'),
    ('dim_date', 'dim_date'),
    ('mart_crm_task', 'mart_crm_task')
    ])

}},

case_data AS (
  SELECT

    CASE WHEN prep_crm_case.subject LIKE 'Multiyear Renewal%' THEN 'Multiyear Renewal'
      WHEN prep_crm_case.subject LIKE 'EOA Renewal%' THEN 'EOA Renewal'
      WHEN prep_crm_case.subject LIKE 'PO Required%' THEN 'PO Required'
      WHEN prep_crm_case.subject LIKE 'Auto-Renewal Will Fail%' THEN 'Auto-Renewal Will Fail'
      WHEN prep_crm_case.subject LIKE 'Overdue Renewal%' THEN 'Overdue Renewal'
      WHEN prep_crm_case.subject LIKE 'Auto-Renew Recently Turned Off%' THEN 'Auto-Renew Recently Turned Off'
      WHEN prep_crm_case.subject LIKE 'Failed QSR%' THEN 'Failed QSR' ELSE prep_crm_case.subject
    END                         AS case_trigger,
    prep_crm_case.* EXCLUDE (created_by, updated_by, model_created_date, model_updated_date, dbt_created_at, dbt_updated_at)
    ,
    dim_crm_user.user_name      AS case_owner_name,
    dim_crm_user.department     AS case_department,
    dim_crm_user.team,
    dim_crm_user.manager_name,
    dim_crm_user.user_role_name AS case_user_role_name,
    dim_crm_user.user_role_type
  FROM prep_crm_case
  LEFT JOIN dim_crm_user ON prep_crm_case.dim_crm_user_id = dim_crm_user.dim_crm_user_id

  WHERE
    prep_crm_case.record_type_id = '0128X000001pPRkQAM'
    AND prep_crm_case.created_date >= '2024-02-01'
--and sfdc_case.is_closed
-- and (sfdc_case.reason not like '%Spam%' or reason is null)
),

task_data_prep AS (
  SELECT
    task.task_id,
    task.task_status,
    IFF(task.dim_crm_account_id LIKE '6bb%', NULL, task.dim_crm_account_id)              AS dim_crm_account_id,
    task.dim_crm_user_id,
    task.dim_crm_person_id,
    task.task_subject,
    CASE WHEN LOWER(task.task_subject) LIKE '%email%' THEN 'Email'
      WHEN LOWER(task.task_subject) LIKE '%call%' THEN 'Call'
      WHEN LOWER(task.task_subject) LIKE '%linkedin%' THEN 'LinkedIn'
      WHEN LOWER(task.task_subject) LIKE '%inmail%' THEN 'LinkedIn'
      WHEN LOWER(task.task_subject) LIKE '%sales navigator%' THEN 'LinkedIn'
      WHEN LOWER(task.task_subject) LIKE '%drift%' THEN 'Chat'
      WHEN LOWER(task.task_subject) LIKE '%chat%' THEN 'Chat'
      ELSE
        task.task_type
    END                                                                                  AS type,
    CASE WHEN task.task_subject LIKE '%Outreach%' AND task.task_subject NOT LIKE '%Advanced Outreach%' THEN 'Outreach'
      WHEN task.task_subject LIKE '%Clari%' THEN 'Clari'
      WHEN task.task_subject LIKE '%Conversica%' THEN 'Conversica'
      ELSE 'Other'
    END                                                                                  AS outreach_clari_flag,
    task.task_created_date,
    task.task_created_by_id,

    --This is looking for either inbound emails (indicating they are from a customer) or completed phone calls

    CASE WHEN outreach_clari_flag = 'Outreach' AND (task.task_subject LIKE '%[Out]%' OR task.task_subject LIKE '%utbound%') THEN 'Outbound'
      WHEN outreach_clari_flag = 'Outreach' AND (task.task_subject LIKE '%[In]%' OR task.task_subject LIKE '%nbound%') THEN 'Inbound'
      ELSE 'Other'
    END                                                                                  AS inbound_outbound_flag,
    COALESCE ((
      inbound_outbound_flag = 'Outbound' AND task.task_subject LIKE '%Answered%' AND task.task_subject NOT LIKE '%Not Answer%'
      AND task.task_subject NOT LIKE '%No Answer%'
    ) OR (LOWER(task.task_subject) LIKE '%call%' AND task.task_subject NOT LIKE '%Outreach%' AND task.task_status = 'Completed'
    ), FALSE)                                                                            AS outbound_answered_flag,
    task.task_date,
    CASE WHEN task.task_created_by_id LIKE '0054M000003Tqub%' THEN 'Outreach'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%GitLab Transactions%' THEN 'Post-Purchase'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Was Sent Email%' THEN 'SFDC Marketing Email Send'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Your GitLab License%' THEN 'Post-Purchase'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Advanced Outreach%' THEN 'Gainsight Marketing Email Send'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Filled Out Form%' THEN 'Marketo Form Fill'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Conversation in Drift%' THEN 'Drift'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Opened Email%' THEN 'Marketing Email Opened'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Sales Navigator%' THEN 'Sales Navigator'
      WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task.task_subject LIKE '%Clari - Email%' THEN 'Clari Email'
      ELSE
        'Other'
    END                                                                                  AS task_type,

    user.user_name                                                                       AS task_user_name,
    CASE WHEN user.department LIKE '%arketin%' THEN 'Marketing' ELSE user.department END AS department,
    user.is_active,
    user.crm_user_sales_segment,
    user.crm_user_geo,
    user.crm_user_region,
    user.crm_user_area,
    user.crm_user_business_unit,
    user.user_role_name
  FROM
    mart_crm_task AS task
  INNER JOIN dim_crm_user AS user ON task.dim_crm_user_id = user.dim_crm_user_id
  WHERE
    task.dim_crm_user_id IS NOT NULL
    AND
    task.is_deleted = FALSE
    AND task.task_date >= '2024-02-01'
    AND task.task_status = 'Completed'
),

task_data AS (
  SELECT * FROM
    task_data_prep


  WHERE --(outreach_clari_flag = 'Outreach' or task_created_by_id = dim_crm_user_id)
    --and outreach_clari_flag <> 'Other'
    --and 
    (
      user_role_name LIKE ANY ('%AE%', '%SDR%', '%BDR%', '%Advocate%')
      OR crm_user_sales_segment = 'SMB'
    )
),

-- lead_account_ids as (
-- select dim_crm_account_id, sfdc_record_id, dim_crm_person_id
-- from PROD.COMMON_MART_MARKETING.MART_CRM_PERSON
-- where sfdc_record_type = 'lead'
-- and dim_crm_account_id is not null
-- )

-- select
-- lead_account_ids.dim_crm_account_id as lead_account_id,
-- task_data.* exclude dim_crm_account_id,
-- coalesce(task_data.dim_crm_account_id,lead_account_id) as dim_crm_account_id_prep,
-- dim_crm_account_id_prep as dim_crm_account_id

-- from task_data
-- left join lead_account_ids on task_data.dim_crm_person_id = lead_account_ids.dim_crm_person_id
-- where dim_crm_account_id_prep is not null
-- )
-- ,
case_task_summary_data AS (
  SELECT
    case_data.case_id                                                                                 AS case_task_summary_id,

    LISTAGG(DISTINCT task_data.task_id, ', ')
      AS case_task_id_list,

    COUNT(DISTINCT CASE WHEN task_data.inbound_outbound_flag = 'Inbound' THEN task_data.task_id END)  AS inbound_email_count,
    COUNT(DISTINCT CASE WHEN task_data.outbound_answered_flag THEN task_data.task_id END)             AS completed_call_count,
    COUNT(DISTINCT CASE WHEN task_data.inbound_outbound_flag = 'Outbound' THEN task_data.task_id END) AS outbound_email_count,
    COALESCE (inbound_email_count > 0, FALSE)                                                         AS task_inbound_flag,
    COALESCE (completed_call_count > 0, FALSE)                                                        AS task_completed_call_flag,
    COALESCE (outbound_email_count > 0, FALSE)                                                        AS task_outbound_flag,
    COUNT(DISTINCT task_data.task_id)                                                                 AS completed_task_count,
    MIN(DATEDIFF('day', case_data.created_date, task_data.task_date))                                 AS days_to_first_task,
    MIN(DATEDIFF('day', task_data.task_date, CURRENT_DATE))                                           AS days_since_last_task

  FROM case_data
  LEFT JOIN task_data
    ON
      case_data.dim_crm_account_id = task_data.dim_crm_account_id
      AND
      task_data.task_date::DATE >= case_data.created_date::DATE
      AND (task_data.task_date::DATE <= case_data.closed_date::DATE OR case_data.closed_date IS NULL)
  --where case_data.account_id is not null
  GROUP BY 1
),

output AS (
  SELECT
    case_data.*,
    case_task_summary_data.* EXCLUDE (case_task_summary_id)

  FROM
    case_data
  LEFT JOIN
    case_task_summary_data ON case_data.case_id = case_task_summary_data.case_task_summary_id
)

{{ dbt_audit(
    cte_ref="output",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-07-15",
    updated_date="2024-07-15"
) }}
