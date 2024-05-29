-- This query consolidates MODEL_Opportunities_Forecasting and MODEL_Opportunities_Forecasting_gds_only
-- The goal is to maintain non-GDS churn in the Q1 actuals while removing those opportunities from the forecast models for Q2-Q4

WITH
dim_crm_user AS (
  SELECT *
  FROM {{ ref('dim_crm_user') }} --prod.common.dim_crm_user
),

dim_date AS (
  SELECT *
  FROM {{ ref('dim_date') }} --prod.common.dim_date
),

dim_order AS (
  SELECT *
  FROM {{ ref('dim_order') }} --prod.common.dim_order
),

dim_order_action AS (
  SELECT *
  FROM {{ ref('dim_order_action') }} --prod.common.dim_order_action
),

dim_product_detail AS (
  SELECT *
  FROM {{ ref('dim_product_detail') }} --prod.common.dim_product_detail
),

dim_subscription AS (
  SELECT *
  FROM {{ ref('dim_subscription') }} --prod.common.dim_subscription
),

mart_crm_task AS (
  SELECT *
  FROM {{ ref('mart_crm_task') }} --prod.common_mart_sales.mart_crm_task
),

mart_arr AS (
  SELECT *
  FROM {{ ref('mart_arr') }} --prod.restricted_safe_common_mart_sales.mart_arr
),

mart_charge AS (
  SELECT *
  FROM {{ ref('mart_charge') }} --prod.restricted_safe_common_mart_sales.mart_charge
),

mart_crm_account AS (
  SELECT *
  FROM {{ ref('mart_crm_account') }} --prod.restricted_safe_common_mart_sales.mart_crm_account
),

mart_crm_opportunity AS (
  SELECT *
  FROM {{ ref('mart_crm_opportunity') }} --prod.restricted_safe_common_mart_sales.mart_crm_opportunity
),

dim_crm_account_daily_snapshot AS (
  SELECT *
  FROM {{ ref('dim_crm_account_daily_snapshot') }} --prod.restricted_safe_common.dim_crm_account_daily_snapshot
),

mart_delta_arr_subscription_month AS (
  SELECT *
  FROM {{ ref('mart_delta_arr_subscription_month') }} --prod.restricted_safe_legacy.mart_delta_arr_subscription_month
),

sfdc_case AS (
  SELECT *
  FROM {{ ref('wk_sales_sfdc_case') }} --prod.workspace_sales.sfdc_case
),

opportunity_data AS (
  SELECT
    mart_crm_opportunity.*,
    user.user_role_type,
    dim_date.fiscal_year                                                              AS date_range_year,
    dim_date.fiscal_quarter_name_fy                                                   AS date_range_quarter,
    DATE_TRUNC(MONTH, dim_date.date_actual)                                           AS date_range_month,
    dim_date.first_day_of_week                                                        AS date_range_week,
    dim_date.date_id                                                                  AS date_range_id,
    dim_date.fiscal_month_name_fy,
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_year,
    dim_date.first_day_of_fiscal_quarter,
    CASE
      WHEN product_category LIKE '%Self%' OR product_details LIKE '%Self%' OR product_category LIKE '%Starter%'
        OR product_details LIKE '%Starter%' THEN 'Self-Managed'
      WHEN product_category LIKE '%SaaS%' OR product_details LIKE '%SaaS%' OR product_category LIKE '%Bronze%'
        OR product_details LIKE '%Bronze%' OR product_category LIKE '%Silver%' OR product_details LIKE '%Silver%'
        OR product_category LIKE '%Gold%' OR product_details LIKE '%Gold%' THEN 'SaaS'
      WHEN product_details NOT LIKE '%SaaS%'
        AND (product_details LIKE '%Premium%' OR product_details LIKE '%Ultimate%') THEN 'Self-Managed'
      WHEN product_category LIKE '%Storage%' OR product_details LIKE '%Storage%' THEN 'Storage'
      ELSE 'Other'
    END                                                                               AS delivery,
    CASE
      WHEN order_type LIKE '3%' OR order_type LIKE '2%' THEN 'Growth'
      WHEN order_type LIKE '1%' THEN 'First Order'
      WHEN order_type LIKE '4%' OR order_type LIKE '5%' OR order_type LIKE '6%' THEN 'Churn / Contraction'
    END                                                                               AS order_type_clean,
    COALESCE (order_type LIKE '5%' AND net_arr = 0, FALSE)                            AS partial_churn_0_narr_flag,
    CASE
      WHEN product_category LIKE '%Premium%' OR product_details LIKE '%Premium%' THEN 'Premium'
      WHEN product_category LIKE '%Ultimate%' OR product_details LIKE '%Ultimate%' THEN 'Ultimate'
      WHEN product_category LIKE '%Bronze%' OR product_details LIKE '%Bronze%' THEN 'Bronze'
      WHEN product_category LIKE '%Starter%' OR product_details LIKE '%Starter%' THEN 'Starter'
      WHEN product_category LIKE '%Storage%' OR product_details LIKE '%Storage%' THEN 'Storage'
      WHEN product_category LIKE '%Silver%' OR product_details LIKE '%Silver%' THEN 'Silver'
      WHEN product_category LIKE '%Gold%' OR product_details LIKE '%Gold%' THEN 'Gold'
      WHEN product_category LIKE 'CI%' OR product_details LIKE 'CI%' THEN 'CI'
      WHEN product_category LIKE '%omput%' OR product_details LIKE '%omput%' THEN 'CI'
      WHEN product_category LIKE '%Duo%' OR product_details LIKE '%Duo%' THEN 'Duo Pro'
      WHEN product_category LIKE '%uggestion%' OR product_details LIKE '%uggestion%' THEN 'Duo Pro'
      WHEN product_category LIKE '%Agile%' OR product_details LIKE '%Agile%' THEN 'Enterprise Agile Planning'
      ELSE product_category
    END                                                                               AS product_tier,
    COALESCE (LOWER(product_details) LIKE ANY ('%code suggestions%', '%duo%'), FALSE) AS duo_flag,
    COALESCE (opportunity_name LIKE '%QSR%', FALSE)                                   AS qsr_flag,
    CASE
      WHEN order_type LIKE '7%' AND qsr_flag = FALSE THEN 'PS/CI/CD'
      WHEN order_type LIKE '1%' AND net_arr > 0 THEN 'First Order'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%') AND net_arr > 0 AND sales_type != 'Renewal'
        AND qsr_flag = FALSE THEN 'Growth - Uplift'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%', '7%') AND net_arr > 0 AND sales_type != 'Renewal'
        AND qsr_flag = TRUE THEN 'QSR - Uplift'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%', '7%') AND net_arr = 0 AND sales_type != 'Renewal'
        AND qsr_flag = TRUE THEN 'QSR - Flat'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%', '7%') AND net_arr < 0 AND sales_type != 'Renewal'
        AND qsr_flag = TRUE THEN 'QSR - Contraction'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%') AND net_arr > 0 AND sales_type = 'Renewal'
        THEN 'Renewal - Uplift'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%') AND net_arr < 0 AND sales_type != 'Renewal'
        THEN 'Non-Renewal - Contraction'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%') AND net_arr = 0 AND sales_type != 'Renewal'
        THEN 'Non-Renewal - Flat'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%') AND net_arr = 0 AND sales_type = 'Renewal'
        THEN 'Renewal - Flat'
      WHEN order_type LIKE ANY ('2.%', '3.%', '4.%') AND net_arr < 0 AND sales_type = 'Renewal'
        THEN 'Renewal - Contraction'
      WHEN order_type LIKE ANY ('5.%', '6.%') THEN 'Churn'
      ELSE 'Other'
    END                                                                               AS trx_type,
    COALESCE (opportunity_name LIKE '%Startups Program%', FALSE)                      AS startup_program_flag

  FROM mart_crm_opportunity
  LEFT JOIN dim_date
    ON mart_crm_opportunity.close_date = dim_date.date_actual
  LEFT JOIN dim_crm_user AS user
    ON mart_crm_opportunity.dim_crm_user_id = user.dim_crm_user_id

  WHERE ((mart_crm_opportunity.is_edu_oss = 1 AND net_arr > 0) OR mart_crm_opportunity.is_edu_oss = 0)
    AND mart_crm_opportunity.is_jihu_account = FALSE
    AND stage_name NOT LIKE '%Duplicate%'
    --and (opportunity_category is null or opportunity_category not like 'Decom%')
    AND partial_churn_0_narr_flag = FALSE
--and fiscal_year >= 2023
--and (is_won or (stage_name = '8-Closed Lost' and sales_type = 'Renewal') or is_closed = False)

)

--select * from opportunity_data;
,

/*
We pull all the Pooled cases, using the record type ID.

We have to manually parse the Subject field to get the Trigger Type, hopefully this will go away in future iterations.

-No spam filter
-Trigger Type logic only valid for FY25 onwards
*/

case_data AS (
  SELECT

    CASE
      WHEN subject LIKE 'Multiyear Renewal%' THEN 'Multiyear Renewal'
      WHEN subject LIKE 'EOA Renewal%' THEN 'EOA Renewal'
      WHEN subject LIKE 'PO Required%' THEN 'PO Required'
      WHEN subject LIKE 'Auto-Renewal Will Fail%' THEN 'Auto-Renewal Will Fail'
      WHEN subject LIKE 'Overdue Renewal%' THEN 'Overdue Renewal'
      WHEN subject LIKE 'Auto-Renew Recently Turned Off%' THEN 'Auto-Renew Recently Turned Off'
      WHEN subject LIKE 'Failed QSR%' THEN 'Failed QSR'
      ELSE subject
    END                                                            AS case_trigger,
    sfdc_case.*
    ,
    DATEDIFF('day', sfdc_case.created_date, sfdc_case.closed_date) AS case_days_to_close,
    DATEDIFF('day', sfdc_case.created_date, CURRENT_DATE)          AS case_age_days,
    dim_crm_user.user_name                                         AS case_owner_name,
    dim_crm_user.department                                        AS case_department,
    dim_crm_user.team,
    dim_crm_user.manager_name,
    dim_crm_user.user_role_name                                    AS case_user_role_name,
    dim_crm_user.user_role_type
  FROM sfdc_case
  LEFT JOIN dim_crm_user
    ON sfdc_case.owner_id = dim_crm_user.dim_crm_user_id
  WHERE sfdc_case.record_type_id = '0128X000001pPRkQAM'
    AND sfdc_case.created_date >= '2024-02-01'
--and sfdc_case.is_closed
-- and (sfdc_case.reason not like '%Spam%' or reason is null)
)
,

task_data AS (

-- Returns all completed Outreach tasks
-- Intended to be used mainly for understanding calls, meetings, and emails by AEs and SDRs
-- May need to be updated for new role names

  SELECT *
  FROM (
    SELECT
      task_id,
      task_status,
      task.dim_crm_account_id,
      task.dim_crm_user_id,
      task.dim_crm_person_id,
      task_subject,
      CASE
        WHEN LOWER(task_subject) LIKE '%email%' THEN 'Email'
        WHEN LOWER(task_subject) LIKE '%call%' THEN 'Call'
        WHEN LOWER(task_subject) LIKE '%linkedin%' THEN 'LinkedIn'
        WHEN LOWER(task_subject) LIKE '%inmail%' THEN 'LinkedIn'
        WHEN LOWER(task_subject) LIKE '%sales navigator%' THEN 'LinkedIn'
        WHEN LOWER(task_subject) LIKE '%drift%' THEN 'Chat'
        WHEN LOWER(task_subject) LIKE '%chat%' THEN 'Chat'
        ELSE
          task_type
      END                                                                                                                 AS type,
      CASE
        WHEN task_subject LIKE '%Outreach%' AND task_subject NOT LIKE '%Advanced Outreach%' THEN 'Outreach'
        WHEN task_subject LIKE '%Clari%' THEN 'Clari'
        WHEN task_subject LIKE '%Conversica%' THEN 'Conversica'
        ELSE 'Other'
      END                                                                                                                 AS outreach_clari_flag,
      task_created_date,
      task_created_by_id,

      --This is looking for either inbound emails (indicating they are from a customer) or completed phone calls

      CASE
        WHEN outreach_clari_flag = 'Outreach' AND (task_subject LIKE '%[Out]%' OR task_subject LIKE '%utbound%')
          THEN 'Outbound'
        WHEN outreach_clari_flag = 'Outreach' AND (task_subject LIKE '%[In]%' OR task_subject LIKE '%nbound%')
          THEN 'Inbound'
        ELSE 'Other'
      END                                                                                                                 AS inbound_outbound_flag,
      COALESCE ((
        inbound_outbound_flag = 'Outbound' AND task_subject LIKE '%Answered%'
        AND task_subject NOT LIKE '%Not Answer%'
        AND task_subject NOT LIKE '%No Answer%'
      )
      OR (LOWER(task_subject) LIKE '%call%' AND task_subject NOT LIKE '%Outreach%' AND task_status = 'Completed'), FALSE) AS outbound_answered_flag,
      task_date,
      CASE
        WHEN task.task_created_by_id LIKE '0054M000003Tqub%' THEN 'Outreach'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%GitLab Transactions%'
          THEN 'Post-Purchase'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Was Sent Email%'
          THEN 'SFDC Marketing Email Send'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Your GitLab License%'
          THEN 'Post-Purchase'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Advanced Outreach%'
          THEN 'Gainsight Marketing Email Send'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Filled Out Form%'
          THEN 'Marketo Form Fill'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Conversation in Drift%'
          THEN 'Drift'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Opened Email%'
          THEN 'Marketing Email Opened'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Sales Navigator%'
          THEN 'Sales Navigator'
        WHEN task.task_created_by_id NOT LIKE '0054M000003Tqub%' AND task_subject LIKE '%Clari - Email%'
          THEN 'Clari Email'
        ELSE
          'Other'
      END                                                                                                                 AS task_type,

      user.user_name                                                                                                      AS task_user_name,
      CASE
        WHEN user.department LIKE '%arketin%' THEN 'Marketing'
        ELSE user.department
      END                                                                                                                 AS department,
      user.is_active,
      user.crm_user_sales_segment,
      user.crm_user_geo,
      user.crm_user_region,
      user.crm_user_area,
      user.crm_user_business_unit,
      user.user_role_name
    FROM mart_crm_task AS task
    INNER JOIN dim_crm_user AS user
      ON task.dim_crm_user_id = user.dim_crm_user_id
    WHERE task.dim_crm_user_id IS NOT NULL
      AND is_deleted = FALSE
      AND task_date >= '2024-02-01'
      AND task_status = 'Completed'
  )

  WHERE (outreach_clari_flag = 'Outreach' OR task_created_by_id = dim_crm_user_id)
    AND outreach_clari_flag != 'Other'
    AND (
      user_role_name LIKE ANY ('%AE%', '%SDR%', '%BDR%', '%Advocate%')
      OR crm_user_sales_segment = 'SMB'
    )
)

--SELECT * from task_data; -- No records?
,

first_order AS (
  SELECT DISTINCT
    dim_parent_crm_account_id,
    LAST_VALUE(net_arr) OVER (PARTITION BY dim_parent_crm_account_id ORDER BY close_date ASC)     AS net_arr,
    LAST_VALUE(close_date) OVER (PARTITION BY dim_parent_crm_account_id ORDER BY close_date ASC)  AS close_date,
    LAST_VALUE(fiscal_year) OVER (PARTITION BY dim_parent_crm_account_id ORDER BY close_date ASC) AS fiscal_year,
    LAST_VALUE(sales_qualified_source_name)
      OVER (PARTITION BY dim_parent_crm_account_id ORDER BY close_date ASC)                       AS sqs
  FROM mart_crm_opportunity
  LEFT JOIN dim_date
    ON mart_crm_opportunity.close_date = dim_date.date_actual
  WHERE is_won
    AND order_type = '1. New - First Order'
),

latest_churn AS (
  SELECT
    snapshot_date                                                                               AS close_date,
    dim_crm_account_id,
    carr_this_account,
    LAG(carr_this_account, 1) OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC) AS prior_carr,
    -prior_carr                                                                                 AS net_arr
    ,
    MAX(CASE
      WHEN carr_this_account > 0 THEN snapshot_date
    END) OVER (PARTITION BY dim_crm_account_id)                                                 AS last_carr_date
  FROM dim_crm_account_daily_snapshot
  WHERE snapshot_date >= '2019-02-01'
    AND snapshot_date = DATE_TRUNC('month', snapshot_date)
  -- and dim_crm_account_id = '0014M00001lbBt1QAE'
  QUALIFY
    prior_carr > 0
    AND carr_this_account = 0
    AND snapshot_date > last_carr_date
),

high_value_case AS  (SELECT 
    *
    FROM (SELECT
    account_id,
    case_id,
    owner_id, 
    subject,
    dim_crm_user.user_name as case_owner_name,
    dim_crm_user.department as case_department,
    dim_crm_user.team,
    dim_crm_user.manager_name,
    dim_crm_user.user_role_name as case_user_role_name,
    dim_crm_user.user_role_type,
    sfdc_case.created_date,
    MAX(sfdc_case.created_date) over(partition by ACCOUNT_ID) as last_high_value_date
    FROM "PROD"."WORKSPACE_SALES"."SFDC_CASE" 
    LEFT JOIN common.dim_crm_user 
        ON sfdc_case.owner_id = dim_crm_user.dim_crm_user_id
    WHERE record_type_id in ('0128X000001pPRkQAM') 
--    and account_id = '0014M00001gTGESQA4'
    and lower(subject) like '%high value account%') -----subject placeholder - this could change
    WHERE created_date = last_high_value_date 
),

start_values AS ( -- This is a large slow to query table
  SELECT
    dim_crm_account_id,
    carr_account_family,
    carr_this_account,
    parent_crm_account_lam_dev_count,
    pte_score,
    ptc_score
  FROM dim_crm_account_daily_snapshot
  WHERE snapshot_date = '2024-02-10' -----placeholder date for start of year
),

first_high_value_case AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      case_id,
      owner_id,
      subject,
      dim_crm_user.user_name                                     AS case_owner_name,
      dim_crm_user.department                                    AS case_department,
      --    dim_crm_user.team,
      dim_crm_user.manager_name,
      dim_crm_user.user_role_name                                AS case_user_role_name,
      dim_crm_user.user_role_type,
      sfdc_case.created_date,
      MIN(sfdc_case.created_date) OVER (PARTITION BY account_id) AS first_high_value_date
    FROM sfdc_case
    LEFT JOIN dim_crm_user
      ON sfdc_case.owner_id = dim_crm_user.dim_crm_user_id
    WHERE record_type_id IN ('0128X000001pPRkQAM')
      AND LOWER(subject) LIKE '%high value account%'
  ) -----subject placeholder - this could change
  WHERE created_date = first_high_value_date
),

eoa_cohorts AS (
   select distinct dim_crm_account_id from
    (SELECT
mart_arr.ARR_MONTH
--,ping_created_at
,mart_arr.SUBSCRIPTION_END_MONTH
,mart_arr.DIM_CRM_ACCOUNT_ID
,mart_arr.CRM_ACCOUNT_NAME
--,MART_CRM_ACCOUNT.CRM_ACCOUNT_OWNER
,mart_arr.DIM_SUBSCRIPTION_ID
,mart_arr.DIM_SUBSCRIPTION_ID_ORIGINAL
,mart_arr.SUBSCRIPTION_NAME
,mart_arr.subscription_sales_type
,mart_arr.AUTO_PAY
,mart_arr.DEFAULT_PAYMENT_METHOD_TYPE
,mart_arr.CONTRACT_AUTO_RENEWAL
,mart_arr.TURN_ON_AUTO_RENEWAL
,mart_arr.TURN_ON_CLOUD_LICENSING
,mart_arr.CONTRACT_SEAT_RECONCILIATION
,mart_arr.TURN_ON_SEAT_RECONCILIATION
,case when mart_arr.CONTRACT_SEAT_RECONCILIATION = 'Yes' and mart_arr.TURN_ON_SEAT_RECONCILIATION = 'Yes' then true else false end as qsr_enabled_flag
,mart_arr.PRODUCT_TIER_NAME
,mart_arr.PRODUCT_DELIVERY_TYPE
,mart_arr.PRODUCT_RATE_PLAN_NAME
,mart_arr.ARR
--,monthly_mart.max_BILLABLE_USER_COUNT - monthly_mart.LICENSE_USER_COUNT AS overage_count
,(mart_arr.ARR / mart_arr.QUANTITY)  as arr_per_user,
arr_per_user/12 as monthly_price_per_user,
mart_arr.mrr/mart_arr.quantity as mrr_check
FROM RESTRICTED_SAFE_COMMON_MART_SALES.MART_ARR mart_arr
-- LEFT JOIN RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_ACCOUNT
--     ON mart_arr.DIM_CRM_ACCOUNT_ID = MART_CRM_ACCOUNT.DIM_CRM_ACCOUNT_ID
WHERE ARR_MONTH = '2023-01-01'
and PRODUCT_TIER_NAME like '%Premium%'
and ((monthly_price_per_user >= 14
and monthly_price_per_user <= 16) or (monthly_price_per_user >= 7.5 and monthly_price_per_user <= 9.5))
and dim_crm_account_id in
(
select
DIM_CRM_ACCOUNT_ID
--,product_rate_plan_name
from
RESTRICTED_SAFE_COMMON_MART_SALES.MART_ARR
where arr_month >= '2020-02-01'
and arr_month <= '2022-02-01'
and product_rate_plan_name like any ('%Bronze%','%Starter%')
))),

free_promo AS (
  SELECT DISTINCT dim_crm_account_id
  FROM mart_charge
  WHERE subscription_start_date >= '2023-02-01'
    AND rate_plan_charge_description = 'fo-discount-70-percent'
),

price_increase AS (
  SELECT DISTINCT dim_crm_account_id
  FROM (
    SELECT
      charge.*,
      arr / NULLIFZERO(quantity)     AS actual_price,
      prod.annual_billing_list_price AS list_price
    FROM mart_charge AS charge
    INNER JOIN dim_product_detail AS prod
      ON charge.dim_product_detail_id = prod.dim_product_detail_id
    WHERE subscription_start_date >= '2023-04-01'
      AND subscription_start_date <= '2023-07-01'
      AND type_of_arr_change = 'New'
      AND quantity > 0
      AND actual_price > 228
      AND actual_price < 290
      AND rate_plan_charge_name LIKE '%Premium%'
  )
),

ultimate AS (
  SELECT DISTINCT
    dim_parent_crm_account_id,
    arr_month
  FROM mart_arr
  WHERE product_tier_name LIKE '%Ultimate%'
    AND arr > 0
),

amer_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN
    ('AMER SMB Sales', 'APAC SMB Sales')
    OR owner_role = 'Advocate_SMB_AMER'
),

emea_accounts AS (
  SELECT dim_crm_account_id
  FROM mart_crm_account
  WHERE crm_account_owner IN
    ('EMEA SMB Sales')
    OR owner_role = 'Advocate_SMB_EMEA'
),

account_base AS (
  SELECT
    acct.*,
    CASE
      WHEN first_high_value_case.case_id IS NOT NULL THEN 'Tier 1'
      WHEN first_high_value_case.case_id IS NULL
        THEN
          CASE
            WHEN acct.carr_this_account > 7000 THEN 'Tier 1'
            WHEN acct.carr_this_account < 3000 AND acct.parent_crm_account_lam_dev_count < 10 THEN 'Tier 3'
            ELSE 'Tier 2'
          END
    END                                                              AS calculated_tier,
    CASE
      WHEN (acct.snapshot_date >= '2024-02-01' AND amer_accounts.dim_crm_account_id IS NOT NULL
    /*acct.dim_crm_account_id IN (
          SELECT dim_crm_account_id
          FROM mart_crm_account
          WHERE crm_account_owner IN
                ('AMER SMB Sales', 'APAC SMB Sales')
            OR owner_role = 'Advocate_SMB_AMER'
        )*/)
      OR (
        acct.snapshot_date < '2024-02-01' AND (
          acct.parent_crm_account_geo IN ('AMER', 'APJ', 'APAC')
          OR acct.parent_crm_account_region IN ('AMER', 'APJ', 'APAC')
        )
      ) THEN 'AMER/APJ'
      WHEN (acct.snapshot_date >= '2024-02-01' AND emea_accounts.dim_crm_account_id IS NOT NULL
    /*acct.dim_crm_account_id IN (
          SELECT dim_crm_account_id
          FROM mart_crm_account
          WHERE crm_account_owner IN
                ('EMEA SMB Sales')
            OR owner_role = 'Advocate_SMB_EMEA'
        )*/)
      OR (
        acct.snapshot_date < '2024-02-01' AND (
          acct.parent_crm_account_geo IN ('EMEA')
          OR acct.parent_crm_account_region IN ('EMEA')
        )
      ) THEN 'EMEA'
      ELSE 'Other'
    END                                                              AS team,
    COALESCE ((acct.snapshot_date >= '2024-02-01' AND (emea_accounts.dim_crm_account_id IS NOT NULL OR amer_accounts.dim_crm_account_id IS NOT NULL)
    /*acct.dim_crm_account_id IN (
          SELECT dim_crm_account_id
          FROM mart_crm_account
          WHERE crm_account_owner IN
                ('AMER SMB Sales', 'APAC SMB Sales', 'EMEA SMB Sales')
            OR owner_role LIKE 'Advocate%'
        )*/)
    OR
    (
      acct.snapshot_date < '2024-02-01'
      AND (acct.carr_account_family <= 30000 --and acct.carr_this_account > 0
      )
      AND (
        acct.parent_crm_account_max_family_employee <= 100
        OR acct.parent_crm_account_max_family_employee IS NULL
      )
      AND ultimate.dim_parent_crm_account_id IS NULL
      AND acct.parent_crm_account_sales_segment IN ('SMB', 'Mid-Market', 'Large')
      AND acct.parent_crm_account_upa_country != 'JP'
      AND acct.is_jihu_account = FALSE
    ), FALSE)                                                        AS gds_account_flag,
    fo.fiscal_year                                                   AS fo_fiscal_year,
    fo.close_date                                                    AS fo_close_date,
    fo.net_arr                                                       AS fo_net_arr,
    fo.sqs                                                           AS fo_sqs,
    churn.close_date                                                 AS churn_close_date,
    churn.net_arr                                                    AS churn_net_arr,
    NOT COALESCE (fo_fiscal_year <= 2024, FALSE)                     AS new_fy25_fo_flag,
    first_high_value_case.created_date                               AS first_high_value_case_created_date,
    high_value_case.case_owner_name                                  AS high_value_account_owner,
    --high_value_case.team as high_value_account_team,
    high_value_case.manager_name                                     AS high_value_manager_name,
    start_values.carr_account_family                                 AS starting_carr_account_family,
    start_values.carr_this_account                                   AS starting_carr_this_account,
    CASE
      WHEN start_values.carr_this_account > 7000 THEN 'Tier 1'
      WHEN start_values.carr_this_account < 3000 AND start_values.parent_crm_account_lam_dev_count < 10 THEN 'Tier 3'
      ELSE 'Tier 2'
    END                                                              AS starting_calculated_tier,
    start_values.pte_score                                           AS starting_pte_score,
    start_values.ptc_score                                           AS starting_ptc_score,
    start_values.parent_crm_account_lam_dev_count                    AS starting_parent_crm_account_lam_dev_count,
    COALESCE (eoa.dim_crm_account_id IS NOT NULL, FALSE)             AS eoa_flag,
    COALESCE (free_promo.dim_crm_account_id IS NOT NULL, FALSE)      AS free_promo_flag,
    COALESCE (price_increase.dim_crm_account_id IS NOT NULL, FALSE)  AS price_increase_promo_flag,
    COALESCE (ultimate.dim_parent_crm_account_id IS NOT NULL, FALSE) AS ultimate_customer_flag
  FROM dim_crm_account_daily_snapshot AS acct
  ------subquery that gets latest FO data
  LEFT JOIN first_order AS fo
    ON acct.dim_parent_crm_account_id = fo.dim_parent_crm_account_id
  ---------subquery that gets latest churn data
  LEFT JOIN latest_churn AS churn
    ON acct.dim_crm_account_id = churn.dim_crm_account_id
  --------subquery to get high tier case owner
  LEFT JOIN high_value_case
    ON acct.dim_crm_account_id = high_value_case.account_id
  --------------subquery to get start of FY25 values
  LEFT JOIN start_values
    ON acct.dim_crm_account_id = start_values.dim_crm_account_id
  -----subquery to get FIRST high value case
  LEFT JOIN first_high_value_case
    ON acct.dim_crm_account_id = first_high_value_case.account_id
  -----EOA cohort accounts
  LEFT JOIN eoa_cohorts AS eoa
    ON acct.dim_crm_account_id = eoa.dim_crm_account_id
  ------free limit promo cohort accounts
  LEFT JOIN free_promo
    ON acct.dim_crm_account_id = free_promo.dim_crm_account_id
  ------price increase promo cohort accounts
  LEFT JOIN price_increase
    ON acct.dim_crm_account_id = price_increase.dim_crm_account_id
  LEFT JOIN ultimate
    ON acct.dim_parent_crm_account_id = ultimate.dim_parent_crm_account_id
      AND ultimate.arr_month = DATE_TRUNC('month', acct.snapshot_date)
  ----- amer and apac accounts
  LEFT JOIN amer_accounts
    ON acct.dim_crm_account_id = amer_accounts.dim_crm_account_id
  ----- emea emea accounts
  LEFT JOIN emea_accounts
    ON acct.dim_crm_account_id = emea_accounts.dim_crm_account_id
  -------filtering to get current account data
  WHERE acct.carr_this_account > 0
    AND acct.snapshot_date >= '2019-12-01'
-- and(((acct.snapshot_date < '2024-02-01' or acct.snapshot_date >= '2024-03-01')
-- and  acct.snapshot_date = date_trunc('month',acct.snapshot_date))
-- or (date_trunc('month',acct.snapshot_date) = '2024-02-01' and acct.snapshot_date = current_date))
--and acct.dim_crm_account_id = '0014M00001ldLdiQAE'
)

--SELECT * from account_base;
,

opp AS (
  SELECT *
  FROM mart_crm_opportunity
  WHERE ((mart_crm_opportunity.is_edu_oss = 1 AND net_arr > 0) OR mart_crm_opportunity.is_edu_oss = 0)
    AND mart_crm_opportunity.is_jihu_account = FALSE
    AND stage_name NOT LIKE '%Duplicate%'
    AND (opportunity_category IS NULL OR opportunity_category NOT LIKE 'Decom%')
--and partial_churn_0_narr_flag = false
--and fiscal_year >= 2023
--and (is_won or (stage_name = '8-Closed Lost' and sales_type = 'Renewal') or is_closed = False)
),

upgrades AS ( 
  select distinct dim_crm_opportunity_id from
(
SELECT
   arr_month,
  type_of_arr_change,
  arr.product_category[0] as upgrade_product,
  previous_month_product_category[0] as prior_product,
  opp.is_web_portal_purchase,
  arr.dim_crm_account_id, 
  opp.dim_crm_opportunity_id,
  SUM(beg_arr)                      AS beg_arr,
  SUM(end_arr)                      AS end_arr,
  SUM(end_arr) - SUM(beg_arr)       AS delta_arr,
  SUM(seat_change_arr)              AS seat_change_arr,
  SUM(price_change_arr)             AS price_change_arr,
  SUM(tier_change_arr)              AS tier_change_arr,
  SUM(beg_quantity)                 AS beg_quantity,
  SUM(end_quantity)                 AS end_quantity,
  SUM(seat_change_quantity)         AS delta_seat_change,
  COUNT(*)                          AS nbr_customers_upgrading
FROM restricted_safe_legacy.mart_delta_arr_subscription_month arr
left join 
(
SELECT
*
FROM restricted_safe_common_mart_sales.mart_crm_opportunity
WHERE 
((mart_crm_opportunity.is_edu_oss = 1 and net_arr > 0) or mart_crm_opportunity.is_edu_oss = 0)
AND 
mart_crm_opportunity.is_jihu_account = False
AND stage_name not like '%Duplicate%'
and (opportunity_category is null or opportunity_category not like 'Decom%')
--and partial_churn_0_narr_flag = false
--and fiscal_year >= 2023
--and (is_won or (stage_name = '8-Closed Lost' and sales_type = 'Renewal') or is_closed = False)

)
 opp
  on (arr.DIM_CRM_ACCOUNT_ID = opp.DIM_CRM_ACCOUNT_ID
  and arr.arr_month = date_trunc('month',opp.subscription_start_date)
 -- and opp.order_type = '3. Growth'
  and opp.is_won)
WHERE 
 (ARRAY_CONTAINS('Self-Managed - Starter'::VARIANT, previous_month_product_category)
          OR ARRAY_CONTAINS('SaaS - Bronze'::VARIANT, previous_month_product_category)
       or ARRAY_CONTAINS('SaaS - Premium'::VARIANT, previous_month_product_category)
       or ARRAY_CONTAINS('Self-Managed - Premium'::VARIANT, previous_month_product_category)
       )
   AND tier_change_arr > 0
GROUP BY 1,2,3,4,5,6,7
))

--select * from upgrades;
,

promo_data_actual_price AS (
  SELECT
    mart_charge.*,
    dim_subscription.dim_crm_opportunity_id,
    MAX(arr) OVER (PARTITION BY mart_charge.dim_subscription_id)      AS actual_arr,
    MAX(quantity) OVER (PARTITION BY mart_charge.dim_subscription_id) AS actual_quantity,
    actual_arr / NULLIFZERO(actual_quantity)                          AS actual_price
  FROM mart_charge
  LEFT JOIN dim_subscription
    ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  WHERE
    -- effective_start_date >= '2023-02-01'
    -- and
    dim_subscription.subscription_start_date >= '2023-04-01'
    AND dim_subscription.subscription_start_date <= '2023-07-01'
    AND type_of_arr_change = 'New'
    AND quantity > 0
    -- and actual_price > 228
    -- and actual_price < 290
    AND rate_plan_charge_name LIKE '%Premium%'
  QUALIFY
    actual_price > 228
    AND actual_price < 290
),

price_increase_promo_fo_data AS (--Gets all FO opportunities associated with Price Increase promo

  SELECT DISTINCT
    dim_crm_opportunity_id,
    actual_price
  FROM promo_data_actual_price
)

--select * from price_increase_promo_fo_data;
,

bronz_starter_accounts AS (
  SELECT dim_crm_account_id
  --,product_rate_plan_name
  FROM mart_arr
  WHERE arr_month >= '2020-02-01'
    AND arr_month <= '2022-02-01'
    AND product_rate_plan_name LIKE ANY ('%Bronze%', '%Starter%')
),

eoa_accounts AS (
  SELECT
    mart_arr.arr_month,
    --,ping_created_at
    mart_arr.subscription_end_month,
    mart_arr.dim_crm_account_id,
    mart_arr.crm_account_name,
    --,MART_CRM_ACCOUNT.CRM_ACCOUNT_OWNER
    mart_arr.dim_subscription_id,
    mart_arr.dim_subscription_id_original,
    mart_arr.subscription_name,
    mart_arr.subscription_sales_type,
    mart_arr.auto_pay,
    mart_arr.default_payment_method_type,
    mart_arr.contract_auto_renewal,
    mart_arr.turn_on_auto_renewal,
    mart_arr.turn_on_cloud_licensing,
    mart_arr.contract_seat_reconciliation,
    mart_arr.turn_on_seat_reconciliation,
    COALESCE (mart_arr.contract_seat_reconciliation = 'Yes' AND mart_arr.turn_on_seat_reconciliation = 'Yes', FALSE) AS qsr_enabled_flag,
    mart_arr.product_tier_name,
    mart_arr.product_delivery_type,
    mart_arr.product_rate_plan_name,
    mart_arr.arr,
    --,monthly_mart.max_BILLABLE_USER_COUNT - monthly_mart.LICENSE_USER_COUNT AS overage_count
    (mart_arr.arr / NULLIFZERO(mart_arr.quantity))                                                                   AS arr_per_user,
    arr_per_user / 12                                                                                                AS monthly_price_per_user,
    mart_arr.mrr / NULLIFZERO(mart_arr.quantity)                                                                     AS mrr_check
  FROM mart_arr
-- LEFT JOIN RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_ACCOUNT
--     ON mart_arr.DIM_CRM_ACCOUNT_ID = MART_CRM_ACCOUNT.DIM_CRM_ACCOUNT_ID
  LEFT JOIN bronz_starter_accounts
    ON mart_arr.dim_crm_account_id = bronz_starter_accounts.dim_crm_account_id
  WHERE arr_month = '2023-01-01'
    AND product_tier_name LIKE '%Premium%'
    AND ((
      monthly_price_per_user >= 14
      AND monthly_price_per_user <= 16
    ) OR (monthly_price_per_user >= 7.5 AND monthly_price_per_user <= 9.5
    ))
    AND bronz_starter_accounts.dim_crm_account_id IS NOT NULL
/*dim_crm_account_id IN
                    (
                      SELECT
                        dim_crm_account_id
--,product_rate_plan_name
                      FROM mart_arr
                      WHERE arr_month >= '2020-02-01'
                        AND arr_month <= '2022-02-01'
                        AND product_rate_plan_name LIKE ANY ('%Bronze%', '%Starter%')
                    )*/
--ORDER BY mart_arr.dim_crm_account_id ASC
),

eoa_accounts_fy24 AS (
--Looks for current month ARR around $15 to account for currency conversion.
--Checks to make sure accounts previously had ARR in Bronze or Starter (to exclude accounts that just have discounts)
  SELECT DISTINCT dim_crm_account_id
  FROM eoa_accounts
),

past_eoa_uplift_opportunities AS (
  SELECT
    mart_charge.*,
    dim_subscription.dim_crm_opportunity_id,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal,
    previous_mrr / NULLIFZERO(previous_quantity) AS previous_price,
    mrr / NULLIFZERO(quantity)                   AS price_after_renewal
  FROM mart_charge
  LEFT JOIN dim_subscription
    ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  INNER JOIN eoa_accounts_fy24
    ON mart_charge.dim_crm_account_id = eoa_accounts_fy24.dim_crm_account_id
  -- left join PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY on dim_subscription.dim_crm_opportunity_id_current_open_renewal = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE mart_charge.rate_plan_name LIKE '%Premium%'
    -- and
    -- mart_charge.term_start_date <= '2024-02-01'
    AND type_of_arr_change != 'New'
    AND mart_charge.term_start_date >= '2022-02-01'
    AND price_after_renewal > previous_price
    AND previous_quantity != 0
    AND quantity != 0
    AND (
      LOWER(rate_plan_charge_description) LIKE '%eoa%'
      OR
      (
        (previous_price >= 5 AND previous_price <= 7)
        OR (previous_price >= 8 AND previous_price <= 10)
        OR (previous_price >= 14 AND previous_price <= 16)
      )
    )
),

past_eoa_uplift_opportunity_data AS (--Gets all opportunities associated with EoA special pricing uplift
  SELECT DISTINCT dim_crm_opportunity_id
  FROM past_eoa_uplift_opportunities
)

--select * from past_eoa_uplift_opportunity_data;
,
free_limit_promo_fo_data AS (--Gets all opportunities associated with Free Limit 70% discount

  SELECT DISTINCT
    dim_crm_opportunity_id
    -- ,
    -- actual_price
  FROM (
    SELECT
      mart_charge.*,
      dim_subscription.dim_crm_opportunity_id,
      MAX(arr) OVER (PARTITION BY mart_charge.dim_subscription_id)      AS actual_arr,
      MAX(quantity) OVER (PARTITION BY mart_charge.dim_subscription_id) AS actual_quantity,
      actual_arr / NULLIFZERO(actual_quantity)                          AS actual_price
    FROM mart_charge
    LEFT JOIN dim_subscription
      ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    WHERE
      -- effective_start_date >= '2023-02-01'
      -- and
      rate_plan_charge_description LIKE '%70%'
      AND type_of_arr_change = 'New'
  )
)

--select * from free_limit_promo_fo_data;
,

discounted_accounts AS (
  SELECT DISTINCT dim_crm_account_id
  FROM mart_charge
  WHERE subscription_start_date >= '2023-02-01'
    AND rate_plan_charge_description = 'fo-discount-70-percent'
),

open_eoa_renewals AS (
  SELECT
    mart_charge.*,
    dim_subscription.dim_crm_opportunity_id,
    close_date,
    MAX(mart_charge.arr) OVER (PARTITION BY mart_charge.dim_subscription_id)      AS actual_arr,
    MAX(mart_charge.quantity) OVER (PARTITION BY mart_charge.dim_subscription_id) AS actual_quantity,
    actual_arr / NULLIFZERO(actual_quantity)                                      AS actual_price,
    previous_mrr / NULLIFZERO(previous_quantity)                                  AS previous_price,
    mrr / NULLIFZERO(quantity)                                                    AS price_after_renewal,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal
  FROM mart_charge
  LEFT JOIN dim_subscription
    ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN mart_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id_current_open_renewal = mart_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN discounted_accounts
    ON mart_charge.dim_crm_account_id = discounted_accounts.dim_crm_account_id
  WHERE mart_crm_opportunity.close_date >= '2024-02-01'
    AND mart_crm_opportunity.is_closed = FALSE
    AND mart_charge.rate_plan_name LIKE '%Premium%'
    -- and
    -- mart_charge.term_start_date <= '2024-02-01'
    AND type_of_arr_change != 'New'
    AND mart_charge.term_start_date >= '2022-02-01'
    AND price_after_renewal > previous_price
    AND previous_quantity != 0
    AND quantity != 0
    AND discounted_accounts.dim_crm_account_id IS NULL
    /*mart_charge.dim_crm_account_id NOT IN
              (
                SELECT DISTINCT
                  dim_crm_account_id
                FROM restricted_safe_common_mart_sales.mart_charge charge
                WHERE subscription_start_date >= '2023-02-01'
                  AND rate_plan_charge_description = 'fo-discount-70-percent'
              )*/
    AND (
      LOWER(rate_plan_charge_description) LIKE '%eoa%'
      OR
      (
        (previous_price >= 5 AND previous_price <= 7)
        OR (previous_price >= 8 AND previous_price <= 10)
        OR (previous_price >= 14 AND previous_price <= 16)
      )
    )
),

open_eoa_renewal_data AS (
--Gets all future renewal opportunities where the account currently has EoA special pricing


  SELECT DISTINCT
    dim_crm_opportunity_id_current_open_renewal
    -- ,
    -- price_after_renewal,
    -- close_date
  FROM open_eoa_renewals


)

--select * from open_eoa_renewal_data;
,
renewal_self_service_data AS (--Uses the Order Action to determine if a Closed Won renewal was Autorenewed, Sales-Assisted, or Manual Portal Renew by the customer

  SELECT
    -- dim_order_action.dim_subscription_id,
    -- dim_subscription.subscription_name,
    -- dim_order_action.contract_effective_date,
    COALESCE (order_description != 'AutoRenew by CustomersDot', FALSE) AS manual_portal_renew_flag,
    -- mart_crm_opportunity.is_web_portal_purchase,
    mart_crm_opportunity.dim_crm_opportunity_id,
    CASE
      WHEN manual_portal_renew_flag AND is_web_portal_purchase THEN 'Manual Portal Renew'
      WHEN manual_portal_renew_flag = FALSE AND is_web_portal_purchase THEN 'Autorenew'
      ELSE 'Sales-Assisted Renew'
    END                                                                AS actual_manual_renew_flag
  FROM dim_order_action
  LEFT JOIN dim_order
    ON dim_order_action.dim_order_id = dim_order.dim_order_id
  LEFT JOIN dim_subscription
    ON dim_order_action.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN mart_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE order_action_type = 'RenewSubscription'
)

--select * from renewal_self_service_data;
,

price_increase_promo_renewal_open AS (
  SELECT
    charge.*,
    arr / NULLIFZERO(quantity)     AS actual_price,
    prod.annual_billing_list_price AS list_price,
    dim_subscription.dim_crm_opportunity_id_current_open_renewal
  FROM mart_charge AS charge
  INNER JOIN dim_product_detail AS prod
    ON charge.dim_product_detail_id = prod.dim_product_detail_id
  INNER JOIN dim_subscription
    ON charge.dim_subscription_id = dim_subscription.dim_subscription_id
  WHERE charge.term_start_date >= '2023-04-01'
    AND charge.term_start_date <= '2024-05-01'
    AND charge.type_of_arr_change != 'New'
    AND charge.subscription_start_date < '2023-04-01'
    AND charge.quantity > 0
    AND actual_price > 228
    AND actual_price < 290
    AND charge.rate_plan_charge_name LIKE '%Premium%'
),

price_increase_promo_renewal_open_data AS (
  SELECT DISTINCT dim_crm_opportunity_id_current_open_renewal
  FROM price_increase_promo_renewal_open

)
--SELECT * FROM price_increase_promo_renewal_open_data;
,
case_task_summary_data AS (
  SELECT
    opportunity_data.dim_crm_opportunity_id                                                       AS case_task_summary_id,

    LISTAGG(DISTINCT case_data.case_trigger, ', ') WITHIN GROUP (ORDER BY case_data.case_trigger)
      AS oppty_trigger_list,
    LISTAGG(DISTINCT case_data.case_id, ', ')
      AS oppty_case_id_list,

    COUNT(DISTINCT case_data.case_id)                                                             AS closed_case_count,
    COUNT(DISTINCT CASE
      WHEN case_data.status = 'Closed - Resolved' OR case_data.status = 'Closed'
        THEN case_data.case_id
    END)                                                                                          AS resolved_case_count,

    COUNT(DISTINCT CASE
      WHEN task_data.inbound_outbound_flag = 'Inbound' THEN task_data.task_id
    END)                                                                                          AS inbound_email_count,
    COUNT(DISTINCT CASE
      WHEN task_data.outbound_answered_flag THEN task_data.task_id
    END)                                                                                          AS completed_call_count,
    COUNT(DISTINCT CASE
      WHEN task_data.inbound_outbound_flag = 'Outbound' THEN task_data.task_id
    END)                                                                                          AS outbound_email_count,
    COALESCE (inbound_email_count > 0, FALSE)                                                     AS task_inbound_flag,
    COALESCE (completed_call_count > 0, FALSE)                                                    AS task_completed_call_flag,
    COALESCE (outbound_email_count > 0, FALSE)                                                    AS task_outbound_flag,
    COUNT(DISTINCT task_data.task_id)                                                             AS completed_task_count

  FROM opportunity_data
  LEFT JOIN case_data
    ON
      opportunity_data.dim_crm_account_id = case_data.account_id
      AND
      (
        (
          trx_type = 'First Order' AND opportunity_data.close_date >= case_data.created_date
          AND case_data.status = 'Closed - Resolved' AND case_data.created_date >= '2024-02-01'
        )
        OR
        (
          trx_type LIKE ANY ('%Growth%', '%QSR%') AND opportunity_data.close_date >= case_data.closed_date
          AND case_data.status = 'Closed - Resolved' AND case_data.closed_date >= opportunity_data.close_date - 90
          AND case_data.created_date >= '2024-02-01'
        )
        OR
        (
          trx_type LIKE ANY ('Renewal%', 'Churn') AND case_data.status = 'Closed - Resolved'
          AND case_data.closed_date <= opportunity_data.close_date + 30 AND case_data.created_date >= '2024-02-01'
        )
      )
  LEFT JOIN task_data
    ON
      opportunity_data.dim_crm_account_id = task_data.dim_crm_account_id
      AND
      (
        (
          trx_type = 'First Order' AND opportunity_data.close_date >= task_data.task_date
          AND task_data.task_date >= '2023-02-01'
        )
        OR
        (
          trx_type LIKE ANY ('%Growth%', '%QSR%') AND opportunity_data.close_date >= task_data.task_date
          AND task_data.task_date >= opportunity_data.close_date - 90 AND task_data.task_date >= '2023-02-01'
        )
        OR
        (
          trx_type LIKE ANY ('Renewal%', 'Churn') AND task_data.task_date <= opportunity_data.close_date + 30
          AND task_data.task_date >= '2023-02-01' AND task_data.task_date >= opportunity_data.close_date - 365
        )
      )
  --where case_data.account_id is not null
  GROUP BY 1
)
--select * from case_task_summary_data;
,
last_carr_data AS (
  SELECT
    snapshot_date,
    dim_crm_account_id,
    MAX(CASE
      WHEN carr_this_account > 0 THEN snapshot_date
    END)
      OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_carr_date,
    MIN(CASE
      WHEN carr_this_account > 0 THEN snapshot_date
    END) OVER (PARTITION BY dim_crm_account_id ORDER BY snapshot_date ASC)                                               AS first_carr_date
  FROM dim_crm_account_daily_snapshot
  WHERE snapshot_date >= '2019-12-01'
    AND crm_account_type != 'Prospect'
  -- and crm_account_name like 'Natural Substances%'
  QUALIFY last_carr_date IS NOT NULL
)
--SELECT * FROM last_carr_data;
,
full_base_data AS (
  SELECT
    opportunity_data.*,
    account_base.calculated_tier,
    CASE
      WHEN account_base.team IS NULL OR account_base.team = 'Other'
        THEN
          CASE
            WHEN opportunity_data.parent_crm_account_geo IN ('AMER', 'APJ', 'APAC')
              OR opportunity_data.parent_crm_account_region IN ('AMER', 'APJ', 'APAC') THEN 'AMER/APJ'
            WHEN opportunity_data.parent_crm_account_geo IN ('EMEA')
              OR opportunity_data.parent_crm_account_region IN ('EMEA') THEN 'EMEA'
            ELSE account_base.team
          END
      ELSE account_base.team
    END                                                                                                              AS team
    ,
    account_base.fo_fiscal_year,
    account_base.fo_close_date,
    account_base.fo_net_arr,
    account_base.fo_sqs,
    account_base.churn_net_arr,
    account_base.churn_close_date,
    account_base.new_fy25_fo_flag,
    account_base.high_value_account_owner,
    --account_base.high_value_account_team,
    account_base.high_value_manager_name,
    account_base.eoa_flag                                                                                            AS eoa_account_flag,
    account_base.free_promo_flag                                                                                     AS free_promo_account_flag,
    account_base.price_increase_promo_flag                                                                           AS price_increase_promo_account_flag,
    account_base.crm_account_owner,
    account_base.owner_role,
    account_base.account_tier,
    account_base.gds_account_flag,
    COALESCE ((
      opportunity_data.close_date >= '2024-02-01'
      -- not(opportunity_data.opportunity_owner like any ('%Taylor Lund%','%Miguel Nunes%','%Kazem Kutob%'))
      -- and
      AND (
        crm_opp_owner_sales_segment_stamped = 'SMB'
        OR (
          crm_opp_owner_sales_segment_stamped IS NULL
          AND (
            sales_type = 'New Business' OR crm_account_owner LIKE '%SMB Sales%'
            OR account_base.owner_role LIKE 'Advocate%'
          )
          AND account_base.carr_account_family <= 30000
          AND (
            account_base.parent_crm_account_max_family_employee <= 100
            OR account_base.parent_crm_account_max_family_employee IS NULL
          )
        )
      )
    )
    OR
    (
      opportunity_data.close_date < '2024-02-01'
      AND gds_account_flag
      AND (
        ultimate_customer_flag = FALSE OR upgrades.dim_crm_opportunity_id IS NOT NULL
        OR trx_type = 'First Order'
      )
    ), FALSE)                                                                                                        AS gds_oppty_flag,
    gs_health_user_engagement,
    gs_health_cd,
    gs_health_devsecops,
    gs_health_ci,
    gs_health_scm,
    carr_account_family,
    carr_this_account,
    pte_score,
    ptc_score,
    ptc_predicted_arr                                                                                                AS ptc_predicted_arr__c,
    ptc_predicted_renewal_risk_category                                                                              AS ptc_predicted_renewal_risk_category__c,
    -- upgrades.prior_product,
    -- upgrades.upgrade_product,
    COALESCE (upgrades.dim_crm_opportunity_id IS NOT NULL, FALSE)                                                    AS upgrade_flag,
    COALESCE (price_increase_promo_fo_data.dim_crm_opportunity_id IS NOT NULL, FALSE)                                AS price_increase_promo_fo_flag,
    COALESCE (free_limit_promo_fo_data.dim_crm_opportunity_id IS NOT NULL, FALSE)                                    AS free_limit_promo_fo_flag,
    COALESCE (past_eoa_uplift_opportunity_data.dim_crm_opportunity_id IS NOT NULL, FALSE)                            AS past_eoa_uplift_opportunity_flag,
    COALESCE (open_eoa_renewal_data.dim_crm_opportunity_id_current_open_renewal IS NOT NULL, FALSE)                  AS open_eoa_renewal_flag,
    COALESCE (price_increase_promo_renewal_open_data.dim_crm_opportunity_id_current_open_renewal IS NOT NULL, FALSE) AS price_increase_promo_open_renewal_flag,
    renewal_self_service_data.actual_manual_renew_flag                                                               AS actual_manual_renew_flag,
    case_task_summary_data.* EXCLUDE (case_task_summary_id),
    dim_subscription.turn_on_auto_renewal,
    ultimate_customer_flag,
    last_carr_date,
    first_carr_date,
    account_base.snapshot_date                                                                                       AS account_snapshot_date,
    last_carr_data.snapshot_date                                                                                     AS last_carr_snapshot_date,
    arr_basis                                                                                                        AS atr,
    CASE
      WHEN trx_type = 'First Order' OR order_type_live LIKE '%First Order%' THEN 'First Order'
      WHEN trx_type IN ('Renewal - Uplift', 'Renewal - Flat') THEN 'Renewal Growth'
      WHEN qsr_flag OR trx_type = 'Growth - Uplift' THEN 'Nonrenewal Growth'
      WHEN trx_type IN ('Churn', 'Renewal - Contraction', 'Non-Renewal - Contraction') THEN 'C&C'
      ELSE 'Other'
    END                                                                                                              AS trx_type_grouping,
    opportunity_data.close_date                                                                                      AS test_date
  FROM opportunity_data
  LEFT JOIN last_carr_data
    ON opportunity_data.dim_crm_account_id = last_carr_data.dim_crm_account_id
      AND ((
        order_type NOT LIKE '%First Order%'
        AND is_closed
        AND
        last_carr_data.snapshot_date = opportunity_data.close_date - 1
      )
      OR
      (
        order_type LIKE '%First Order%'
        AND is_closed
        AND last_carr_data.snapshot_date = last_carr_data.first_carr_date
      )
      OR
      (is_closed = FALSE AND last_carr_data.snapshot_date = CURRENT_DATE)
      )
  LEFT JOIN account_base
    ON opportunity_data.dim_crm_account_id = account_base.dim_crm_account_id
      AND
      (
        (
          is_closed
          AND order_type LIKE '%First Order%'
          AND account_base.snapshot_date = first_carr_date
          AND last_carr_data.dim_crm_account_id = account_base.dim_crm_account_id
        )
        OR
        (
          is_closed
          AND order_type NOT LIKE '%First Order%'
          AND account_base.snapshot_date = last_carr_date
          AND last_carr_data.dim_crm_account_id = account_base.dim_crm_account_id
        )
        OR
        (
          is_closed = FALSE
          AND account_base.snapshot_date = CURRENT_DATE
        )
      )
  LEFT JOIN upgrades
    ON opportunity_data.dim_crm_opportunity_id = upgrades.dim_crm_opportunity_id
  LEFT JOIN price_increase_promo_fo_data
    ON opportunity_data.dim_crm_opportunity_id = price_increase_promo_fo_data.dim_crm_opportunity_id
  LEFT JOIN past_eoa_uplift_opportunity_data
    ON opportunity_data.dim_crm_opportunity_id = past_eoa_uplift_opportunity_data.dim_crm_opportunity_id
  LEFT JOIN free_limit_promo_fo_data
    ON opportunity_data.dim_crm_opportunity_id = free_limit_promo_fo_data.dim_crm_opportunity_id
  LEFT JOIN open_eoa_renewal_data
    ON opportunity_data.dim_crm_opportunity_id = open_eoa_renewal_data.dim_crm_opportunity_id_current_open_renewal
  LEFT JOIN renewal_self_service_data
    ON opportunity_data.dim_crm_opportunity_id = renewal_self_service_data.dim_crm_opportunity_id
  LEFT JOIN case_task_summary_data
    ON
      opportunity_data.dim_crm_opportunity_id = case_task_summary_data.case_task_summary_id
  LEFT JOIN price_increase_promo_renewal_open_data
    ON opportunity_data.dim_crm_opportunity_id
      = price_increase_promo_renewal_open_data.dim_crm_opportunity_id_current_open_renewal
  LEFT JOIN (select
distinct
dim_crm_opportunity_id_current_open_renewal, 
first_value(turn_on_auto_renewal) over(partition by dim_crm_opportunity_id_current_open_renewal order by turn_on_auto_renewal asc) as TURN_ON_AUTO_RENEWAL
from common.dim_subscription
where dim_crm_opportunity_id_current_open_renewal is not null) dim_subscription
    ON opportunity_data.dim_crm_opportunity_id = dim_subscription.dim_crm_opportunity_id_current_open_renewal
  WHERE opportunity_data.dim_crm_opportunity_id != '0068X00001IozQZQAZ'
)

--select * from full_base_data;
,
renewal_forecast_base_data AS (
  SELECT
    team,
    calculated_tier,
    close_month,
    SUM(atr)                     AS total_atr,
    SUM(net_arr)                 AS total_net_arr,
    SUM(CASE
      WHEN trx_type_grouping = 'Renewal Growth' AND is_won THEN net_arr
      ELSE 0
    END) / NULLIFZERO(total_atr) AS renewal_growth_rate,
    SUM(CASE
      WHEN trx_type_grouping = 'C&C' THEN net_arr
      ELSE 0
    END) / NULLIFZERO(total_atr) AS c_and_c_rate,
    SUM(CASE
      WHEN ptc_predicted_arr__c IS NOT NULL AND is_closed = FALSE THEN ptc_predicted_arr__c
      ELSE 0
    END)
    - SUM(CASE
      WHEN ptc_predicted_arr__c IS NOT NULL AND is_closed = FALSE THEN atr
      ELSE 0
    END)                         AS ai_atr_net_forecast,
    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Churn (Actionable)'
        AND is_closed
        AND trx_type != 'Churn'
        THEN net_arr
      ELSE 0
    END)                         AS predicted_will_churn_narr_best_case,
    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Churn (Actionable)'
        AND is_closed
        THEN net_arr
      ELSE 0
    END)                         AS predicted_will_churn_narr_commit,

    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Contract (Actionable)'
        AND is_closed
        AND trx_type != 'Churn'
        THEN net_arr
      ELSE 0
    END)                         AS predicted_will_contract_narr_best_case,
    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Contract (Actionable)'
        AND is_closed
        THEN net_arr
      ELSE 0
    END)                         AS predicted_will_contract_narr_commit,

    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Renew'
        AND is_closed
        AND trx_type != 'Churn'
        THEN net_arr
      ELSE 0
    END)                         AS predicted_will_renew_narr_best_case,
    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Renew'
        AND is_closed
        THEN net_arr
      ELSE 0
    END)                         AS predicted_will_renew_narr_commit,


    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Churn (Actionable)'
        AND is_closed
        THEN atr
      ELSE 0
    END)                         AS predicted_will_churn_atr,
    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Contract (Actionable)'
        AND is_closed
        THEN atr
      ELSE 0
    END)                         AS predicted_will_contract_atr,
    SUM(CASE
      WHEN ptc_predicted_renewal_risk_category__c = 'Will Renew'
        AND is_closed
        THEN atr
      ELSE 0
    END)                         AS predicted_will_renew_atr
  FROM full_base_data
  WHERE close_date >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
    AND ((is_closed AND close_month < DATE_TRUNC('month', CURRENT_DATE)))
    AND sales_type = 'Renewal'
    AND team != 'Other'
    AND (crm_account_user_sales_segment = 'SMB' OR close_date < '2024-02-01')
    AND gds_oppty_flag
  GROUP BY 1, 2, 3
)

--select * from renewal_forecast_base_data;
,
renewal_forecast_rates AS (
  SELECT
    *,
    PERCENTILE_CONT(.8) WITHIN GROUP (ORDER BY renewal_growth_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS renewal_growth_rate_best_case,
    PERCENTILE_CONT(.4) WITHIN GROUP (ORDER BY renewal_growth_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS renewal_growth_rate_commit,
    PERCENTILE_CONT(.6) WITHIN GROUP (ORDER BY renewal_growth_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS renewal_growth_rate_most_likely,
    PERCENTILE_CONT(.8) WITHIN GROUP (ORDER BY c_and_c_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS c_and_c_rate_best_case,
    PERCENTILE_CONT(.4) WITHIN GROUP (ORDER BY c_and_c_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS c_and_c_rate_commit,
    PERCENTILE_CONT(.6) WITHIN GROUP (ORDER BY c_and_c_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS c_and_c_rate_most_likely
      --,
  -- sum(predicted_will_churn_narr_best_case)  over (partition by team,calculated_tier)
  --  /
  --  sum(predicted_will_churn_atr) over (partition by team,calculated_tier) as predicted_atr_rate_will_churn_best_case,

  --  sum(predicted_will_contract_narr_best_case)  over (partition by team,calculated_tier)
  --  /
  --  sum(predicted_will_contract_atr) over (partition by team,calculated_tier) as predicted_atr_rate_will_contract_best_case,

  --  sum(predicted_will_renew_narr_best_case)  over (partition by team,calculated_tier)
  --  /
  --  sum(predicted_will_renew_atr) over (partition by team,calculated_tier) as predicted_atr_rate_will_renew_best_case,

  --  sum(predicted_will_churn_narr_commit )  over (partition by team,calculated_tier)
  --  /
  --  sum(predicted_will_churn_atr) over (partition by team,calculated_tier) as predicted_atr_rate_will_churn_commit,

  --  sum(predicted_will_contract_narr_commit)  over (partition by team,calculated_tier)
  --  /
  --  sum(predicted_will_contract_atr) over (partition by team,calculated_tier) as predicted_atr_rate_will_contract_commit,

  --  sum(predicted_will_renew_narr_commit)  over (partition by team,calculated_tier)
  --  /
  --  sum(predicted_will_renew_atr) over (partition by team,calculated_tier) as predicted_atr_rate_will_renew_commit
  FROM renewal_forecast_base_data
)

--select * from renewal_forecast_rates;
,

limited_renewal_forecast_rates AS (
  SELECT DISTINCT
    team,
    calculated_tier,
    renewal_growth_rate_best_case,
    renewal_growth_rate_most_likely,
    renewal_growth_rate_commit,
    c_and_c_rate_best_case,
    c_and_c_rate_most_likely,
    c_and_c_rate_commit--,
  -- predicted_atr_rate_will_churn_best_case,
  -- predicted_atr_rate_will_churn_commit,
  -- predicted_atr_rate_will_contract_best_case,
  -- predicted_atr_rate_will_contract_commit,
  -- predicted_atr_rate_will_renew_best_case,
  -- predicted_atr_rate_will_renew_commit
  FROM renewal_forecast_rates
),

renewal_arr_predictions AS (
  SELECT
    full_base_data.*,
    CASE
      WHEN is_closed = FALSE THEN atr
      ELSE 0
    END                                                                     AS open_atr,
    limited_renewal_forecast_rates.* EXCLUDE (team, calculated_tier),
    --Best Case
    -- case when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C is not null then
    --   atr * (case when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Churn (Actionable)' then predicted_atr_rate_will_churn_best_case
    --               when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Contract (Actionable)' then predicted_atr_rate_will_contract_best_case
    --               when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Renew' then predicted_atr_rate_will_renew_best_case
    --               else 0 end)
    --   else atr * (renewal_growth_rate_best_case + c_and_c_rate_best_case) end as predicted_renewal_arr_best_case,
    open_atr * (renewal_growth_rate_best_case + c_and_c_rate_best_case)     AS predicted_renewal_arr_best_case_no_ai,
    --Commit
    -- case when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C is not null then
    --   atr * (case when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Churn (Actionable)' then (predicted_atr_rate_will_churn_best_case + predicted_atr_rate_will_churn_commit)/2
    --               when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Contract (Actionable)' then (predicted_atr_rate_will_contract_best_case + predicted_atr_rate_will_contract_commit)/2
    --               when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Renew' then (predicted_atr_rate_will_renew_best_case + predicted_atr_rate_will_renew_commit)/2
    --               else 0 end)
    --   else atr * (renewal_growth_rate_most_likely + c_and_c_rate_most_likely) end as predicted_renewal_arr_most_likely,
    open_atr * (renewal_growth_rate_most_likely + c_and_c_rate_most_likely) AS predicted_renewal_arr_most_likely_no_ai,
    --Most Likely
    -- case when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C is not null then
    --   atr * (case when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Churn (Actionable)' then predicted_atr_rate_will_churn_commit
    --               when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Contract (Actionable)' then predicted_atr_rate_will_contract_commit
    --               when PTC_PREDICTED_RENEWAL_RISK_CATEGORY__C = 'Will Renew' then predicted_atr_rate_will_renew_commit
    --               else 0 end)
    --   else atr * (renewal_growth_rate_commit + c_and_c_rate_commit) end as predicted_renewal_arr_commit,
    open_atr * (renewal_growth_rate_commit + c_and_c_rate_commit)           AS predicted_renewal_arr_commit_no_ai,
    open_atr * c_and_c_rate_best_case                                       AS predicted_c_and_c_arr_best_case,
    open_atr * c_and_c_rate_commit                                          AS predicted_c_and_c_arr_commit,
    open_atr * c_and_c_rate_most_likely                                     AS predicted_c_and_c_arr_most_likely
  FROM full_base_data
  LEFT JOIN limited_renewal_forecast_rates
    ON full_base_data.team = limited_renewal_forecast_rates.team
      AND full_base_data.calculated_tier = limited_renewal_forecast_rates.calculated_tier
  --and month(full_base_data.close_month = renewal_forecast_rates.close_month
  WHERE sales_type = 'Renewal'
    AND gds_oppty_flag
    AND full_base_data.sales_type = 'Renewal'
    --and full_base_data.is_closed = false
    AND full_base_data.fiscal_year = 2025
)

--select * from renewal_arr_predictions;
,
renewal_forecast_output AS (
  SELECT
    'Renewal'                                    AS forecast_type,
    team,
    calculated_tier,
    close_month,
    SUM(CASE
      WHEN is_closed THEN net_arr
      ELSE 0
    END)                                         AS total_actual_narr,
    SUM(CASE
      WHEN is_closed AND net_arr < 0 THEN net_arr
      ELSE 0
    END)                                         AS total_actual_c_and_c_narr,
    SUM(CASE
      WHEN is_closed AND net_arr > 0 THEN net_arr
      ELSE 0
    END)                                         AS total_actual_renewal_growth_narr,
    SUM(CASE
      WHEN is_closed THEN atr
      ELSE 0
    END)                                         AS total_closed_atr,
    SUM(atr)                                     AS total_atr,
    SUM(CASE
      WHEN is_closed = FALSE THEN atr
      ELSE 0
    END)                                         AS total_open_atr,
    SUM(CASE
      WHEN ptc_predicted_arr__c IS NOT NULL AND is_closed = FALSE THEN ptc_predicted_arr__c
      ELSE 0
    END)
    - SUM(CASE
      WHEN ptc_predicted_arr__c IS NOT NULL AND is_closed = FALSE THEN atr
      ELSE 0
    END)                                         AS ai_atr_net_forecast,
    -- sum(predicted_renewal_arr_best_case) as predicted_renewal_arr_best_case,
    -- sum(predicted_renewal_arr_commit) as predicted_renewal_arr_commit,
    -- sum(predicted_renewal_arr_most_likely) as predicted_renewal_arr_most_likely,
    SUM(predicted_renewal_arr_best_case_no_ai)   AS predicted_renewal_arr_best_case_no_ai,
    SUM(predicted_renewal_arr_commit_no_ai)      AS predicted_renewal_arr_commit_no_ai,
    SUM(predicted_renewal_arr_most_likely_no_ai) AS predicted_renewal_arr_most_likely_no_ai,
    SUM(predicted_c_and_c_arr_best_case)         AS predicted_c_and_c_arr_best_case,
    SUM(predicted_c_and_c_arr_commit)            AS predicted_c_and_c_arr_commit,
    SUM(predicted_c_and_c_arr_most_likely)       AS predicted_c_and_c_arr_most_likely
  FROM renewal_arr_predictions
  WHERE fiscal_year = 2025
  GROUP BY 1, 2, 3, 4
)
--SELECT * from renewal_forecast_output;
,

fo_forecast_base_data AS (
  SELECT
    team,
    DATEADD('year', 1, close_month)                  AS fy25_close_month,
    COUNT(DISTINCT dim_crm_opportunity_id)           AS fo_count,
    SUM(net_arr)                                     AS total_net_arr,
    COUNT(
      DISTINCT
      CASE
        WHEN free_promo_account_flag = FALSE AND price_increase_promo_account_flag = FALSE
          THEN
            dim_crm_opportunity_id
      END
    )                                                AS fo_count_without_promos,
    SUM(
      CASE
        WHEN free_promo_account_flag = FALSE AND price_increase_promo_account_flag = FALSE
          THEN
            net_arr
        ELSE 0
      END
    )                                                AS net_arr_without_promos,
    total_net_arr / fo_count                         AS total_actual_asp,
    net_arr_without_promos / fo_count_without_promos AS actual_asp_no_promos
  FROM full_base_data
  WHERE close_date >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
    AND ((is_closed AND close_month < DATE_TRUNC('month', CURRENT_DATE)))
    AND trx_type = 'First Order'
    AND team != 'Other'
    AND gds_oppty_flag
    AND is_won
  GROUP BY 1, 2
)

--SELECT * FROM fo_forecast_base_data;
,

date_spine AS (
  (
    SELECT
      date_actual,
      'EMEA' AS team
    FROM dim_date
    WHERE date_actual = DATE_TRUNC('month', date_actual)
      AND fiscal_year = 2025
  )
  UNION ALL
  (
    SELECT
      date_actual,
      'AMER/APJ' AS team
    FROM dim_date
    WHERE date_actual = DATE_TRUNC('month', date_actual)
      AND fiscal_year = 2025
  )
)
--SELECT * FROM date_spine;
,

fo_forecast_rates AS (
  SELECT DISTINCT
    team,
    PERCENTILE_CONT(.8) WITHIN GROUP (ORDER BY actual_asp_no_promos ASC) OVER (PARTITION BY team) AS asp_best_case,
    PERCENTILE_CONT(.4) WITHIN GROUP (ORDER BY actual_asp_no_promos ASC) OVER (PARTITION BY team) AS asp_commit,
    PERCENTILE_CONT(.6) WITHIN GROUP (ORDER BY actual_asp_no_promos ASC)
      OVER (PARTITION BY team)                                                                    AS asp_most_likely,
    PERCENTILE_CONT(.8) WITHIN GROUP (ORDER BY fo_count_without_promos ASC)
      OVER (PARTITION BY team)                                                                    AS fo_count_best_case,
    PERCENTILE_CONT(.4) WITHIN GROUP (ORDER BY fo_count_without_promos ASC)
      OVER (PARTITION BY team)                                                                    AS fo_count_commit,
    PERCENTILE_CONT(.6) WITHIN GROUP (ORDER BY fo_count_without_promos ASC)
      OVER (PARTITION BY team)                                                                    AS fo_count_most_likely
  FROM fo_forecast_base_data
)
--SELECT * FROM fo_forecast_rates;
,
fo_forecast_output AS (
  SELECT
    'First Order'                          AS forecast_type,
    date_spine.team,
    'First Order'                          AS calculated_tier,
    date_spine.date_actual                 AS close_month,
    fo_forecast_rates.* EXCLUDE (team),
    asp_best_case * fo_count_best_case     AS predicted_fo_arr_best_case,
    asp_commit * fo_count_commit           AS predicted_fo_arr_commit,
    asp_most_likely * fo_count_most_likely AS predicted_fo_arr_most_likely
  FROM date_spine
  LEFT JOIN fo_forecast_rates
    ON date_spine.team = fo_forecast_rates.team
)

--SELECT * FROM fo_forecast_output;
,

-- Cosider unifying with ultimate CTE
ultimate_accounts AS (
  SELECT DISTINCT dim_parent_crm_account_id
  FROM mart_arr
  WHERE arr_month = DATE_TRUNC('month', CURRENT_DATE)
    AND product_tier_name LIKE '%Ultimate%'
),

get_monthly_carr AS (
  SELECT
    DATEADD('year', 1, snapshot_date) AS fy25_target_month,
    CASE
      WHEN (acct.snapshot_date >= '2024-02-01' AND amer_accounts.dim_crm_account_id IS NOT NULL
    /*acct.dim_crm_account_id IN (
            SELECT dim_crm_account_id
            FROM prod.restricted_safe_common_mart_sales.mart_crm_account
            WHERE crm_account_owner IN
                  ('AMER SMB Sales', 'APAC SMB Sales')
          )*/)
      OR (
        acct.snapshot_date < '2024-02-01' AND (
          acct.parent_crm_account_geo IN ('AMER', 'APJ', 'APAC')
          OR acct.parent_crm_account_region IN ('AMER', 'APJ', 'APAC')
        )
      ) THEN 'AMER/APJ'
      WHEN (acct.snapshot_date >= '2024-02-01' AND emea_accounts.dim_crm_account_id IS NOT NULL
    /*acct.dim_crm_account_id IN (
            SELECT dim_crm_account_id
            FROM prod.restricted_safe_common_mart_sales.mart_crm_account
            WHERE crm_account_owner IN
                  ('EMEA SMB Sales')
          )*/)
      OR (
        acct.snapshot_date < '2024-02-01' AND (
          acct.parent_crm_account_geo IN ('EMEA')
          OR acct.parent_crm_account_region IN ('EMEA')
        )
      ) THEN 'EMEA'
      ELSE 'Other'
    END                               AS team,
    CASE
      WHEN carr_this_account > 7000 THEN 'Tier 1'
      WHEN carr_this_account < 3000 AND parent_crm_account_lam_dev_count < 10 THEN 'Tier 3'
      ELSE 'Tier 2'
    END                               AS calculated_tier,
    COALESCE ((acct.snapshot_date >= '2024-02-01' AND (amer_accounts.dim_crm_account_id IS NOT NULL OR emea_accounts.dim_crm_account_id IS NOT NULL)
    /*acct.dim_crm_account_id IN (
            SELECT dim_crm_account_id
            FROM prod.restricted_safe_common_mart_sales.mart_crm_account
            WHERE crm_account_owner IN
                  ('AMER SMB Sales', 'APAC SMB Sales', 'EMEA SMB Sales')
          )*/)
    OR
    (
      acct.snapshot_date < '2024-02-01'
      AND (acct.carr_account_family <= 30000 --and acct.carr_this_account > 0
      )
      AND (
        acct.parent_crm_account_max_family_employee <= 100
        OR acct.parent_crm_account_max_family_employee IS NULL
      )
      AND acct.parent_crm_account_sales_segment IN ('SMB', 'Mid-Market', 'Large')
      AND acct.parent_crm_account_upa_country != 'JP'
      AND acct.is_jihu_account = FALSE
      AND ultimate_accounts.dim_parent_crm_account_id IS NULL
      /*dim_parent_crm_account_id NOT IN
                       (
                         SELECT DISTINCT
                           dim_parent_crm_account_id
                         FROM prod.restricted_safe_common_mart_sales.mart_arr
                         WHERE arr_month = DATE_TRUNC('month', CURRENT_DATE)
                           AND product_tier_name LIKE '%Ultimate%'
                       )*/), FALSE)   AS gds_account_flag,
    SUM(carr_this_account)            AS total_monthly_carr
  FROM dim_crm_account_daily_snapshot AS acct
  LEFT JOIN amer_accounts
    ON acct.dim_crm_account_id = amer_accounts.dim_crm_account_id
  LEFT JOIN emea_accounts
    ON acct.dim_crm_account_id = emea_accounts.dim_crm_account_id
  LEFT JOIN ultimate_accounts
    ON acct.dim_parent_crm_account_id = ultimate_accounts.dim_parent_crm_account_id
  WHERE snapshot_date >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
    AND snapshot_date < DATE_TRUNC('month', CURRENT_DATE)
    AND snapshot_date = DATE_TRUNC('month', snapshot_date)
    AND gds_account_flag
    AND team != 'Other'
  GROUP BY 1, 2, 3, 4
)
--select * from get_monthly_carr;
,

nonrenewal_growth_base_data AS (
  SELECT
    team,
    calculated_tier,
    DATEADD('year', 1, close_month) AS fy25_close_month,
    SUM(net_arr)                    AS total_net_arr
  FROM full_base_data
  WHERE close_date >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
    AND is_closed
    AND close_month < DATE_TRUNC('month', CURRENT_DATE)
    AND trx_type_grouping = 'Nonrenewal Growth'
    AND team != 'Other'
    AND gds_oppty_flag
    AND is_won
  GROUP BY 1, 2, 3
)
--select * from nonrenewal_growth_base_data;
,

nonrenewal_growth_rates AS (
  SELECT
    nonrenewal_growth_base_data.*,
    get_monthly_carr.total_monthly_carr,
    total_net_arr / get_monthly_carr.total_monthly_carr AS nonrenewal_growth_rate
  FROM nonrenewal_growth_base_data
  LEFT JOIN get_monthly_carr
    ON nonrenewal_growth_base_data.team = get_monthly_carr.team
      AND nonrenewal_growth_base_data.calculated_tier = get_monthly_carr.calculated_tier
      AND nonrenewal_growth_base_data.fy25_close_month = get_monthly_carr.fy25_target_month
)
,
nonrenewal_growth_calcs AS (
  SELECT
    *,
    PERCENTILE_CONT(.8) WITHIN GROUP (ORDER BY nonrenewal_growth_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS nonrenewal_growth_best_case,
    PERCENTILE_CONT(.4) WITHIN GROUP (ORDER BY nonrenewal_growth_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS nonrenewal_growth_commit,
    PERCENTILE_CONT(.6) WITHIN GROUP (ORDER BY nonrenewal_growth_rate ASC)
      OVER (PARTITION BY team, calculated_tier) AS nonrenewal_growth_most_likely
  FROM nonrenewal_growth_rates
)
,
nonrenewal_growth_output AS (
  SELECT DISTINCT
    nonrenewal_growth_calcs.* EXCLUDE (fy25_close_month, total_net_arr, total_monthly_carr, nonrenewal_growth_rate),
    carr.total_monthly_carr AS carr
  FROM nonrenewal_growth_calcs
  LEFT JOIN
    (
      SELECT
        team,
        calculated_tier,
        total_monthly_carr
      FROM get_monthly_carr
      WHERE fy25_target_month = DATEADD('month', 11, DATE_TRUNC('month', CURRENT_DATE))
    ) AS carr
    ON nonrenewal_growth_calcs.team = carr.team
      AND nonrenewal_growth_calcs.calculated_tier = carr.calculated_tier
)
,
combined_output AS (
  SELECT *

  FROM (
    SELECT
      forecast_type,
      renewal_forecast_output.team,
      renewal_forecast_output.calculated_tier,
      close_month,
      ai_atr_net_forecast,
      -- predicted_renewal_arr_best_case,
      -- predicted_renewal_arr_commit,
      -- predicted_renewal_arr_most_likely,
      predicted_renewal_arr_best_case_no_ai,
      predicted_renewal_arr_commit_no_ai,
      predicted_renewal_arr_most_likely_no_ai,
      predicted_c_and_c_arr_best_case,
      predicted_c_and_c_arr_commit,
      predicted_c_and_c_arr_most_likely,
      0    AS asp_best_case,
      0    AS asp_commit,
      0    AS asp_most_likely,
      0    AS fo_count_best_case,
      0    AS fo_count_commit,
      0    AS fo_count_most_likely,
      0    AS predicted_fo_arr_best_case,
      0    AS predicted_fo_arr_commit,
      0    AS predicted_fo_arr_most_likely,
      nonrenewal_growth_best_case,
      nonrenewal_growth_commit,
      nonrenewal_growth_most_likely,
      carr AS current_carr
    FROM renewal_forecast_output
    LEFT JOIN nonrenewal_growth_output
      ON renewal_forecast_output.team = nonrenewal_growth_output.team
        AND renewal_forecast_output.calculated_tier = nonrenewal_growth_output.calculated_tier
  )
  UNION ALL
  (
    SELECT
      forecast_type,
      team,
      calculated_tier,
      close_month,
      0 AS ai_atr_net_forecast,
      0 AS predicted_renewal_arr_best_case,
      0 AS predicted_renewal_arr_commit,
      0 AS predicted_renewal_arr_most_likely,
      -- 0 as predicted_renewal_arr_best_case_no_ai,
      -- 0 as predicted_renewal_arr_commit_no_ai,
      -- 0 as predicted_renewal_arr_most_likely_no_ai,
      0 AS predicted_c_and_c_arr_best_case,
      0 AS predicted_c_and_c_arr_commit,
      0 AS predicted_c_and_c_arr_most_likely,
      asp_best_case,
      asp_commit,
      asp_most_likely,
      fo_count_best_case,
      fo_count_commit,
      fo_count_most_likely,
      predicted_fo_arr_best_case,
      predicted_fo_arr_commit,
      predicted_fo_arr_most_likely,
      0 AS nonrenewal_growth_best_case,
      0 AS nonrenewal_growth_commit,
      0 AS nonrenewal_growth_most_likely,
      0 AS current_carr
    FROM fo_forecast_output
  )
),

actual_data AS (
  SELECT
    close_month,
    CASE
      WHEN team IS NULL OR team = 'Other' THEN 'AMER/APJ'
      ELSE team
    END                                    AS team,
    CASE
      WHEN trx_type_grouping = 'First Order' THEN 'First Order'
      ELSE
        COALESCE (calculated_tier, 'Tier 3')
    END                                    AS calculated_tier,
    CASE
      WHEN trx_type_grouping IN ('Renewal Growth', 'C&C', 'Nonrenewal Growth') THEN 'Renewal'
      WHEN trx_type_grouping = 'First Order' THEN 'First Order'
      ELSE 'Renewal'
    END                                    AS forecast_type,
    SUM(net_arr)                           AS total_net_arr,
    SUM(CASE
      WHEN is_won OR (is_closed AND sales_type = 'Renewal') THEN net_arr
      ELSE 0
    END)                                   AS closed_net_arr,
    COUNT(DISTINCT dim_crm_opportunity_id) AS total_trx,
    COUNT(DISTINCT CASE
      WHEN is_won THEN dim_crm_opportunity_id
    END)                                   AS won_trx,
    SUM(CASE
      WHEN sales_type = 'Renewal' THEN atr
      ELSE 0
    END)                                   AS total_atr,
    SUM(CASE
      WHEN is_closed AND sales_type = 'Renewal' THEN atr
      ELSE 0
    END)                                   AS closed_total_atr,
    SUM(CASE
      WHEN is_won AND trx_type_grouping = 'Nonrenewal Growth' THEN net_arr
      ELSE 0
    END)                                   AS closed_nonrenewal_arr,
    SUM(CASE
      WHEN is_closed AND trx_type_grouping = 'C&C' AND net_arr < 0 THEN net_arr
      ELSE 0
    END)                                   AS closed_c_and_c_arr,
    SUM(CASE
      WHEN is_won AND forecast_type = 'First Order' THEN net_arr
      ELSE 0
    END)                                   AS closed_fo_arr,
    SUM(CASE
      WHEN is_closed AND trx_type_grouping = 'Renewal Growth' AND net_arr > 0 THEN net_arr
      ELSE 0
    END)                                   AS closed_renewal_arr,
    SUM(CASE
      WHEN (is_won OR (is_closed AND sales_type = 'Renewal')) AND trx_type_grouping != 'First Order'
        AND crm_account_user_sales_segment != 'SMB' THEN net_arr
      ELSE 0
    END)                                   AS non_gds_account_arr
  FROM full_base_data
  WHERE --forecast_type is not null
    --and
    gds_oppty_flag
    AND fiscal_year = 2025
  GROUP BY 1, 2, 3, 4
)
,
add_actual_data AS (
  SELECT
    combined_output.* EXCLUDE (predicted_fo_arr_best_case, predicted_fo_arr_commit, predicted_fo_arr_most_likely
    ),
    ZEROIFNULL(actual_data.total_net_arr)                                                                  AS total_net_arr_actual,
    ZEROIFNULL(actual_data.total_trx)                                                                      AS total_trx_actual,
    ZEROIFNULL(actual_data.total_atr)                                                                      AS total_atr_actual,
    ZEROIFNULL(actual_data.closed_total_atr)                                                               AS closed_total_atr_actual,
    ZEROIFNULL(actual_data.closed_net_arr)                                                                 AS closed_net_arr_actual,
    ZEROIFNULL(actual_data.won_trx)                                                                        AS won_trx_actual,
    ZEROIFNULL(actual_data.closed_c_and_c_arr)                                                             AS closed_c_and_c_arr_actual,
    ZEROIFNULL(actual_data.closed_nonrenewal_arr)                                                          AS closed_nonrenewal_arr_actual,
    ZEROIFNULL(actual_data.closed_fo_arr)                                                                  AS closed_fo_arr_actual,
    ZEROIFNULL(actual_data.closed_renewal_arr)                                                             AS closed_renewal_arr_actual,
    ZEROIFNULL(actual_data.non_gds_account_arr)                                                            AS closed_non_gds_account_arr_actual,
    (DATEDIFF(
      'day', GREATEST(CURRENT_DATE, combined_output.close_month),
      DATEADD('month', 1, combined_output.close_month)
    ) - 1)
    / (DATEDIFF('day', combined_output.close_month, DATEADD('month', 1, combined_output.close_month)) - 1)
      AS perc_month_remaining_mid,
    CASE
      WHEN perc_month_remaining_mid < 0 THEN 0
      ELSE perc_month_remaining_mid
    END                                                                                                    AS perc_month_remaining,
    predicted_fo_arr_best_case * perc_month_remaining                                                      AS predicted_fo_arr_best_case,
    predicted_fo_arr_most_likely * perc_month_remaining                                                    AS predicted_fo_arr_most_likely,
    predicted_fo_arr_commit * perc_month_remaining                                                         AS predicted_fo_arr_commit,
    (ZEROIFNULL(
      LAG(predicted_renewal_arr_best_case_no_ai + closed_net_arr_actual, 1)
        OVER (
          PARTITION BY combined_output.forecast_type, combined_output.team, combined_output.calculated_tier
          ORDER BY combined_output.close_month ASC
        )
    ) + current_carr + predicted_c_and_c_arr_best_case)
    * nonrenewal_growth_best_case * perc_month_remaining                                                   AS predicted_nonrenewal_arr_best_case,
    (ZEROIFNULL(
      LAG(predicted_renewal_arr_most_likely_no_ai + closed_net_arr_actual, 1)
        OVER (
          PARTITION BY combined_output.forecast_type, combined_output.team, combined_output.calculated_tier
          ORDER BY combined_output.close_month ASC
        )
    ) + current_carr
    + predicted_c_and_c_arr_most_likely) * nonrenewal_growth_most_likely
    * perc_month_remaining                                                                                 AS predicted_nonrenewal_arr_most_likely,
    (ZEROIFNULL(
      LAG(predicted_renewal_arr_commit_no_ai + closed_net_arr_actual, 1)
        OVER (
          PARTITION BY combined_output.forecast_type, combined_output.team, combined_output.calculated_tier
          ORDER BY combined_output.close_month ASC
        )
    ) + current_carr + predicted_c_and_c_arr_commit)
    * nonrenewal_growth_commit * perc_month_remaining                                                      AS predicted_nonrenewal_arr_commit
  FROM --actual_data full outer join
    combined_output
  LEFT JOIN actual_data
    ON combined_output.close_month = actual_data.close_month
      AND combined_output.team = actual_data.team
      AND combined_output.calculated_tier = actual_data.calculated_tier
      AND combined_output.forecast_type = actual_data.forecast_type
      --where combined_output.team <> 'Other'
  --and combined_output.calculated_tier is not null
  --and combined_output.team is not null
  ORDER BY
    combined_output.forecast_type, combined_output.team, combined_output.calculated_tier,
    combined_output.close_month
)

SELECT *
FROM add_actual_data
