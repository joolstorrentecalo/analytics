WITH prep_crm_opportunity AS (

  SELECT * 
  FROM {{ref('prep_crm_opportunity')}}

), 

live_date AS (

    SELECT *
    FROM {{ref('dim_date')}}
    WHERE date_actual = CURRENT_DATE

), 

sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

),

sfdc_opportunity_live AS (

  SELECT
    sfdc_opportunity_source.opportunity_id                                                                        AS dim_crm_opportunity_id,
    sfdc_opportunity_source.order_type_stamped                                                                    AS order_type_live,
    {{ sales_qualified_source_cleaning('sfdc_opportunity_source.sales_qualified_source') }}                       AS sales_qualified_source_live,
    sfdc_opportunity_source.user_segment_stamped                                                                  AS crm_opp_owner_sales_segment_stamped_live,
    sfdc_opportunity_source.user_geo_stamped                                                                      AS crm_opp_owner_geo_stamped_live,
    sfdc_opportunity_source.user_region_stamped                                                                   AS crm_opp_owner_region_stamped_live,
    sfdc_opportunity_source.user_area_stamped                                                                     AS crm_opp_owner_area_stamped_live,
    sfdc_opportunity_source.user_segment_geo_region_area_stamped                                                  AS crm_opp_owner_sales_segment_geo_region_area_stamped_live,
    sfdc_opportunity_source.user_business_unit_stamped                                                            AS crm_opp_owner_business_unit_stamped_live,
    sfdc_opportunity_source.is_edu_oss                                                                            AS is_edu_oss_live,
    sfdc_opportunity_source.opportunity_category                                                                  AS opportunity_category_live,
    sfdc_opportunity_source.is_deleted                                                                            AS is_deleted_live,      
    sfdc_opportunity_source.forecast_category_name                                                                AS forecast_category_name_live,
    sfdc_opportunity_source.fpa_master_bookings_flag                                                              AS fpa_master_bookings_flag_live,

    CASE
    WHEN sfdc_opportunity_source.order_type = '1. New - First Order'
      THEN '1. New'
    WHEN sfdc_opportunity_source.order_type IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction')
      THEN '2. Growth'
    ELSE '3. Other'
  END                                                                                                             AS deal_group_live,
    IFF(LOWER(sfdc_opportunity_source.sales_type) like '%renewal%', 1, 0)                                         AS is_renewal_live,
    sfdc_opportunity_source.is_closed                                                                             AS is_closed_live,
    sfdc_opportunity_source.is_web_portal_purchase                                                                AS is_web_portal_purchase_live,
    sfdc_opportunity_source.is_won                                                                                AS is_won_live,
    sfdc_opportunity_source.stage_name                                                                            AS stage_name_live,
    sfdc_opportunity_source.order_type_grouped                                                                    AS order_type_grouped_live,
    sfdc_opportunity_source.created_date::DATE                                                                    AS created_date_live,
    sfdc_opportunity_source.sales_accepted_date::DATE                                                             AS sales_accepted_date_live,
    sfdc_opportunity_source.close_date::DATE                                                                      AS close_date_live,
    sfdc_opportunity_source.net_arr                                                                               AS net_arr_live,
    {{ dbt_utils.generate_surrogate_key(['sfdc_opportunity_source.opportunity_id',"'99991231'"])}}                         AS crm_opportunity_snapshot_id,
    sfdc_account.is_jihu_account                                                                                  AS is_jihu_account_live,
    sfdc_opportunity_source.iacv_created_date                                                                     AS arr_created_date_live,   
    CASE
      WHEN sfdc_opportunity_source.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 
                                                  'Closed Won', '10-Duplicate')
          THEN 0
      ELSE 1
    END                                                                                                           AS is_open_live,
    1                                                                                                             AS is_live
  FROM {{ref('sfdc_opportunity_source')}}
  LEFT JOIN live_date
    ON CURRENT_DATE() = live_date.date_actual
  LEFT JOIN sfdc_account
    ON sfdc_opportunity_source.account_id= sfdc_account.account_id
  WHERE sfdc_opportunity_source.account_id IS NOT NULL
    AND sfdc_opportunity_source.is_deleted = FALSE

),

final AS (

  SELECT 
    prep_crm_opportunity.*,
    COALESCE(
    prep_crm_opportunity.is_pipeline_created_eligible,
    CASE
      WHEN sfdc_opportunity_live.order_type_live IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
        AND sfdc_opportunity_live.is_edu_oss_live  = 0
        AND prep_crm_opportunity.arr_created_fiscal_quarter_date IS NOT NULL
        AND sfdc_opportunity_live.opportunity_category_live  IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
        -- 20211222 Adjusted to remove the ommitted filter
        AND prep_crm_opportunity.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
        AND (net_arr > 0
          OR sfdc_opportunity_live.opportunity_category_live  = 'Credit')
        -- 20220128 Updated to remove webdirect SQS deals
        AND sfdc_opportunity_live.sales_qualified_source_live   != 'Web Direct Generated'
        AND sfdc_opportunity_live.is_jihu_account_live = 0
      THEN 1
      ELSE 0
    END
  )                                                                                                                 AS is_net_arr_pipeline_created_combined,
  CASE
    WHEN LOWER(sfdc_opportunity_live.order_type_grouped_live) LIKE ANY ('%growth%', '%new%')
      AND sfdc_opportunity_live.is_edu_oss_live = 0
      AND prep_crm_opportunity.is_stage_1_plus = 1
      AND prep_crm_opportunity.forecast_category_name != 'Omitted'
      AND prep_crm_opportunity.is_open = 1
      THEN 1
      ELSE 0
  END                                                                                                               AS is_eligible_open_pipeline_combined,
  CASE
    WHEN
    prep_crm_opportunity.dim_parent_crm_account_id IN (
      '001610000111bA3',
      '0016100001F4xla',
      '0016100001CXGCs',
      '00161000015O9Yn',
      '0016100001b9Jsc'
    )
    AND prep_crm_opportunity.close_date < '2020-08-01'
    THEN 1
  -- NF 2021 - Pubsec extreme deals
    WHEN
      prep_crm_opportunity.dim_crm_opportunity_id IN ('0064M00000WtZKUQA3', '0064M00000Xb975QAB')
      AND (prep_crm_opportunity.snapshot_date < '2021-05-01' OR prep_crm_opportunity.is_live = 1)
      THEN 1
    -- exclude vision opps from FY21-Q2
    WHEN prep_crm_opportunity.arr_created_fiscal_quarter_name = 'FY21-Q2'
      AND prep_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised = 90
      AND prep_crm_opportunity.stage_name IN (
        '00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
      )
      THEN 1
    -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
    WHEN
      prep_crm_opportunity.dim_crm_opportunity_id IN (
        '0064M00000ZGpfQQAT', '0064M00000ZGpfVQAT', '0064M00000ZGpfGQAT'
      )
      THEN 1
      -- remove test accounts
      WHEN
        prep_crm_opportunity.dim_crm_account_id = '0014M00001kGcORQA0'
        THEN 1
      --remove test accounts
      WHEN (prep_crm_opportunity.dim_parent_crm_account_id = ('0016100001YUkWVAA1')
          OR prep_crm_opportunity.dim_crm_account_id IS NULL)
        THEN 1
      -- remove jihu accounts
      WHEN sfdc_opportunity_live.is_jihu_account_live = 1
        THEN 1
      -- remove deleted opps
      WHEN prep_crm_opportunity.is_deleted = 1
        THEN 1
        ELSE 0
  END                                                                                                               AS is_excluded_from_pipeline_created_combined,
  CASE
    WHEN prep_crm_opportunity.arr_created_fiscal_quarter_date = prep_crm_opportunity.close_fiscal_quarter_date
      AND is_net_arr_pipeline_created_combined = 1
        THEN net_arr
    ELSE 0
  END                                                                                                               AS created_and_won_same_quarter_net_arr_combined,
  CASE
    WHEN sfdc_opportunity_live.is_edu_oss_live = 0
      AND prep_crm_opportunity.is_deleted = 0
      AND prep_crm_opportunity.is_renewal = 0
      AND sfdc_opportunity_live.order_type_live IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
      AND sfdc_opportunity_live.opportunity_category_live IN ('Standard','Ramp Deal','Decommissioned')
      AND prep_crm_opportunity.is_web_portal_purchase = 0
        THEN 1
      ELSE 0
  END                                                                                                               AS is_eligible_age_analysis_combined,
  CASE
    WHEN prep_crm_opportunity.is_renewal = 1 AND prep_crm_opportunity.is_closed = 1
        THEN DATEDIFF(day, arr_created_date, close_date)
    WHEN prep_crm_opportunity.is_renewal = 0 AND prep_crm_opportunity.is_closed = 1
        THEN DATEDIFF(day, prep_crm_opportunity.created_date, close_date)
      WHEN prep_crm_opportunity.is_renewal = 1 AND prep_crm_opportunity.is_open = 1
        THEN DATEDIFF(day, arr_created_date, prep_crm_opportunity.snapshot_date)
    WHEN prep_crm_opportunity.is_renewal = 0 AND prep_crm_opportunity.is_open = 1
        THEN DATEDIFF(day, prep_crm_opportunity.created_date, prep_crm_opportunity.snapshot_date)
  END                                                                                                               AS cycle_time_in_days_combined
  FROM prep_crm_opportunity
  LEFT JOIN sfdc_opportunity_live
    ON sfdc_opportunity_live.dim_crm_opportunity_id = prep_crm_opportunity.dim_crm_opportunity_id


)


SELECT * 
FROM final