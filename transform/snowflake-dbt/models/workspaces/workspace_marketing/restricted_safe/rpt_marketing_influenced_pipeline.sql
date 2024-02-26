{{ config(materialized='table') }}



{{ simple_cte([
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('wk_sales_sfdc_opportunity_snapshot_history_xf','wk_sales_sfdc_opportunity_snapshot_history_xf'),
    ('mart_crm_opportunity_stamped_hierarchy_hist','mart_crm_opportunity_stamped_hierarchy_hist'),
    ('mart_crm_account','mart_crm_account'),
    ('sfdc_bizible_attribution_touchpoint_snapshots_source', 'sfdc_bizible_attribution_touchpoint_snapshots_source'),
    ('dim_date','dim_date')
]) }}

, snapshot_dates AS (
  SELECT DISTINCT
  date_day,
  fiscal_year,
  fiscal_quarter,
  fiscal_quarter_name_fy,
  snapshot_date_fpa
  FROM 
  dim_date
  WHERE 
  day_of_fiscal_quarter_normalised = 90 AND date_day BETWEEN '2023-01-31' AND CURRENT_DATE-2
  UNION 
  
--latest snapshot for current quarter
  SELECT DISTINCT
  date_day,
  fiscal_year,
  fiscal_quarter,
  fiscal_quarter_name_fy,
  snapshot_date_fpa
  FROM 
  dim_date 
  WHERE (day_of_fiscal_quarter_normalised BETWEEN 3 AND 89) AND date_day = CURRENT_DATE-2 

    ORDER BY 1 DESC


),  attribution_touchpoint_snapshot_base AS (
    SELECT DISTINCT
    snapshot_dates.date_day AS touchpoint_snapshot_date,
    snapshot_dates.fiscal_quarter_name_fy AS snapshot_quarter_name,
    sfdc_bizible_attribution_touchpoint_snapshots_source.touchpoint_id AS dim_crm_touchpoint_id,
    sfdc_bizible_attribution_touchpoint_snapshots_source.opportunity_id AS dim_crm_opportunity_id,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_touchpoint_date,
    mart_crm_attribution_touchpoint.bizible_touchpoint_type,
    mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
    mart_crm_attribution_touchpoint.bizible_marketing_channel,
    mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_marketing_channel AS snapshot_marketing_channel,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_marketing_channel_path AS snapshot_marketing_channel_path,
    mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
    mart_crm_attribution_touchpoint.budget_holder,
    mart_crm_attribution_touchpoint.campaign_rep_role_name,
    mart_crm_attribution_touchpoint.campaign_region,
    mart_crm_attribution_touchpoint.campaign_sub_region,
    mart_crm_attribution_touchpoint.budgeted_cost,
    mart_crm_attribution_touchpoint.actual_cost,
    mart_crm_attribution_touchpoint.utm_campaign,
    mart_crm_attribution_touchpoint.utm_medium,
    mart_crm_attribution_touchpoint.utm_source,
      
    mart_crm_attribution_touchpoint.utm_budget,
    mart_crm_attribution_touchpoint.utm_content,
    mart_crm_attribution_touchpoint.utm_allptnr,
    mart_crm_attribution_touchpoint.utm_partnerid,

    mart_crm_attribution_touchpoint.integrated_budget_holder,
    mart_crm_attribution_touchpoint.type AS sfdc_campaign_type,
    mart_crm_attribution_touchpoint.gtm_motion,
    mart_crm_attribution_touchpoint.account_demographics_sales_segment AS person_sales_segment,
    mart_crm_attribution_touchpoint.touchpoint_offer_type,
    mart_crm_attribution_touchpoint.touchpoint_offer_type_grouped,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_weight_custom_model/100 AS bizible_count_custom_model,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_weight_custom_model,
    mart_crm_attribution_touchpoint.touchpoint_sales_stage AS opp_touchpoint_sales_stage
    FROM 
    sfdc_bizible_attribution_touchpoint_snapshots_source

    INNER JOIN mart_crm_attribution_touchpoint ON 
    sfdc_bizible_attribution_touchpoint_snapshots_source.touchpoint_id = mart_crm_attribution_touchpoint.dim_crm_touchpoint_id

    INNER JOIN snapshot_dates ON 
        (dbt_valid_FROM <= date_day AND dbt_valid_to > date_day) OR (dbt_valid_from <= date_day AND dbt_valid_to is null)


    LEFT JOIN mart_crm_opportunity_stamped_hierarchy_hist ON
    sfdc_bizible_attribution_touchpoint_snapshots_source.opportunity_id = mart_crm_opportunity_stamped_hierarchy_hist.DIM_CRM_OPPORTUNITY_ID

    WHERE 
    snapshot_dates.fiscal_quarter_name_fy = mart_crm_opportunity_stamped_hierarchy_hist.pipeline_created_fiscal_quarter_name 

)

, wk_sales_sfdc_opportunity_snapshot_history_xf_base AS (

  SELECT
    wk_sales_sfdc_opportunity_snapshot_history_xf.opportunity_id AS dim_crm_opportunity_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf.account_id AS dim_crm_account_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf.account_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf.ultimate_parent_account_id AS dim_crm_ultimate_parent_account_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf.ultimate_parent_account_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_opportunity_category AS opportunity_category,
    wk_sales_sfdc_opportunity_snapshot_history_xf.sales_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf.order_type_stamped AS order_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf.sales_qualified_source AS sales_qualified_source_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf.stage_name,

--Account Info
    wk_sales_sfdc_opportunity_snapshot_history_xf.parent_crm_account_sales_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf.parent_crm_account_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf.parent_crm_account_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf.parent_crm_account_area,

--Dates 
    wk_sales_sfdc_opportunity_snapshot_history_xf.created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.sales_accepted_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.pipeline_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.pipeline_created_fiscal_quarter_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf.pipeline_created_fiscal_year,
    wk_sales_sfdc_opportunity_snapshot_history_xf.net_arr_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.close_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf.close_fiscal_quarter_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_date AS opportunity_snapshot_date,
    dim_date.day_of_fiscal_quarter_normalised as pipeline_created_day_of_fiscal_quarter_normalised,
    dim_date.day_of_fiscal_year_normalised as pipeline_created_day_of_fiscal_year_normalised,

--User Hierarchy
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_area,
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_business_unit,
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_sub_business_unit,
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_division,
    wk_sales_sfdc_opportunity_snapshot_history_xf.report_opportunity_user_asm,

--Flags
    CASE
        WHEN wk_sales_sfdc_opportunity_snapshot_history_xf.sales_accepted_date IS NOT NULL
          AND wk_sales_sfdc_opportunity_snapshot_history_xf.is_edu_oss = 0
          AND wk_sales_sfdc_opportunity_snapshot_history_xf.stage_name != '10-Duplicate'
            THEN TRUE
        ELSE FALSE
      END AS is_sao,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_won,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_web_portal_purchase,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_edu_oss,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_eligible_created_pipeline_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_open,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_lost,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_closed,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_renewal,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_refund,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_credit_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_eligible_sao_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_eligible_open_pipeline_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_booked_net_arr_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf.is_eligible_age_analysis_flag,

--Metrics
    wk_sales_sfdc_opportunity_snapshot_history_xf.net_arr AS opp_net_arr

  FROM wk_sales_sfdc_opportunity_snapshot_history_xf
  INNER JOIN snapshot_dates ON wk_sales_sfdc_opportunity_snapshot_history_xf.snapshot_date = snapshot_dates.date_day
  LEFT JOIN dim_date on wk_sales_sfdc_opportunity_snapshot_history_xf.pipeline_created_date = dim_date.date_day 
  WHERE snapshot_dates.fiscal_quarter_name_fy = pipeline_created_fiscal_quarter_name

), 


combined_models AS (

  SELECT
  --IDs
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_opportunity_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_account_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_ultimate_parent_account_id,
    attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id,

  --Dates
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_accepted_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_fiscal_quarter_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_fiscal_year,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_day_of_fiscal_quarter_normalised,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_day_of_fiscal_year_normalised,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.net_arr_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.close_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.close_fiscal_quarter_name,
    attribution_touchpoint_snapshot_base.bizible_touchpoint_date,
    attribution_touchpoint_snapshot_base.touchpoint_snapshot_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_snapshot_date,
  
  --Account Info
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_sales_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_area,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.account_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.ultimate_parent_account_name,

--User Hierarchy
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_area,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_business_unit,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_sub_business_unit,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_division,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_asm,

--Opportunity Dimensions
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_category,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.order_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.stage_name,

--Touchpoint Dimensions
    attribution_touchpoint_snapshot_base.bizible_touchpoint_type,
    attribution_touchpoint_snapshot_base.bizible_integrated_campaign_grouping,
    attribution_touchpoint_snapshot_base.opp_touchpoint_sales_stage,
    CASE 
      WHEN wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name = 'SDR Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'SDR Generated'
      WHEN wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name = 'Web Direct Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'Web Direct'
      ELSE attribution_touchpoint_snapshot_base.bizible_marketing_channel 
    END AS bizible_marketing_channel,
    CASE 
      WHEN wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name = 'SDR Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'SDR Generated.No Touchpoint'
      WHEN wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name = 'Web Direct Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'Web Direct.No Touchpoint'
      ELSE attribution_touchpoint_snapshot_base.bizible_marketing_channel_path 
    END AS bizible_marketing_channel_path,
    attribution_touchpoint_snapshot_base.snapshot_marketing_channel,
    attribution_touchpoint_snapshot_base.snapshot_marketing_channel_path,
    attribution_touchpoint_snapshot_base.bizible_ad_campaign_name,
    attribution_touchpoint_snapshot_base.budget_holder,
    attribution_touchpoint_snapshot_base.campaign_rep_role_name,
    attribution_touchpoint_snapshot_base.campaign_region,
    attribution_touchpoint_snapshot_base.campaign_sub_region,
    attribution_touchpoint_snapshot_base.budgeted_cost,
    attribution_touchpoint_snapshot_base.actual_cost,
    attribution_touchpoint_snapshot_base.utm_campaign,
    attribution_touchpoint_snapshot_base.utm_source,
    attribution_touchpoint_snapshot_base.utm_medium,
    attribution_touchpoint_snapshot_base.utm_content,
    attribution_touchpoint_snapshot_base.utm_budget,
    attribution_touchpoint_snapshot_base.utm_allptnr,
    attribution_touchpoint_snapshot_base.utm_partnerid,
    attribution_touchpoint_snapshot_base.integrated_budget_holder,
    attribution_touchpoint_snapshot_base.sfdc_campaign_type,
    attribution_touchpoint_snapshot_base.gtm_motion,
    attribution_touchpoint_snapshot_base.person_sales_segment,
    attribution_touchpoint_snapshot_base.touchpoint_offer_type,
    attribution_touchpoint_snapshot_base.touchpoint_offer_type_grouped,

  --Metrics
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opp_net_arr,
    attribution_touchpoint_snapshot_base.bizible_count_custom_model,
    attribution_touchpoint_snapshot_base.bizible_weight_custom_model/100 * wk_sales_sfdc_opportunity_snapshot_history_xf_base.opp_net_arr AS custom_net_arr_base,
    CASE 
      WHEN wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name IN ('SDR Generated','Web Direct Generated') AND dim_crm_touchpoint_id IS NULL 
      THEN opp_net_arr 
    ELSE custom_net_arr_base 
    END AS custom_net_arr,
    COALESCE(custom_net_arr,opp_net_arr) AS net_arr,
  --

  --Flags
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_sao,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_won,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_web_portal_purchase,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_edu_oss,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_created_pipeline_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_open,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_lost,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_closed,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_renewal,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_refund,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_credit_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_sao_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_open_pipeline_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_booked_net_arr_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_age_analysis_flag
    
  FROM wk_sales_sfdc_opportunity_snapshot_history_xf_base
  LEFT JOIN attribution_touchpoint_snapshot_base
    ON wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_opportunity_id = attribution_touchpoint_snapshot_base.dim_crm_opportunity_id
    AND wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_snapshot_date = attribution_touchpoint_snapshot_base.touchpoint_snapshot_date

), missing_net_arr_difference AS (
    SELECT 
    dim_crm_opportunity_id, 
    pipeline_created_fiscal_quarter_name, 
    opp_net_arr, 
    sum(custom_net_arr) AS sum_custom_net_arr,
    opp_net_arr-sum_custom_net_arr AS net_arr_difference,
    DIV0(net_arr_difference,opp_net_arr) AS count_difference
    FROM 
    combined_models 
    WHERE 
    custom_net_arr IS NOT NULL  
    GROUP BY 
    1,2,3

),  missing_net_arr_base AS (
    SELECT 
 --IDs
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_opportunity_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_account_id,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_ultimate_parent_account_id,
    NULL AS dim_crm_touchpoint_id,

  --Dates
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_accepted_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_fiscal_quarter_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_fiscal_year,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_day_of_fiscal_quarter_normalised,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_day_of_fiscal_year_normalised,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.net_arr_created_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.close_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.close_fiscal_quarter_name,
    NULL AS bizible_touchpoint_date,
    NULL AS touchpoint_snapshot_date,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_snapshot_date,
  
  --Account Info
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_sales_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.parent_crm_account_area,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.account_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.ultimate_parent_account_name,

--User Hierarchy
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_segment,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_geo,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_region,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_area,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_business_unit,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_sub_business_unit,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_division,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.report_opportunity_user_asm,

--Opportunity Dimensions
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opportunity_category,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.order_type,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.sales_qualified_source_name,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.stage_name,


--Touchpoint Dimensions
    NULL AS bizible_touchpoint_type,
    NULL AS bizible_integrated_campaign_grouping,
    NULL AS opp_touchpoint_sales_stage,
    'Other' AS bizible_marketing_channel,
    'Other.Removed Touchpoint' AS bizible_marketing_channel_path,
    'Other' AS snapshot_marketing_channel,
    'Other.Removed Touchpoint' AS snapshot_marketing_channel_path,
    NULL AS bizible_ad_campaign_name,
    NULL AS budget_holder,
    NULL AS campaign_rep_role_name,
    NULL AS campaign_region,
    NULL AS campaign_sub_region,
    NULL AS budgeted_cost,
    NULL AS actual_cost,
    NULL AS utm_campaign,
    NULL AS utm_source,
    NULL AS utm_medium,
    NULL AS utm_content,
    NULL AS utm_budget,
    NULL AS utm_allptnr,
    NULL AS utm_partnerid,
    NULL AS integrated_budget_holder,
    NULL AS sfdc_campaign_type,
    NULL AS gtm_motion,
    NULL AS person_sales_segment,
    NULL AS touchpoint_offer_type,
    NULL AS touchpoint_offer_type_grouped,

  --Metrics
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.opp_net_arr,
    missing_net_arr_difference.count_difference AS bizible_count_custom_model,
    NULL AS custom_net_arr_base,
    missing_net_arr_difference.net_arr_difference AS custom_net_arr,
    COALESCE(custom_net_arr,wk_sales_sfdc_opportunity_snapshot_history_xf_base.opp_net_arr) AS net_arr,
  --

  --Flags
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_sao,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_won,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_web_portal_purchase,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_edu_oss,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_created_pipeline_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_open,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_lost,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_closed,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_renewal,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_refund,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_credit_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_sao_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_open_pipeline_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_booked_net_arr_flag,
    wk_sales_sfdc_opportunity_snapshot_history_xf_base.is_eligible_age_analysis_flag

    FROM missing_net_arr_difference
    INNER JOIN
    wk_sales_sfdc_opportunity_snapshot_history_xf_base ON missing_net_arr_difference.dim_crm_opportunity_id = wk_sales_sfdc_opportunity_snapshot_history_xf_base.dim_crm_opportunity_id 
    AND missing_net_arr_difference.pipeline_created_fiscal_quarter_name = wk_sales_sfdc_opportunity_snapshot_history_xf_base.pipeline_created_fiscal_quarter_name

), final AS (
    SELECT 
    *
    FROM 
    combined_models
    UNION ALL
    SELECT
    *
    FROM
    missing_net_arr_base
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-04-11",
    updated_date="2024-02-22",
  ) }}