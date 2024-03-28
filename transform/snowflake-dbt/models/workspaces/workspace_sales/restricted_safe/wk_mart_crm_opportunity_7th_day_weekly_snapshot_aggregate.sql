{{ simple_cte([
    ('fct_crm_opportunity','wk_fct_crm_opportunity_7th_day_weekly_snapshot_aggregate'),
    ('dim_crm_account','dim_crm_account_daily_snapshot'),
    ('dim_crm_user', 'wk_prep_crm_user_daily_snapshot'),
    ('dim_date', 'dim_date'),
    ('dim_crm_user_hierarchy','wk_dim_crm_user_hierarchy')
]) }},


final AS (


  SELECT
    fct_crm_opportunity.dim_sales_qualified_source_id,
    fct_crm_opportunity.dim_order_type_id,
    fct_crm_opportunity.dim_order_type_live_id,
    fct_crm_opportunity.dim_crm_user_hierarchy_sk,
    fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
    fct_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk,

    -- crm owner/sales rep live fields
    dim_crm_user_hierarchy.crm_user_sales_segment,
    dim_crm_user_hierarchy.crm_user_geo,
    dim_crm_user_hierarchy.crm_user_region,
    dim_crm_user_hierarchy.crm_user_area,
    dim_crm_user_hierarchy.crm_user_business_unit,
    dim_crm_user_hierarchy.crm_user_sales_segment_grouped,
    dim_crm_user_hierarchy.crm_user_sales_segment_region_grouped,

    dim_crm_user_hierarchy.crm_user_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5,

    fct_crm_opportunity.sales_qualified_source_name,
    fct_crm_opportunity.sales_qualified_source_grouped,
    fct_crm_opportunity.order_type,
    fct_crm_opportunity.order_type_grouped,
    fct_crm_opportunity.order_type_live,
    fct_crm_opportunity.stage_name,
    fct_crm_opportunity.deal_path_name,
    fct_crm_opportunity.sales_type,

    fct_crm_opportunity.snapshot_date,
    fct_crm_opportunity.snapshot_month,
    fct_crm_opportunity.snapshot_fiscal_year,
    fct_crm_opportunity.snapshot_fiscal_quarter_name,
    fct_crm_opportunity.snapshot_fiscal_quarter_date,
    fct_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised,
    fct_crm_opportunity.snapshot_day_of_fiscal_year_normalised,

    -- Dates
    DAYNAME(dim_date.current_date_actual) AS current_day_name,  --need to add this field to date_details
    dim_date.current_date_actual,
    dim_date.current_fiscal_year,
    dim_date.current_first_day_of_fiscal_year,
    dim_date.current_fiscal_quarter_name_fy,
    dim_date.current_first_day_of_month,
    dim_date.current_first_day_of_fiscal_quarter,
    dim_date.current_day_of_month,
    dim_date.current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year,
    CASE WHEN current_day_name = 'Sun' THEN dim_date.current_date_actual
      ELSE DATEADD('day', -1, DATE_TRUNC('week', dim_date.current_date_actual)) END     AS current_first_day_of_week,--n--need to add this field to date_details
    FLOOR((DATEDIFF(day, dim_date.current_first_day_of_fiscal_quarter, dim_date.current_date_actual) / 7))                   
                                                                    AS current_week_of_fiscal_quarter_normalised,
    DATEDIFF('week',dim_date.current_first_day_of_fiscal_quarter, dim_date.current_date_actual) + 1     
                                                                    AS current_week_of_fiscal_quarter,
    dim_date.date_day                                               AS snapshot_day,
    dim_date.day_name                                               AS snapshot_day_name, 
    dim_date.day_of_week                                            AS snapshot_day_of_week,
    dim_date.first_day_of_week                                      AS snapshot_first_day_of_week,
    dim_date.week_of_year                                           AS snapshot_week_of_year,
    dim_date.day_of_month                                           AS snapshot_day_of_month,
    dim_date.day_of_quarter                                         AS snapshot_day_of_quarter,
    dim_date.day_of_year                                            AS snapshot_day_of_year,
    dim_date.fiscal_quarter                                         AS snapshot_fiscal_quarter,
    dim_date.day_of_fiscal_quarter                                  AS snapshot_day_of_fiscal_quarter,
    dim_date.day_of_fiscal_year                                     AS snapshot_day_of_fiscal_year,
    dim_date.month_name                                             AS snapshot_month_name,
    dim_date.first_day_of_month                                     AS snapshot_first_day_of_month,
    dim_date.last_day_of_month                                      AS snapshot_last_day_of_month,
    dim_date.first_day_of_year                                      AS snapshot_first_day_of_year,
    dim_date.last_day_of_year                                       AS snapshot_last_day_of_year,
    dim_date.first_day_of_quarter                                   AS snapshot_first_day_of_quarter,
    dim_date.last_day_of_quarter                                    AS snapshot_last_day_of_quarter,
    dim_date.first_day_of_fiscal_quarter                            AS snapshot_first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter                             AS snapshot_last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year                               AS snapshot_first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year                                AS snapshot_last_day_of_fiscal_year,
    dim_date.week_of_fiscal_year                                    AS snapshot_week_of_fiscal_year,
    dim_date.month_of_fiscal_year                                   AS snapshot_month_of_fiscal_year,
    dim_date.last_day_of_week                                       AS snapshot_last_day_of_week,
    dim_date.quarter_name                                           AS snapshot_quarter_name,
    dim_date.fiscal_quarter_name_fy                                 AS snapshot_fiscal_quarter_name_fy,
    dim_date.fiscal_quarter_number_absolute                         AS snapshot_fiscal_quarter_number_absolute,
    dim_date.fiscal_month_name                                      AS snapshot_fiscal_month_name,
    dim_date.fiscal_month_name_fy                                   AS snapshot_fiscal_month_name_fy,
    dim_date.holiday_desc                                           AS snapshot_holiday_desc,
    dim_date.is_holiday                                             AS snapshot_is_holiday,
    dim_date.last_month_of_fiscal_quarter                           AS snapshot_last_month_of_fiscal_quarter,
    dim_date.is_first_day_of_last_month_of_fiscal_quarter           AS snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year                              AS snapshot_last_month_of_fiscal_year,
    dim_date.is_first_day_of_last_month_of_fiscal_year              AS snapshot_is_first_day_of_last_month_of_fiscal_year,
    dim_date.days_in_month_count                                    AS snapshot_days_in_month_count,
    dim_date.week_of_month_normalised                               AS snapshot_week_of_month_normalised,
    dim_date.week_of_fiscal_quarter_normalised                      AS snapshot_week_of_fiscal_quarter_normalised,
    dim_date.is_first_day_of_fiscal_quarter_week                    AS snapshot_is_first_day_of_fiscal_quarter_week,
    dim_date.days_until_last_day_of_month                           AS snapshot_days_until_last_day_of_month,
    DATEDIFF('week',dim_date.first_day_of_fiscal_quarter, snapshot_date) + 1     
                                                                    AS snapshot_week_of_fiscal_quarter,
    
    -- Total actuals quarter

    
    fct_crm_opportunity.created_arr_quarter_total,
    fct_crm_opportunity.closed_won_opps_quarter_total,
    fct_crm_opportunity.closed_opps_quarter_total,
    fct_crm_opportunity.closed_net_arr_quarter_total,
    fct_crm_opportunity.cycle_time_in_days_quarter_total,
    fct_crm_opportunity.booked_deal_count_quarter_total,
    fct_crm_opportunity.booked_net_arr_quarter_total,
    fct_crm_opportunity.created_deals_quarter_total,
    fct_crm_opportunity.open_1plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_3plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_4plus_net_arr_in_snapshot_quarter,

    --additive fields
    fct_crm_opportunity.created_arr_in_snapshot_quarter,
    fct_crm_opportunity.closed_won_opps_in_snapshot_quarter,
    fct_crm_opportunity.closed_opps_in_snapshot_quarter,
    fct_crm_opportunity.closed_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.booked_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.created_deals_in_snapshot_quarter,
    fct_crm_opportunity.cycle_time_in_days_in_snapshot_quarter,
    fct_crm_opportunity.booked_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.created_arr,
    fct_crm_opportunity.closed_won_opps,
    fct_crm_opportunity.closed_opps,
    fct_crm_opportunity.closed_net_arr,
    fct_crm_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    fct_crm_opportunity.calculated_from_ratio_net_arr,
    fct_crm_opportunity.net_arr,
    fct_crm_opportunity.raw_net_arr,
    fct_crm_opportunity.created_and_won_same_quarter_net_arr_combined,
    fct_crm_opportunity.new_logo_count,
    fct_crm_opportunity.amount,
    fct_crm_opportunity.recurring_amount,
    fct_crm_opportunity.true_up_amount,
    fct_crm_opportunity.proserv_amount,
    fct_crm_opportunity.other_non_recurring_amount,
    fct_crm_opportunity.arr_basis,
    fct_crm_opportunity.arr,
    fct_crm_opportunity.count_crm_attribution_touchpoints,
    fct_crm_opportunity.weighted_linear_iacv,
    fct_crm_opportunity.count_campaigns,
    fct_crm_opportunity.probability,
    fct_crm_opportunity.days_in_sao,
    fct_crm_opportunity.open_1plus_deal_count,
    fct_crm_opportunity.open_3plus_deal_count,
    fct_crm_opportunity.open_4plus_deal_count,
    fct_crm_opportunity.booked_deal_count,
    fct_crm_opportunity.churned_contraction_deal_count,
    fct_crm_opportunity.open_1plus_net_arr,
    fct_crm_opportunity.open_3plus_net_arr,
    fct_crm_opportunity.open_4plus_net_arr,
    fct_crm_opportunity.booked_net_arr,
    fct_crm_opportunity.churned_contraction_net_arr,
    fct_crm_opportunity.calculated_deal_count,
    fct_crm_opportunity.booked_churned_contraction_deal_count,
    fct_crm_opportunity.booked_churned_contraction_net_arr,
    fct_crm_opportunity.renewal_amount,
    fct_crm_opportunity.total_contract_value,
    fct_crm_opportunity.days_in_stage,
    fct_crm_opportunity.calculated_age_in_days,
    fct_crm_opportunity.days_since_last_activity,
    fct_crm_opportunity.pre_military_invasion_arr,
    fct_crm_opportunity.won_arr_basis_for_clari,
    fct_crm_opportunity.arr_basis_for_clari,
    fct_crm_opportunity.forecasted_churn_for_clari,
    fct_crm_opportunity.override_arr_basis_clari,
    fct_crm_opportunity.vsa_start_date_net_arr,
    fct_crm_opportunity.cycle_time_in_days_combined,
    'aggregate' AS source,
    IFF(dim_date.current_first_day_of_fiscal_quarter = snapshot_first_day_of_fiscal_quarter, TRUE, FALSE) AS is_current_snapshot_quarter,
    IFF(current_first_day_of_week = dim_date.first_day_of_week, TRUE, FALSE) AS is_current_snapshot_week
  FROM fct_crm_opportunity
  LEFT JOIN dim_date 
    ON fct_crm_opportunity.snapshot_date = dim_date.date_actual
  LEFT JOIN dim_crm_user_hierarchy
    ON dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk = fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk


)

SELECT * 
FROM final