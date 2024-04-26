WITH granular AS (

  SELECT * 
  FROM {{ ref('wk_mart_crm_opportunity_7th_day_weekly_snapshot')}}

),

aggregate AS (

  SELECT * 
  FROM {{ ref('wk_mart_crm_opportunity_7th_day_weekly_snapshot_aggregate')}}

),

targets_actuals AS (

  SELECT * 
  FROM {{ ref('wk_mart_targets_actuals_7th_day_weekly_snapshot')}}

),

unioned AS (


  SELECT 
    granular.*,
    NULL AS total_quarter_target,
    NULL AS coverage_booked_net_arr,
    NULL AS coverage_open_1plus_net_arr,
    NULL AS coverage_open_3plus_net_arr,
    NULL AS coverage_open_4plus_net_arr,
    NULL AS total_booked_net_arr
  FROM granular
  WHERE is_current_snapshot_quarter

  UNION 

  SELECT 
    NULL AS crm_opportunity_snapshot_id,
    NULL AS dim_crm_opportunity_id,
    NULL AS dim_crm_user_id,
    NULL AS snapshot_id,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_order_type_live_id,
    
    dim_crm_current_account_set_hierarchy_sk,
    crm_current_account_set_sales_segment,
    crm_current_account_set_geo,
    crm_current_account_set_region,
    crm_current_account_set_area,
    crm_current_account_set_business_unit,
    crm_current_account_set_role_name,
    crm_current_account_set_role_level_1,
    crm_current_account_set_role_level_2,
    crm_current_account_set_role_level_3,
    crm_current_account_set_role_level_4,
    crm_current_account_set_role_level_5,

    NULL AS merged_crm_opportunity_id,
    NULL AS dim_crm_account_id,
    NULL AS dim_crm_person_id,
    NULL AS sfdc_contact_id,
    NULL AS record_type_id,
    NULL AS opportunity_name,
    NULL AS report_user_segment_geo_region_area_sqs_ot,
    NULL AS opp_owner_name,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_live,
    order_type_grouped,
    stage_name,
    deal_path_name,
    sales_type,
    snapshot_date,
    snapshot_month,
    snapshot_fiscal_year,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    snapshot_day_of_fiscal_quarter_normalised,
    snapshot_day_of_fiscal_year_normalised,
    NULL AS days_in_0_pending_acceptance,
    NULL AS days_in_1_discovery,
    NULL AS days_in_2_scoping,
    NULL AS days_in_3_technical_evaluation,
    NULL AS days_in_4_proposal,
    NULL AS days_in_5_negotiating,
    NULL AS ssp_id,
    NULL AS ga_client_id,
    NULL AS is_closed,
    NULL AS is_won,
    NULL AS is_refund,
    NULL AS is_downgrade,
    NULL AS is_swing_deal,
    NULL AS is_edu_oss,
    NULL AS is_web_portal_purchase,
    NULL AS fpa_master_bookings_flag,
    NULL AS is_sao,
    NULL AS is_sdr_sao,
    NULL AS is_net_arr_closed_deal,
    NULL AS is_new_logo_first_order,
    NULL AS is_net_arr_pipeline_created_combined,
    NULL AS is_win_rate_calc,
    NULL AS is_closed_won,
    NULL AS is_stage_1_plus,
    NULL AS is_stage_3_plus,
    NULL AS is_stage_4_plus,
    NULL AS is_lost,
    NULL AS is_open,
    NULL AS is_active,
    NULL AS is_credit,
    NULL AS is_renewal,
    NULL AS is_deleted,
    NULL AS is_excluded_from_pipeline_created_combined,
    NULL AS created_in_snapshot_quarter_deal_count,
    NULL AS is_duplicate,
    NULL AS is_contract_reset,
    NULL AS is_comp_new_logo_override,
    NULL AS is_eligible_open_pipeline_combined,
    NULL AS is_eligible_age_analysis_combined,
    NULL AS is_eligible_churn_contraction,
    NULL AS is_booked_net_arr,
    NULL AS is_abm_tier_sao,
    NULL AS is_abm_tier_closed_won,
    NULL AS primary_solution_architect,
    NULL AS product_details,
    NULL AS product_category,
    NULL AS intended_product_tier,
    NULL AS products_purchased,
    NULL AS growth_type,
    NULL AS opportunity_deal_size,
    NULL AS closed_buckets,
    NULL AS calculated_deal_size,
    NULL AS deal_size,
    NULL AS lead_source,
    NULL AS dr_partner_deal_type,
    NULL AS dr_partner_engagement,
    NULL AS partner_account,
    NULL AS dr_status,
    NULL AS dr_deal_id,
    NULL AS dr_primary_registration,
    NULL AS distributor,
    NULL AS influence_partner,
    NULL AS fulfillment_partner,
    NULL AS platform_partner,
    NULL AS partner_track,
    NULL AS resale_partner_track,
    NULL AS is_public_sector_opp,
    NULL AS is_registration_from_portal,
    NULL AS calculated_discount,
    NULL AS partner_discount,
    NULL AS partner_discount_calc,
    NULL AS comp_channel_neutral,
    NULL AS dim_parent_crm_account_id,
    NULL AS crm_account_name,
    NULL AS parent_crm_account_name,
    NULL AS parent_crm_account_business_unit,
    NULL AS parent_crm_account_sales_segment,
    NULL AS parent_crm_account_geo,
    NULL AS parent_crm_account_region,
    NULL AS parent_crm_account_area,
    NULL AS parent_crm_account_territory,
    NULL AS parent_crm_account_role_type,
    NULL AS parent_crm_account_max_family_employee,
    NULL AS parent_crm_account_upa_country,
    NULL AS parent_crm_account_upa_state,
    NULL AS parent_crm_account_upa_city,
    NULL AS parent_crm_account_upa_street,
    NULL AS parent_crm_account_upa_postal_code,
    NULL AS crm_account_employee_count,
    NULL AS crm_account_gtm_strategy,
    NULL AS crm_account_focus_account,
    NULL AS crm_account_zi_technologies,
    NULL AS is_jihu_account,
    NULL AS sao_crm_opp_owner_sales_segment_stamped,
    NULL AS sao_crm_opp_owner_sales_segment_stamped_grouped,
    NULL AS sao_crm_opp_owner_geo_stamped,
    NULL AS sao_crm_opp_owner_region_stamped,
    NULL AS sao_crm_opp_owner_area_stamped,
    NULL AS sao_crm_opp_owner_segment_region_stamped_grouped,
    NULL AS sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
    NULL AS crm_opp_owner_stamped_name,
    NULL AS crm_account_owner_stamped_name,
    NULL AS crm_opp_owner_sales_segment_stamped,
    NULL AS crm_opp_owner_sales_segment_stamped_grouped,
    NULL AS crm_opp_owner_geo_stamped,
    NULL AS crm_opp_owner_region_stamped,
    NULL AS crm_opp_owner_area_stamped,
    NULL AS crm_opp_owner_business_unit_stamped,
    NULL AS crm_opp_owner_sales_segment_region_stamped_grouped,
    NULL AS crm_opp_owner_sales_segment_geo_region_area_stamped,
    NULL AS crm_opp_owner_user_role_type_stamped,
    NULL AS crm_user_sales_segment,
    NULL AS crm_user_geo,
    NULL AS crm_user_region,
    NULL AS crm_user_area,
    NULL AS crm_user_business_unit,
    NULL AS crm_user_sales_segment_grouped,
    NULL AS crm_user_sales_segment_region_grouped,
    NULL AS crm_user_role_name,
    NULL AS crm_user_role_level_1,
    NULL AS crm_user_role_level_2,
    NULL AS crm_user_role_level_3,
    NULL AS crm_user_role_level_4,
    NULL AS crm_user_role_level_5,
    NULL AS crm_account_user_sales_segment,
    NULL AS crm_account_user_sales_segment_grouped,
    NULL AS crm_account_user_geo,
    NULL AS crm_account_user_region,
    NULL AS crm_account_user_area,
    NULL AS crm_account_user_sales_segment_region_grouped,
    NULL AS partner_account_name,
    NULL AS partner_gitlab_program,
    NULL AS fulfillment_partner_name,
    current_day_name,
    current_date_actual,
    current_fiscal_year,
    current_first_day_of_fiscal_year,
    current_fiscal_quarter_name_fy,
    current_first_day_of_month,
    current_first_day_of_fiscal_quarter,
    current_day_of_month,
    current_day_of_fiscal_quarter,
    current_day_of_fiscal_year,
    current_first_day_of_week,
    current_week_of_fiscal_quarter_normalised,
    current_week_of_fiscal_quarter,
    NULL AS created_date,
    NULL AS created_month,
    NULL AS created_fiscal_quarter_date,
    NULL AS created_fiscal_quarter_name,
    NULL AS created_fiscal_year,
    NULL AS sales_accepted_date,
    NULL AS sales_accepted_month,
    NULL AS sales_accepted_fiscal_quarter_date,
    NULL AS sales_accepted_fiscal_quarter_name,
    NULL AS sales_accepted_fiscal_year,
    NULL AS close_date,
    NULL AS close_month,
    NULL AS close_fiscal_quarter_date,
    NULL AS close_fiscal_quarter_name,
    NULL AS close_fiscal_year,
    NULL AS stage_0_pending_acceptance_date,
    NULL AS stage_0_pending_acceptance_month,
    NULL AS stage_0_pending_acceptance_fiscal_quarter_date,
    NULL AS stage_0_pending_acceptance_fiscal_quarter_name,
    NULL AS stage_0_pending_acceptance_fiscal_year,
    NULL AS stage_1_discovery_date,
    NULL AS stage_1_discovery_month,
    NULL AS stage_1_discovery_fiscal_quarter_date,
    NULL AS stage_1_discovery_fiscal_quarter_name,
    NULL AS stage_1_discovery_fiscal_year,
    NULL AS stage_2_scoping_date,
    NULL AS stage_2_scoping_month,
    NULL AS stage_2_scoping_fiscal_quarter_date,
    NULL AS stage_2_scoping_fiscal_quarter_name,
    NULL AS stage_2_scoping_fiscal_year,
    NULL AS stage_3_technical_evaluation_date,
    NULL AS stage_3_technical_evaluation_month,
    NULL AS stage_3_technical_evaluation_fiscal_quarter_date,
    NULL AS stage_3_technical_evaluation_fiscal_quarter_name,
    NULL AS stage_3_technical_evaluation_fiscal_year,
    NULL AS stage_4_proposal_date,
    NULL AS stage_4_proposal_month,
    NULL AS stage_4_proposal_fiscal_quarter_date,
    NULL AS stage_4_proposal_fiscal_quarter_name,
    NULL AS stage_4_proposal_fiscal_year,
    NULL AS stage_5_negotiating_date,
    NULL AS stage_5_negotiating_month,
    NULL AS stage_5_negotiating_fiscal_quarter_date,
    NULL AS stage_5_negotiating_fiscal_quarter_name,
    NULL AS stage_5_negotiating_fiscal_year,
    NULL AS stage_6_awaiting_signature_date,
    NULL AS stage_6_awaiting_signature_date_date,
    NULL AS stage_6_awaiting_signature_date_month,
    NULL AS stage_6_awaiting_signature_date_fiscal_quarter_date,
    NULL AS stage_6_awaiting_signature_date_fiscal_quarter_name,
    NULL AS stage_6_awaiting_signature_date_fiscal_year,
    NULL AS stage_6_closed_won_date,
    NULL AS stage_6_closed_won_month,
    NULL AS stage_6_closed_won_fiscal_quarter_date,
    NULL AS stage_6_closed_won_fiscal_quarter_name,
    NULL AS stage_6_closed_won_fiscal_year,
    NULL AS stage_6_closed_lost_date,
    NULL AS stage_6_closed_lost_month,
    NULL AS stage_6_closed_lost_fiscal_quarter_date,
    NULL AS stage_6_closed_lost_fiscal_quarter_name,
    NULL AS stage_6_closed_lost_fiscal_year,
    NULL AS subscription_start_date,
    NULL AS subscription_start_month,
    NULL AS subscription_start_fiscal_quarter_date,
    NULL AS subscription_start_fiscal_quarter_name,
    NULL AS subscription_start_fiscal_year,
    NULL AS subscription_end_date,
    NULL AS subscription_end_month,
    NULL AS subscription_end_fiscal_quarter_date,
    NULL AS subscription_end_fiscal_quarter_name,
    NULL AS subscription_end_fiscal_year,
    NULL AS sales_qualified_date,
    NULL AS sales_qualified_month,
    NULL AS sales_qualified_fiscal_quarter_date,
    NULL AS sales_qualified_fiscal_quarter_name,
    NULL AS sales_qualified_fiscal_year,
    NULL AS last_activity_date,
    NULL AS last_activity_month,
    NULL AS last_activity_fiscal_quarter_date,
    NULL AS last_activity_fiscal_quarter_name,
    NULL AS last_activity_fiscal_year,
    NULL AS sales_last_activity_date,
    NULL AS sales_last_activity_month,
    NULL AS sales_last_activity_fiscal_quarter_date,
    NULL AS sales_last_activity_fiscal_quarter_name,
    NULL AS sales_last_activity_fiscal_year,
    NULL AS technical_evaluation_date,
    NULL AS technical_evaluation_month,
    NULL AS technical_evaluation_fiscal_quarter_date,
    NULL AS technical_evaluation_fiscal_quarter_name,
    NULL AS technical_evaluation_fiscal_year,
    NULL AS arr_created_date,
    NULL AS arr_created_month,
    NULL AS arr_created_fiscal_quarter_date,
    NULL AS arr_created_fiscal_quarter_name,
    NULL AS arr_created_fiscal_year,
    NULL AS pipeline_created_date,
    NULL AS pipeline_created_month,
    NULL AS pipeline_created_fiscal_quarter_date,
    NULL AS pipeline_created_fiscal_quarter_name,
    NULL AS pipeline_created_fiscal_year,
    NULL AS net_arr_created_date,
    NULL AS net_arr_created_month,
    NULL AS net_arr_created_fiscal_quarter_date,
    NULL AS net_arr_created_fiscal_quarter_name,
    NULL AS net_arr_created_fiscal_year,
    snapshot_day,
    snapshot_day_name,
    snapshot_day_of_week,
    snapshot_first_day_of_week,
    snapshot_week_of_year,
    snapshot_day_of_month,
    snapshot_day_of_quarter,
    snapshot_day_of_year,
    snapshot_fiscal_quarter,
    snapshot_day_of_fiscal_quarter,
    snapshot_day_of_fiscal_year,
    snapshot_month_name,
    snapshot_first_day_of_month,
    snapshot_last_day_of_month,
    snapshot_first_day_of_year,
    snapshot_last_day_of_year,
    snapshot_first_day_of_quarter,
    snapshot_last_day_of_quarter,
    snapshot_first_day_of_fiscal_quarter,
    snapshot_last_day_of_fiscal_quarter,
    snapshot_first_day_of_fiscal_year,
    snapshot_last_day_of_fiscal_year,
    snapshot_week_of_fiscal_year,
    snapshot_month_of_fiscal_year,
    snapshot_last_day_of_week,
    snapshot_quarter_name,
    snapshot_fiscal_quarter_name_fy,
    snapshot_fiscal_quarter_number_absolute,
    snapshot_fiscal_month_name,
    snapshot_fiscal_month_name_fy,
    snapshot_holiday_desc,
    snapshot_is_holiday,
    snapshot_last_month_of_fiscal_quarter,
    snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    snapshot_last_month_of_fiscal_year,
    snapshot_is_first_day_of_last_month_of_fiscal_year,
    snapshot_days_in_month_count,
    snapshot_week_of_month_normalised,
    snapshot_week_of_fiscal_quarter_normalised,
    snapshot_is_first_day_of_fiscal_quarter_week,
    snapshot_days_until_last_day_of_month,
    snapshot_week_of_fiscal_quarter,
    positive_booked_deal_count_in_snapshot_quarter,
    positive_booked_net_arr_in_snapshot_quarter,
    positive_open_deal_count_in_snapshot_quarter,
    positive_open_net_arr_in_snapshot_quarter,
    closed_deals_in_snapshot_quarter,
    closed_net_arr_in_snapshot_quarter,
    open_1plus_net_arr_in_snapshot_quarter,
    open_3plus_net_arr_in_snapshot_quarter,
    open_4plus_net_arr_in_snapshot_quarter,
    open_1plus_deal_count_in_snapshot_quarter,
    open_3plus_deal_count_in_snapshot_quarter,
    open_4plus_deal_count_in_snapshot_quarter,
    created_arr_in_snapshot_quarter,
    closed_won_opps_in_snapshot_quarter,
    closed_opps_in_snapshot_quarter,
    booked_net_arr_in_snapshot_quarter,
    created_deals_in_snapshot_quarter,
    cycle_time_in_days_in_snapshot_quarter,
    booked_deal_count_in_snapshot_quarter,
    created_arr,
    closed_won_opps,
    closed_opps,
    closed_net_arr,
    segment_order_type_iacv_to_net_arr_ratio,
    calculated_from_ratio_net_arr,
    net_arr,
    raw_net_arr,
    created_and_won_same_quarter_net_arr_combined,
    new_logo_count,
    amount,
    recurring_amount,
    true_up_amount,
    proserv_amount,
    other_non_recurring_amount,
    arr_basis,
    arr,
    count_crm_attribution_touchpoints,
    weighted_linear_iacv,
    count_campaigns,
    probability,
    days_in_sao,
    open_1plus_deal_count,
    open_3plus_deal_count,
    open_4plus_deal_count,
    booked_deal_count,
    churned_contraction_deal_count,
    open_1plus_net_arr,
    open_3plus_net_arr,
    open_4plus_net_arr,
    booked_net_arr,
    churned_contraction_net_arr,
    calculated_deal_count,
    booked_churned_contraction_deal_count,
    booked_churned_contraction_net_arr,
    renewal_amount,
    total_contract_value,
    days_in_stage,
    calculated_age_in_days,
    days_since_last_activity,
    pre_military_invasion_arr,
    won_arr_basis_for_clari,
    arr_basis_for_clari,
    forecasted_churn_for_clari,
    override_arr_basis_clari,
    vsa_start_date_net_arr,
    NULL AS day_of_week,
    NULL AS first_day_of_week,
    NULL AS date_id,
    NULL AS fiscal_month_name_fy,
    NULL AS fiscal_quarter_name_fy,
    NULL AS first_day_of_fiscal_quarter,
    NULL AS first_day_of_fiscal_year,
    NULL AS last_day_of_week,
    NULL AS last_day_of_month,
    NULL AS last_day_of_fiscal_quarter,
    NULL AS last_day_of_fiscal_year,
    NULL AS is_current_snapshot_quarter,
    NULL AS is_current_snapshot_week,
    source,
    NULL AS total_quarter_target,
    NULL AS coverage_booked_net_arr,
    NULL AS coverage_open_1plus_net_arr,
    NULL AS coverage_open_3plus_net_arr,
    NULL AS coverage_open_4plus_net_arr,
    NULL AS total_booked_net_arr
  FROM aggregate
  WHERE NOT is_current_snapshot_quarter
    AND snapshot_date >= DATEADD(QUARTER, -6, current_first_day_of_fiscal_quarter) -- include only the last 6 quarters 


  UNION 

  SELECT      
    NULL AS crm_opportunity_snapshot_id,
    NULL AS dim_crm_opportunity_id,
    NULL AS dim_crm_user_id,
    NULL AS snapshot_id,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    NULL AS dim_order_type_live_id,
    
    dim_crm_user_hierarchy_sk AS dim_crm_current_account_set_hierarchy_sk,
    crm_current_account_set_sales_segment,
    crm_current_account_set_geo,
    crm_current_account_set_region,
    crm_current_account_set_area,
    crm_current_account_set_business_unit,
    crm_current_account_set_role_name,
    crm_current_account_set_role_level_1,
    crm_current_account_set_role_level_2,
    crm_current_account_set_role_level_3,
    crm_current_account_set_role_level_4,
    crm_current_account_set_role_level_5,

    NULL AS merged_crm_opportunity_id,
    NULL AS dim_crm_account_id,
    NULL AS dim_crm_person_id,
    NULL AS sfdc_contact_id,
    NULL AS record_type_id,
    NULL AS opportunity_name,
    NULL AS report_user_segment_geo_region_area_sqs_ot,
    NULL AS opp_owner_name,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    NULL AS order_type_live,
    order_type_grouped,
    NULL AS stage_name,
    NULL AS deal_path_name,
    NULL AS sales_type,
    date_actual AS snapshot_date,
    NULL AS snapshot_month,
    NULL AS snapshot_fiscal_year,
    fiscal_quarter_name AS snapshot_fiscal_quarter_name,
    fiscal_quarter_date AS snapshot_fiscal_quarter_date,
    NULL AS snapshot_day_of_fiscal_quarter_normalised,
    NULL AS snapshot_day_of_fiscal_year_normalised,
    NULL AS days_in_0_pending_acceptance,
    NULL AS days_in_1_discovery,
    NULL AS days_in_2_scoping,
    NULL AS days_in_3_technical_evaluation,
    NULL AS days_in_4_proposal,
    NULL AS days_in_5_negotiating,
    NULL AS ssp_id,
    NULL AS ga_client_id,
    NULL AS is_closed,
    NULL AS is_won,
    NULL AS is_refund,
    NULL AS is_downgrade,
    NULL AS is_swing_deal,
    NULL AS is_edu_oss,
    NULL AS is_web_portal_purchase,
    NULL AS fpa_master_bookings_flag,
    NULL AS is_sao,
    NULL AS is_sdr_sao,
    NULL AS is_net_arr_closed_deal,
    NULL AS is_new_logo_first_order,
    NULL AS is_net_arr_pipeline_created_combined,
    NULL AS is_win_rate_calc,
    NULL AS is_closed_won,
    NULL AS is_stage_1_plus,
    NULL AS is_stage_3_plus,
    NULL AS is_stage_4_plus,
    NULL AS is_lost,
    NULL AS is_open,
    NULL AS is_active,
    NULL AS is_credit,
    NULL AS is_renewal,
    NULL AS is_deleted,
    NULL AS is_excluded_from_pipeline_created_combined,
    NULL AS created_in_snapshot_quarter_deal_count,
    NULL AS is_duplicate,
    NULL AS is_contract_reset,
    NULL AS is_comp_new_logo_override,
    NULL AS is_eligible_open_pipeline_combined,
    NULL AS is_eligible_age_analysis_combined,
    NULL AS is_eligible_churn_contraction,
    NULL AS is_booked_net_arr,
    NULL AS is_abm_tier_sao,
    NULL AS is_abm_tier_closed_won,
    NULL AS primary_solution_architect,
    NULL AS product_details,
    NULL AS product_category,
    NULL AS intended_product_tier,
    NULL AS products_purchased,
    NULL AS growth_type,
    NULL AS opportunity_deal_size,
    NULL AS closed_buckets,
    NULL AS calculated_deal_size,
    NULL AS deal_size,
    NULL AS lead_source,
    NULL AS dr_partner_deal_type,
    NULL AS dr_partner_engagement,
    NULL AS partner_account,
    NULL AS dr_status,
    NULL AS dr_deal_id,
    NULL AS dr_primary_registration,
    NULL AS distributor,
    NULL AS influence_partner,
    NULL AS fulfillment_partner,
    NULL AS platform_partner,
    NULL AS partner_track,
    NULL AS resale_partner_track,
    NULL AS is_public_sector_opp,
    NULL AS is_registration_from_portal,
    NULL AS calculated_discount,
    NULL AS partner_discount,
    NULL AS partner_discount_calc,
    NULL AS comp_channel_neutral,
    NULL AS dim_parent_crm_account_id,
    NULL AS crm_account_name,
    NULL AS parent_crm_account_name,
    NULL AS parent_crm_account_business_unit,
    NULL AS parent_crm_account_sales_segment,
    NULL AS parent_crm_account_geo,
    NULL AS parent_crm_account_region,
    NULL AS parent_crm_account_area,
    NULL AS parent_crm_account_territory,
    NULL AS parent_crm_account_role_type,
    NULL AS parent_crm_account_max_family_employee,
    NULL AS parent_crm_account_upa_country,
    NULL AS parent_crm_account_upa_state,
    NULL AS parent_crm_account_upa_city,
    NULL AS parent_crm_account_upa_street,
    NULL AS parent_crm_account_upa_postal_code,
    NULL AS crm_account_employee_count,
    NULL AS crm_account_gtm_strategy,
    NULL AS crm_account_focus_account,
    NULL AS crm_account_zi_technologies,
    NULL AS is_jihu_account,
    NULL AS sao_crm_opp_owner_sales_segment_stamped,
    NULL AS sao_crm_opp_owner_sales_segment_stamped_grouped,
    NULL AS sao_crm_opp_owner_geo_stamped,
    NULL AS sao_crm_opp_owner_region_stamped,
    NULL AS sao_crm_opp_owner_area_stamped,
    NULL AS sao_crm_opp_owner_segment_region_stamped_grouped,
    NULL AS sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
    NULL AS crm_opp_owner_stamped_name,
    NULL AS crm_account_owner_stamped_name,
    NULL AS crm_opp_owner_sales_segment_stamped,
    NULL AS crm_opp_owner_sales_segment_stamped_grouped,
    NULL AS crm_opp_owner_geo_stamped,
    NULL AS crm_opp_owner_region_stamped,
    NULL AS crm_opp_owner_area_stamped,
    NULL AS crm_opp_owner_business_unit_stamped,
    NULL AS crm_opp_owner_sales_segment_region_stamped_grouped,
    NULL AS crm_opp_owner_sales_segment_geo_region_area_stamped,
    NULL AS crm_opp_owner_user_role_type_stamped,
    NULL AS crm_user_sales_segment,
    NULL AS crm_user_geo,
    NULL AS crm_user_region,
    NULL AS crm_user_area,
    NULL AS crm_user_business_unit,
    NULL AS crm_user_sales_segment_grouped,
    NULL AS crm_user_sales_segment_region_grouped,
    NULL AS crm_user_role_name,
    NULL AS crm_user_role_level_1,
    NULL AS crm_user_role_level_2,
    NULL AS crm_user_role_level_3,
    NULL AS crm_user_role_level_4,
    NULL AS crm_user_role_level_5,
    NULL AS crm_account_user_sales_segment,
    NULL AS crm_account_user_sales_segment_grouped,
    NULL AS crm_account_user_geo,
    NULL AS crm_account_user_region,
    NULL AS crm_account_user_area,
    NULL AS crm_account_user_sales_segment_region_grouped,
    NULL AS partner_account_name,
    NULL AS partner_gitlab_program,
    NULL AS fulfillment_partner_name,
    current_day_name,
    current_date_actual,
    current_fiscal_year,
    current_first_day_of_fiscal_year,
    current_fiscal_quarter_name_fy,
    current_first_day_of_month,
    current_first_day_of_fiscal_quarter,
    current_day_of_month,
    current_day_of_fiscal_quarter,
    current_day_of_fiscal_year,
    current_first_day_of_week,
    current_week_of_fiscal_quarter_normalised,
    current_week_of_fiscal_quarter,
    NULL AS created_date,
    NULL AS created_month,
    NULL AS created_fiscal_quarter_date,
    NULL AS created_fiscal_quarter_name,
    NULL AS created_fiscal_year,
    NULL AS sales_accepted_date,
    NULL AS sales_accepted_month,
    NULL AS sales_accepted_fiscal_quarter_date,
    NULL AS sales_accepted_fiscal_quarter_name,
    NULL AS sales_accepted_fiscal_year,
    NULL AS close_date,
    NULL AS close_month,
    NULL AS close_fiscal_quarter_date,
    NULL AS close_fiscal_quarter_name,
    NULL AS close_fiscal_year,
    NULL AS stage_0_pending_acceptance_date,
    NULL AS stage_0_pending_acceptance_month,
    NULL AS stage_0_pending_acceptance_fiscal_quarter_date,
    NULL AS stage_0_pending_acceptance_fiscal_quarter_name,
    NULL AS stage_0_pending_acceptance_fiscal_year,
    NULL AS stage_1_discovery_date,
    NULL AS stage_1_discovery_month,
    NULL AS stage_1_discovery_fiscal_quarter_date,
    NULL AS stage_1_discovery_fiscal_quarter_name,
    NULL AS stage_1_discovery_fiscal_year,
    NULL AS stage_2_scoping_date,
    NULL AS stage_2_scoping_month,
    NULL AS stage_2_scoping_fiscal_quarter_date,
    NULL AS stage_2_scoping_fiscal_quarter_name,
    NULL AS stage_2_scoping_fiscal_year,
    NULL AS stage_3_technical_evaluation_date,
    NULL AS stage_3_technical_evaluation_month,
    NULL AS stage_3_technical_evaluation_fiscal_quarter_date,
    NULL AS stage_3_technical_evaluation_fiscal_quarter_name,
    NULL AS stage_3_technical_evaluation_fiscal_year,
    NULL AS stage_4_proposal_date,
    NULL AS stage_4_proposal_month,
    NULL AS stage_4_proposal_fiscal_quarter_date,
    NULL AS stage_4_proposal_fiscal_quarter_name,
    NULL AS stage_4_proposal_fiscal_year,
    NULL AS stage_5_negotiating_date,
    NULL AS stage_5_negotiating_month,
    NULL AS stage_5_negotiating_fiscal_quarter_date,
    NULL AS stage_5_negotiating_fiscal_quarter_name,
    NULL AS stage_5_negotiating_fiscal_year,
    NULL AS stage_6_awaiting_signature_date,
    NULL AS stage_6_awaiting_signature_date_date,
    NULL AS stage_6_awaiting_signature_date_month,
    NULL AS stage_6_awaiting_signature_date_fiscal_quarter_date,
    NULL AS stage_6_awaiting_signature_date_fiscal_quarter_name,
    NULL AS stage_6_awaiting_signature_date_fiscal_year,
    NULL AS stage_6_closed_won_date,
    NULL AS stage_6_closed_won_month,
    NULL AS stage_6_closed_won_fiscal_quarter_date,
    NULL AS stage_6_closed_won_fiscal_quarter_name,
    NULL AS stage_6_closed_won_fiscal_year,
    NULL AS stage_6_closed_lost_date,
    NULL AS stage_6_closed_lost_month,
    NULL AS stage_6_closed_lost_fiscal_quarter_date,
    NULL AS stage_6_closed_lost_fiscal_quarter_name,
    NULL AS stage_6_closed_lost_fiscal_year,
    NULL AS subscription_start_date,
    NULL AS subscription_start_month,
    NULL AS subscription_start_fiscal_quarter_date,
    NULL AS subscription_start_fiscal_quarter_name,
    NULL AS subscription_start_fiscal_year,
    NULL AS subscription_end_date,
    NULL AS subscription_end_month,
    NULL AS subscription_end_fiscal_quarter_date,
    NULL AS subscription_end_fiscal_quarter_name,
    NULL AS subscription_end_fiscal_year,
    NULL AS sales_qualified_date,
    NULL AS sales_qualified_month,
    NULL AS sales_qualified_fiscal_quarter_date,
    NULL AS sales_qualified_fiscal_quarter_name,
    NULL AS sales_qualified_fiscal_year,
    NULL AS last_activity_date,
    NULL AS last_activity_month,
    NULL AS last_activity_fiscal_quarter_date,
    NULL AS last_activity_fiscal_quarter_name,
    NULL AS last_activity_fiscal_year,
    NULL AS sales_last_activity_date,
    NULL AS sales_last_activity_month,
    NULL AS sales_last_activity_fiscal_quarter_date,
    NULL AS sales_last_activity_fiscal_quarter_name,
    NULL AS sales_last_activity_fiscal_year,
    NULL AS technical_evaluation_date,
    NULL AS technical_evaluation_month,
    NULL AS technical_evaluation_fiscal_quarter_date,
    NULL AS technical_evaluation_fiscal_quarter_name,
    NULL AS technical_evaluation_fiscal_year,
    NULL AS arr_created_date,
    NULL AS arr_created_month,
    NULL AS arr_created_fiscal_quarter_date,
    NULL AS arr_created_fiscal_quarter_name,
    NULL AS arr_created_fiscal_year,
    NULL AS pipeline_created_date,
    NULL AS pipeline_created_month,
    NULL AS pipeline_created_fiscal_quarter_date,
    NULL AS pipeline_created_fiscal_quarter_name,
    NULL AS pipeline_created_fiscal_year,
    NULL AS net_arr_created_date,
    NULL AS net_arr_created_month,
    NULL AS net_arr_created_fiscal_quarter_date,
    NULL AS net_arr_created_fiscal_quarter_name,
    NULL AS net_arr_created_fiscal_year,
    snapshot_day,
    snapshot_day_name,
    snapshot_day_of_week,
    snapshot_first_day_of_week,
    snapshot_week_of_year,
    snapshot_day_of_month,
    snapshot_day_of_quarter,
    snapshot_day_of_year,
    snapshot_fiscal_quarter,
    snapshot_day_of_fiscal_quarter,
    snapshot_day_of_fiscal_year,
    snapshot_month_name,
    snapshot_first_day_of_month,
    snapshot_last_day_of_month,
    snapshot_first_day_of_year,
    snapshot_last_day_of_year,
    snapshot_first_day_of_quarter,
    snapshot_last_day_of_quarter,
    snapshot_first_day_of_fiscal_quarter,
    snapshot_last_day_of_fiscal_quarter,
    snapshot_first_day_of_fiscal_year,
    snapshot_last_day_of_fiscal_year,
    snapshot_week_of_fiscal_year,
    snapshot_month_of_fiscal_year,
    snapshot_last_day_of_week,
    snapshot_quarter_name,
    snapshot_fiscal_quarter_name_fy,
    snapshot_fiscal_quarter_number_absolute,
    snapshot_fiscal_month_name,
    snapshot_fiscal_month_name_fy,
    snapshot_holiday_desc,
    snapshot_is_holiday,
    snapshot_last_month_of_fiscal_quarter,
    snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    snapshot_last_month_of_fiscal_year,
    snapshot_is_first_day_of_last_month_of_fiscal_year,
    snapshot_days_in_month_count,
    snapshot_week_of_month_normalised,
    snapshot_week_of_fiscal_quarter_normalised,
    snapshot_is_first_day_of_fiscal_quarter_week,
    snapshot_days_until_last_day_of_month,
    snapshot_week_of_fiscal_quarter,
    NULL AS positive_booked_deal_count_in_snapshot_quarter,
    NULL AS positive_booked_net_arr_in_snapshot_quarter,
    NULL AS positive_open_deal_count_in_snapshot_quarter,
    NULL AS positive_open_net_arr_in_snapshot_quarter,
    NULL AS closed_deals_in_snapshot_quarter,
    NULL AS closed_net_arr_in_snapshot_quarter,
    NULL AS open_1plus_net_arr_in_snapshot_quarter,
    NULL AS open_3plus_net_arr_in_snapshot_quarter,
    NULL AS open_4plus_net_arr_in_snapshot_quarter,
    NULL AS open_1plus_deal_count_in_snapshot_quarter,
    NULL AS open_3plus_deal_count_in_snapshot_quarter,
    NULL AS open_4plus_deal_count_in_snapshot_quarter,
    NULL AS created_arr_in_snapshot_quarter,
    NULL AS closed_won_opps_in_snapshot_quarter,
    NULL AS closed_opps_in_snapshot_quarter,
    NULL AS booked_net_arr_in_snapshot_quarter,
    NULL AS created_deals_in_snapshot_quarter,
    NULL AS cycle_time_in_days_in_snapshot_quarter,
    NULL AS booked_deal_count_in_snapshot_quarter,
    NULL AS created_arr,
    NULL AS closed_won_opps,
    NULL AS closed_opps,
    NULL AS closed_net_arr,
    NULL AS segment_order_type_iacv_to_net_arr_ratio,
    NULL AS calculated_from_ratio_net_arr,
    NULL AS net_arr,
    NULL AS raw_net_arr,
    NULL AS created_and_won_same_quarter_net_arr_combined,
    NULL AS new_logo_count,
    NULL AS amount,
    NULL AS recurring_amount,
    NULL AS true_up_amount,
    NULL AS proserv_amount,
    NULL AS other_non_recurring_amount,
    NULL AS arr_basis,
    NULL AS arr,
    NULL AS count_crm_attribution_touchpoints,
    NULL AS weighted_linear_iacv,
    NULL AS count_campaigns,
    NULL AS probability,
    NULL AS days_in_sao,
    NULL AS open_1plus_deal_count,
    NULL AS open_3plus_deal_count,
    NULL AS open_4plus_deal_count,
    NULL AS booked_deal_count,
    NULL AS churned_contraction_deal_count,
    NULL AS open_1plus_net_arr,
    NULL AS open_3plus_net_arr,
    NULL AS open_4plus_net_arr,
    NULL AS booked_net_arr,
    NULL AS churned_contraction_net_arr,
    NULL AS calculated_deal_count,
    NULL AS booked_churned_contraction_deal_count,
    NULL AS booked_churned_contraction_net_arr,
    NULL AS renewal_amount,
    NULL AS total_contract_value,
    NULL AS days_in_stage,
    NULL AS calculated_age_in_days,
    NULL AS days_since_last_activity,
    NULL AS pre_military_invasion_arr,
    NULL AS won_arr_basis_for_clari,
    NULL AS arr_basis_for_clari,
    NULL AS forecasted_churn_for_clari,
    NULL AS override_arr_basis_clari,
    NULL AS vsa_start_date_net_arr,
    NULL AS day_of_week,
    NULL AS first_day_of_week,
    NULL AS date_id,
    NULL AS fiscal_month_name_fy,
    NULL AS fiscal_quarter_name_fy,
    NULL AS first_day_of_fiscal_quarter,
    NULL AS first_day_of_fiscal_year,
    NULL AS last_day_of_week,
    NULL AS last_day_of_month,
    NULL AS last_day_of_fiscal_quarter,
    NULL AS last_day_of_fiscal_year,
    NULL AS is_current_snapshot_quarter,
    NULL AS is_current_snapshot_week,
    'targets_actuals' AS source,
    total_quarter_target,
    coverage_booked_net_arr,
    coverage_open_1plus_net_arr,
    coverage_open_3plus_net_arr,
    coverage_open_4plus_net_arr,
    total_booked_net_arr
  FROM targets_actuals

)

SELECT 
  unioned.*,
  MAX(IFF(snapshot_date <= CURRENT_DATE(),snapshot_date, NULL)) OVER () AS max_snapshot_date,
  FLOOR((DATEDIFF(day, current_first_day_of_fiscal_quarter, max_snapshot_date) / 7)) 
                                                                        AS most_recent_snapshot_week
FROM unioned

  