{{ simple_cte([
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('order_type', 'prep_order_type'),
    ('dim_date', 'dim_date'),
    ('sales_rep', 'wk_prep_crm_user'),
    ('prep_crm_account', 'prep_crm_account'),
    ('sfdc_opportunity', 'wk_prep_crm_opportunity'),
    ('deal_path', 'prep_deal_path'),
    ('sales_segment', 'prep_sales_segment'),
    ('crm_account_dimensions', 'map_crm_account'),
    ('prep_crm_user_hierarchy', 'wk_prep_crm_user_hierarchy'),
]) }},



 final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['sales_rep_account.dim_crm_user_hierarchy_sk',
                                 'dim_date.fiscal_year', 
                                 'dim_date.first_day_of_month', 
                                 'sales_qualified_source.dim_sales_qualified_source_id',
                                 'order_type.dim_order_type_id',
                                 'dim_date.date_day'
                                 ]) }}                                                                                          AS actuals_targets_pk,
    sfdc_opportunity.dim_crm_opportunity_id,
    

    --Common dimension keys
    {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                                               AS dim_sales_qualified_source_id,
    {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                                       AS dim_order_type_id,
    {{ get_keyed_nulls('order_type_live.dim_order_type_id') }}                                                                  AS dim_order_type_live_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_hierarchy_sk') }}                                                        AS dim_crm_user_hierarchy_sk,
    {{ get_keyed_nulls('crm_account_dimensions.dim_parent_sales_segment_id,sales_segment.dim_sales_segment_id') }}              AS dim_parent_sales_segment_id,
    {{ get_keyed_nulls('crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id') }}             AS dim_account_sales_segment_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_hierarchy_id') }}                                                  AS dim_crm_opp_owner_user_hierarchy_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_business_unit_id') }}                                              AS dim_crm_opp_owner_business_unit_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_sales_segment_id') }}                                              AS dim_crm_opp_owner_sales_segment_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_geo_id') }}                                                        AS dim_crm_opp_owner_geo_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_region_id') }}                                                     AS dim_crm_opp_owner_region_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_area_id') }}                                                       AS dim_crm_opp_owner_area_stamped_id,
    {{ get_keyed_nulls('sales_rep.dim_crm_user_hierarchy_sk') }}                                                                AS dim_crm_user_hierarchy_live_sk,
    {{ get_keyed_nulls('sales_rep.dim_crm_user_business_unit_id') }}                                                            AS dim_crm_user_business_unit_id,
    {{ get_keyed_nulls('sales_rep.dim_crm_user_sales_segment_id') }}                                                            AS dim_crm_user_sales_segment_id,
    {{ get_keyed_nulls('sales_rep.dim_crm_user_geo_id') }}                                                                      AS dim_crm_user_geo_id,
    {{ get_keyed_nulls('sales_rep.dim_crm_user_region_id') }}                                                                   AS dim_crm_user_region_id,
    {{ get_keyed_nulls('sales_rep.dim_crm_user_area_id') }}                                                                     AS dim_crm_user_area_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_hierarchy_sk') }}                                                        AS dim_crm_user_hierarchy_account_user_sk,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_business_unit_id') }}                                                    AS dim_crm_account_user_business_unit_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_sales_segment_id') }}                                                    AS dim_crm_account_user_sales_segment_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_geo_id') }}                                                              AS dim_crm_account_user_geo_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_region_id') }}                                                           AS dim_crm_account_user_region_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_area_id') }}                                                             AS dim_crm_account_user_area_id,
    CASE
        WHEN close_fiscal_year < dim_date.current_fiscal_year AND sales_rep_account.is_hybrid_user = 0
          THEN dim_crm_user_hierarchy_account_user_sk  -- live account owner hierarchy
        WHEN close_fiscal_year < dim_date.current_fiscal_year AND sales_rep_account.is_hybrid_user = 1
          THEN {{ get_keyed_nulls('account_hierarchy.dim_crm_user_hierarchy_sk') }} -- account hierarchy
        ELSE sfdc_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk -- stamped account owner hierarchy
    END                                                                                                                         AS dim_crm_current_account_set_hierarchy_sk,
    crm_account_dimensions.dim_parent_sales_territory_id,
    crm_account_dimensions.dim_parent_industry_id,
    
    crm_account_dimensions.dim_account_sales_territory_id,
    crm_account_dimensions.dim_account_industry_id,
    crm_account_dimensions.dim_account_location_country_id,
    crm_account_dimensions.dim_account_location_region_id,
    
    sfdc_opportunity.merged_opportunity_id                                                                              AS merged_crm_opportunity_id,
    sfdc_opportunity.dim_crm_account_id,
    sfdc_opportunity.dim_crm_person_id,
    sfdc_opportunity.sfdc_contact_id,
    sfdc_opportunity.record_type_id,

    --attributes
    sfdc_opportunity.report_user_segment_geo_region_area_sqs_ot,
    sales_qualified_source.sales_qualified_source_name,
    order_type.order_type_name AS order_type,
    order_type_live.order_type_name AS order_type_live,
    sfdc_opportunity.order_type_grouped,
    sales_rep_account.crm_user_sales_segment, 
    sales_rep_account.crm_user_geo, 
    sales_rep_account.crm_user_region, 
    sales_rep_account.crm_user_area, 
    sales_rep_account.crm_user_business_unit,
    sfdc_opportunity.stage_name,
    deal_path.deal_path_name,
    sfdc_opportunity.sales_type,
    prep_crm_account.parent_crm_account_industry,

    -- dates
    sfdc_opportunity.snapshot_date,
    sfdc_opportunity.snapshot_month,
    sfdc_opportunity.snapshot_fiscal_year,
    sfdc_opportunity.snapshot_fiscal_quarter_name,
    sfdc_opportunity.snapshot_fiscal_quarter_date,
    sfdc_opportunity.snapshot_day_of_fiscal_quarter_normalised,
    sfdc_opportunity.snapshot_day_of_fiscal_year_normalised,
    sfdc_opportunity.created_date,
    sfdc_opportunity.created_date_id,
    sfdc_opportunity.sales_accepted_date,
    sfdc_opportunity.sales_accepted_date_id,
    sfdc_opportunity.close_date,
    sfdc_opportunity.close_date_id,
    sfdc_opportunity.arr_created_date_id,
    sfdc_opportunity.arr_created_date,
    sfdc_opportunity.stage_0_pending_acceptance_date,
    sfdc_opportunity.stage_0_pending_acceptance_date_id,
    sfdc_opportunity.stage_1_discovery_date,
    sfdc_opportunity.stage_1_discovery_date_id,
    sfdc_opportunity.stage_2_scoping_date,
    sfdc_opportunity.stage_2_scoping_date_id,
    sfdc_opportunity.stage_3_technical_evaluation_date,
    sfdc_opportunity.stage_3_technical_evaluation_date_id,
    sfdc_opportunity.stage_4_proposal_date,
    sfdc_opportunity.stage_4_proposal_date_id,
    sfdc_opportunity.stage_5_negotiating_date,
    sfdc_opportunity.stage_5_negotiating_date_id,
    sfdc_opportunity.stage_6_awaiting_signature_date,
    sfdc_opportunity.stage_6_awaiting_signature_date_id,
    sfdc_opportunity.stage_6_closed_won_date,
    sfdc_opportunity.stage_6_closed_won_date_id,
    sfdc_opportunity.stage_6_closed_lost_date,
    sfdc_opportunity.stage_6_closed_lost_date_id,
    sfdc_opportunity.days_in_0_pending_acceptance,
    sfdc_opportunity.days_in_1_discovery,
    sfdc_opportunity.days_in_2_scoping,
    sfdc_opportunity.days_in_3_technical_evaluation,
    sfdc_opportunity.days_in_4_proposal,
    sfdc_opportunity.days_in_5_negotiating,
    sfdc_opportunity.subscription_start_date_id,
    sfdc_opportunity.subscription_end_date_id,
    sfdc_opportunity.sales_qualified_date_id,
    sfdc_opportunity.last_activity_date,
    sfdc_opportunity.last_activity_date_id,
    sfdc_opportunity.sales_last_activity_date,
    sfdc_opportunity.sales_last_activity_date_id,
    sfdc_opportunity.technical_evaluation_date,
    sfdc_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk,
    sfdc_opportunity.technical_evaluation_date_id,
    sfdc_opportunity.ssp_id,
    sfdc_opportunity.ga_client_id,

    -- flags
    sfdc_opportunity.is_closed,
    sfdc_opportunity.is_won,
    sfdc_opportunity.is_refund,
    sfdc_opportunity.is_downgrade,
    sfdc_opportunity.is_swing_deal,
    sfdc_opportunity.is_edu_oss,
    sfdc_opportunity.is_web_portal_purchase,
    sfdc_opportunity.fpa_master_bookings_flag,
    sfdc_opportunity.is_sao,
    sfdc_opportunity.is_sdr_sao,
    sfdc_opportunity.is_net_arr_closed_deal,
    sfdc_opportunity.is_new_logo_first_order,
    sfdc_opportunity.is_net_arr_pipeline_created_combined,
    sfdc_opportunity.is_win_rate_calc,
    sfdc_opportunity.is_closed_won,
    sfdc_opportunity.is_stage_1_plus,
    sfdc_opportunity.is_stage_3_plus,
    sfdc_opportunity.is_stage_4_plus,
    sfdc_opportunity.is_lost,
    sfdc_opportunity.is_open,
    sfdc_opportunity.is_active,
    sfdc_opportunity.is_credit,
    sfdc_opportunity.is_renewal,
    sfdc_opportunity.is_deleted,
    sfdc_opportunity.is_excluded_from_pipeline_created_combined,
    sfdc_opportunity.created_in_snapshot_quarter_deal_count,
    sfdc_opportunity.is_duplicate,
    sfdc_opportunity.is_contract_reset,
    sfdc_opportunity.is_comp_new_logo_override,
    sfdc_opportunity.is_eligible_open_pipeline_combined,
    sfdc_opportunity.is_eligible_age_analysis_combined,
    sfdc_opportunity.is_eligible_churn_contraction,
    sfdc_opportunity.is_booked_net_arr,
    sfdc_opportunity.is_abm_tier_sao,
    sfdc_opportunity.is_abm_tier_closed_won,

    sfdc_opportunity.primary_solution_architect,
    sfdc_opportunity.product_details,
    sfdc_opportunity.product_category,
    sfdc_opportunity.intended_product_tier,
    sfdc_opportunity.products_purchased,
    sfdc_opportunity.growth_type,
    sfdc_opportunity.opportunity_deal_size,
    sfdc_opportunity.closed_buckets,
    sfdc_opportunity.calculated_deal_size,
    sfdc_opportunity.deal_size,

    -- channel fields
    sfdc_opportunity.lead_source,
    sfdc_opportunity.dr_partner_deal_type,
    sfdc_opportunity.dr_partner_engagement,
    sfdc_opportunity.partner_account,
    sfdc_opportunity.dr_status,
    sfdc_opportunity.dr_deal_id,
    sfdc_opportunity.dr_primary_registration,
    sfdc_opportunity.distributor,
    sfdc_opportunity.influence_partner,
    sfdc_opportunity.fulfillment_partner,
    sfdc_opportunity.platform_partner,
    sfdc_opportunity.partner_track,
    sfdc_opportunity.resale_partner_track,
    sfdc_opportunity.is_public_sector_opp,
    sfdc_opportunity.is_registration_from_portal,
    sfdc_opportunity.calculated_discount,
    sfdc_opportunity.partner_discount,
    sfdc_opportunity.partner_discount_calc,
    sfdc_opportunity.comp_channel_neutral,

    -- additive fields
    sfdc_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    sfdc_opportunity.calculated_from_ratio_net_arr,
    sfdc_opportunity.net_arr,
    sfdc_opportunity.raw_net_arr,
    sfdc_opportunity.created_and_won_same_quarter_net_arr_combined,
    sfdc_opportunity.new_logo_count,
    sfdc_opportunity.amount,
    sfdc_opportunity.recurring_amount,
    sfdc_opportunity.true_up_amount,
    sfdc_opportunity.proserv_amount,
    sfdc_opportunity.other_non_recurring_amount,
    sfdc_opportunity.arr_basis,
    sfdc_opportunity.arr,
    sfdc_opportunity.count_crm_attribution_touchpoints,
    sfdc_opportunity.weighted_linear_iacv,
    sfdc_opportunity.count_campaigns,
    sfdc_opportunity.probability,
    sfdc_opportunity.days_in_sao,
    sfdc_opportunity.open_1plus_deal_count,
    sfdc_opportunity.open_3plus_deal_count,
    sfdc_opportunity.open_4plus_deal_count,
    sfdc_opportunity.booked_deal_count,
    sfdc_opportunity.churned_contraction_deal_count,
    sfdc_opportunity.open_1plus_net_arr,
    sfdc_opportunity.open_3plus_net_arr,
    sfdc_opportunity.open_4plus_net_arr,
    sfdc_opportunity.booked_net_arr,
    sfdc_opportunity.churned_contraction_net_arr,
    sfdc_opportunity.calculated_deal_count,
    sfdc_opportunity.booked_churned_contraction_deal_count,
    sfdc_opportunity.booked_churned_contraction_net_arr,
    sfdc_opportunity.renewal_amount,
    sfdc_opportunity.total_contract_value,
    sfdc_opportunity.days_in_stage,
    sfdc_opportunity.calculated_age_in_days,
    sfdc_opportunity.days_since_last_activity,
    sfdc_opportunity.pre_military_invasion_arr,
    sfdc_opportunity.won_arr_basis_for_clari,
    sfdc_opportunity.arr_basis_for_clari,
    sfdc_opportunity.forecasted_churn_for_clari,
    sfdc_opportunity.override_arr_basis_clari,
    sfdc_opportunity.vsa_start_date_net_arr,
    sfdc_opportunity.cycle_time_in_days_combined,
    dim_date.day_of_week,
    dim_date.first_day_of_week,
    dim_date.date_id,
    dim_date.fiscal_month_name_fy,
    dim_date.fiscal_quarter_name_fy,
    dim_date.first_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year,
    dim_date.last_day_of_week,
    dim_date.last_day_of_month,
    dim_date.last_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_year
  FROM sfdc_opportunity
  INNER JOIN dim_date
    ON sfdc_opportunity.snapshot_date = dim_date.date_actual
  LEFT JOIN crm_account_dimensions
    ON sfdc_opportunity.dim_crm_account_id = crm_account_dimensions.dim_crm_account_id
  LEFT JOIN prep_crm_account
  ON sfdc_opportunity.dim_crm_account_id = prep_crm_account.dim_crm_account_id
  LEFT JOIN sales_qualified_source
    ON sfdc_opportunity.sales_qualified_source = sales_qualified_source.sales_qualified_source_name
  LEFT JOIN order_type
    ON sfdc_opportunity.order_type = order_type.order_type_name
  LEFT JOIN order_type AS order_type_live
    ON sfdc_opportunity.order_type_live = order_type_live.order_type_name
  LEFT JOIN sales_rep
    ON sfdc_opportunity.dim_crm_user_id = sales_rep.dim_crm_user_id
  LEFT JOIN sales_rep AS sales_rep_account
    ON prep_crm_account.dim_crm_user_id = sales_rep_account.dim_crm_user_id
  LEFT JOIN prep_crm_user_hierarchy
    ON sfdc_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk = prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  LEFT JOIN prep_crm_user_hierarchy AS account_hierarchy
    ON prep_crm_account.dim_crm_parent_account_hierarchy_sk = account_hierarchy.dim_crm_user_hierarchy_sk
  LEFT JOIN deal_path
    ON sfdc_opportunity.deal_path = deal_path.deal_path_name
  LEFT JOIN sales_segment
    ON sfdc_opportunity.sales_segment = sales_segment.sales_segment_name
  WHERE is_live = 0

  {% if is_incremental() %}
  
    AND snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

  {% endif %}


)

SELECT * 
FROM final