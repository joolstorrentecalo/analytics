{{ config(
    materialized="incremental",
    unique_key="crm_opportunity_snapshot_id"
) }}

{{ simple_cte([
    ('sales_rep', 'prep_crm_user_daily_snapshot'),
    ('prep_crm_opportunity', 'prep_crm_opportunity'),
    ('prep_crm_user_hierarchy', 'prep_crm_user_hierarchy'),
    ('prep_date', 'prep_date'),
    ('prep_crm_account', 'prep_crm_account_daily_snapshot'),
    ('crm_account_dimensions','map_crm_account'),
    ('sales_qualified_source','prep_sales_qualified_source'),
    ('order_type','prep_order_type'),
    ('deal_path','prep_deal_path'),
    ('sales_segment','prep_sales_segment'),
    ('dr_partner_engagement','prep_dr_partner_engagement'),
    ('channel_type','prep_channel_type')
]) }},

final AS (

  SELECT 
    prep_crm_opportunity.dim_crm_opportunity_id,
    prep_crm_opportunity.dim_parent_crm_opportunity_id,
    {{ get_keyed_nulls('prep_crm_opportunity.dim_crm_account_id') }}                                                            AS dim_crm_account_id,
    {{ get_keyed_nulls('prep_crm_opportunity.dim_crm_user_id') }}                                                               AS dim_crm_user_id,
    {{ get_keyed_nulls('prep_crm_opportunity.dim_crm_account_user_id') }}                                                       AS dim_crm_account_user_id,
    {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                                       AS dim_order_type_id,
    {{ get_keyed_nulls('order_type_current.dim_order_type_id') }}                                                               AS dim_order_type_current_id,
    {{ get_keyed_nulls('dr_partner_engagement.dim_dr_partner_engagement_id') }}                                                 AS dim_dr_partner_engagement_id,
    {{ get_keyed_nulls('channel_type.dim_channel_type_id') }}                                                                   AS dim_channel_type_id,
    {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                                               AS dim_sales_qualified_source_id,
    {{ get_keyed_nulls('deal_path.dim_deal_path_id') }}                                                                         AS dim_deal_path_id,
    {{ get_keyed_nulls('crm_account_dimensions.dim_parent_sales_segment_id,sales_segment.dim_sales_segment_id') }}              AS dim_parent_sales_segment_id,
    crm_account_dimensions.dim_parent_sales_territory_id,
    crm_account_dimensions.dim_parent_industry_id,
    {{ get_keyed_nulls('crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id') }}             AS dim_account_sales_segment_id,
    crm_account_dimensions.dim_account_sales_territory_id,
    crm_account_dimensions.dim_account_industry_id,
    crm_account_dimensions.dim_account_location_country_id,
    crm_account_dimensions.dim_account_location_region_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_hierarchy_id') }}                                                  AS dim_crm_opp_owner_user_hierarchy_id,
    prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_business_unit_id') }}                                              AS dim_crm_opp_owner_business_unit_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_sales_segment_id') }}                                              AS dim_crm_opp_owner_sales_segment_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_geo_id') }}                                                        AS dim_crm_opp_owner_geo_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_region_id') }}                                                     AS dim_crm_opp_owner_region_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_area_id') }}                                                       AS dim_crm_opp_owner_area_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_name_id') }}                                                  AS dim_crm_opp_owner_role_name_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_1_id') }}                                               AS dim_crm_opp_owner_role_level_1_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_2_id') }}                                               AS dim_crm_opp_owner_role_level_2_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_3_id') }}                                               AS dim_crm_opp_owner_role_level_3_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_4_id') }}                                               AS dim_crm_opp_owner_role_level_4_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_5_id') }}                                               AS dim_crm_opp_owner_role_level_5_id,
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
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_name_id') }}                                                        AS dim_crm_account_user_role_name_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_1_id') }}                                                     AS dim_crm_account_user_role_level_1_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_2_id') }}                                                     AS dim_crm_account_user_role_level_2_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_3_id') }}                                                     AS dim_crm_account_user_role_level_3_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_4_id') }}                                                     AS dim_crm_account_user_role_level_4_id,
    {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_5_id') }}                                                     AS dim_crm_account_user_role_level_5_id,
    prep_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_sales_segment_id
      ELSE dim_crm_opp_owner_sales_segment_stamped_id
    END                                                                                                                         AS dim_crm_current_account_set_sales_segment_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_geo_id
      ELSE  dim_crm_opp_owner_geo_stamped_id
    END                                                                                                                         AS dim_crm_current_account_set_geo_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_region_id
      ELSE dim_crm_opp_owner_region_stamped_id
    END                                                                                                                         AS dim_crm_current_account_set_region_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_area_id
      ELSE dim_crm_opp_owner_area_stamped_id
    END                                                                                                                         AS dim_crm_current_account_set_area_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_business_unit_id
      ELSE dim_crm_opp_owner_business_unit_stamped_id
    END                                                                                                                         AS dim_crm_current_account_set_business_unit_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_role_name_id
      ELSE dim_crm_opp_owner_role_name_id
    END                                                                                                                         AS dim_crm_current_account_set_role_name_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_role_level_1_id
      ELSE dim_crm_opp_owner_role_level_1_id
    END                                                                                                                         AS dim_crm_current_account_set_role_level_1_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_role_level_2_id
      ELSE dim_crm_opp_owner_role_level_2_id
    END                                                                                                                         AS dim_crm_current_account_set_role_level_2_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_role_level_3_id
      ELSE dim_crm_opp_owner_role_level_3_id
    END                                                                                                                         AS dim_crm_current_account_set_role_level_3_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_role_level_4_id
      ELSE dim_crm_opp_owner_role_level_4_id
    END                                                                                                                         AS dim_crm_current_account_set_role_level_4_id,
    CASE
      WHEN close_fiscal_year < prep_date.current_fiscal_year
        THEN dim_crm_account_user_role_level_5_id
      ELSE dim_crm_opp_owner_role_level_5_id
    END                                                                                                                         AS dim_crm_current_account_set_role_level_5_id,
    
    --live fields
    prep_crm_opportunity.sales_qualified_source_live,
    prep_crm_opportunity.sales_qualified_source_grouped_live,
    prep_crm_opportunity.is_edu_oss_live,
    prep_crm_opportunity.opportunity_category_live,
    prep_crm_opportunity.is_jihu_account_live,
    prep_crm_opportunity.deal_path_live,
    prep_crm_opportunity.parent_crm_account_geo_live,
    prep_crm_opportunity.order_type_live,
    prep_crm_opportunity.order_type_grouped_live,

    prep_crm_opportunity.order_type,
    prep_crm_opportunity.opportunity_term_base,
    prep_crm_opportunity.sales_qualified_source,
    prep_crm_opportunity.crm_opp_owner_sales_segment_stamped,
    prep_crm_opportunity.crm_opp_owner_geo_stamped,
    prep_crm_opportunity.crm_opp_owner_region_stamped,
    prep_crm_opportunity.crm_opp_owner_area_stamped,
    prep_crm_opportunity.crm_opp_owner_sales_segment_geo_region_area_stamped,
    prep_crm_opportunity.crm_opp_owner_business_unit_stamped,
    prep_crm_opportunity.created_date,
    prep_crm_opportunity.sales_accepted_date,
    prep_crm_opportunity.close_date,
    prep_crm_opportunity.raw_net_arr,
    prep_crm_opportunity.crm_opportunity_snapshot_id,
    prep_crm_opportunity.snapshot_id,
    prep_crm_opportunity.snapshot_date,
    prep_crm_opportunity.snapshot_month,
    prep_crm_opportunity.snapshot_fiscal_year,
    prep_crm_opportunity.snapshot_fiscal_quarter_name,
    prep_crm_opportunity.snapshot_fiscal_quarter_date,
    prep_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised,
    prep_crm_opportunity.snapshot_day_of_fiscal_year_normalised,
    prep_crm_opportunity.snapshot_last_day_of_fiscal_quarter,
    prep_crm_opportunity.parent_crm_account_geo,
    prep_crm_opportunity.crm_account_owner_sales_segment,
    prep_crm_opportunity.crm_account_owner_geo,
    prep_crm_opportunity.crm_account_owner_region,
    prep_crm_opportunity.crm_account_owner_area,
    prep_crm_opportunity.crm_account_owner_sales_segment_geo_region_area,
    prep_crm_opportunity.fulfillment_partner_account_name,
    prep_crm_opportunity.fulfillment_partner_partner_track,
    prep_crm_opportunity.partner_account_account_name,
    prep_crm_opportunity.partner_account_partner_track,
    prep_crm_opportunity.is_jihu_account,
    prep_crm_opportunity.dim_parent_crm_account_id,
    prep_crm_opportunity.is_open,
    prep_crm_opportunity.opportunity_owner_user_segment,
    prep_crm_opportunity.opportunity_owner_role,
    prep_crm_opportunity.opportunity_owner_title,
    prep_crm_opportunity.opportunity_account_owner_role,
    prep_crm_opportunity.opportunity_name,
    prep_crm_opportunity.is_closed,
    prep_crm_opportunity.days_in_stage,
    prep_crm_opportunity.deployment_preference,
    prep_crm_opportunity.generated_source,
    prep_crm_opportunity.lead_source,
    prep_crm_opportunity.merged_opportunity_id,
    prep_crm_opportunity.duplicate_opportunity_id,
    prep_crm_opportunity.account_owner,
    prep_crm_opportunity.opportunity_owner,
    prep_crm_opportunity.opportunity_owner_manager,
    prep_crm_opportunity.opportunity_owner_department,
    prep_crm_opportunity.opportunity_sales_development_representative,
    prep_crm_opportunity.opportunity_business_development_representative,
    prep_crm_opportunity.opportunity_business_development_representative_lookup,
    prep_crm_opportunity.opportunity_development_representative,
    prep_crm_opportunity.sales_path,
    prep_crm_opportunity.sales_qualified_date,
    prep_crm_opportunity.iqm_submitted_by_role,
    prep_crm_opportunity.sales_type,
    prep_crm_opportunity.net_new_source_categories,
    prep_crm_opportunity.source_buckets,
    prep_crm_opportunity.stage_name,
    prep_crm_opportunity.deal_path,
    prep_crm_opportunity.acv,
    prep_crm_opportunity.amount,
    prep_crm_opportunity.competitors,
    prep_crm_opportunity.critical_deal_flag,
    prep_crm_opportunity.forecast_category_name,
    prep_crm_opportunity.forecasted_iacv,
    prep_crm_opportunity.iacv_created_date,
    prep_crm_opportunity.incremental_acv,
    prep_crm_opportunity.invoice_number,
    prep_crm_opportunity.is_refund,
    prep_crm_opportunity.is_downgrade,
    prep_crm_opportunity.is_swing_deal,
    prep_crm_opportunity.is_edu_oss,
    prep_crm_opportunity.is_ps_opp,
    prep_crm_opportunity.net_incremental_acv,
    prep_crm_opportunity.primary_campaign_source_id,
    prep_crm_opportunity.probability,
    prep_crm_opportunity.professional_services_value,
    prep_crm_opportunity.edu_services_value,
    prep_crm_opportunity.investment_services_value,
    prep_crm_opportunity.pushed_count,
    prep_crm_opportunity.reason_for_loss,
    prep_crm_opportunity.reason_for_loss_details,
    prep_crm_opportunity.refund_iacv,
    prep_crm_opportunity.downgrade_iacv,
    prep_crm_opportunity.renewal_acv,
    prep_crm_opportunity.renewal_amount,
    prep_crm_opportunity.sales_qualified_source_grouped,
    prep_crm_opportunity.sqs_bucket_engagement,
    prep_crm_opportunity.sdr_pipeline_contribution,
    prep_crm_opportunity.solutions_to_be_replaced,
    prep_crm_opportunity.technical_evaluation_date,
    prep_crm_opportunity.total_contract_value,
    prep_crm_opportunity.recurring_amount,
    prep_crm_opportunity.true_up_amount,
    prep_crm_opportunity.proserv_amount,
    prep_crm_opportunity.other_non_recurring_amount,
    prep_crm_opportunity.upside_iacv,
    prep_crm_opportunity.upside_swing_deal_iacv,
    prep_crm_opportunity.is_web_portal_purchase,
    prep_crm_opportunity.partner_initiated_opportunity,
    prep_crm_opportunity.user_segment,
    prep_crm_opportunity.subscription_start_date,
    prep_crm_opportunity.subscription_end_date,
    prep_crm_opportunity.true_up_value,
    prep_crm_opportunity.order_type_current,
    prep_crm_opportunity.order_type_grouped,
    prep_crm_opportunity.growth_type,
    prep_crm_opportunity.arr_basis,
    prep_crm_opportunity.arr,
    prep_crm_opportunity.xdr_net_arr_stage_3,
    prep_crm_opportunity.xdr_net_arr_stage_1,
    prep_crm_opportunity.enterprise_agile_planning_net_arr,
    prep_crm_opportunity.duo_net_arr,
    prep_crm_opportunity.days_in_sao,
    prep_crm_opportunity.new_logo_count,
    prep_crm_opportunity.user_segment_stamped,
    prep_crm_opportunity.user_segment_stamped_grouped,
    prep_crm_opportunity.user_geo_stamped,
    prep_crm_opportunity.user_region_stamped,
    prep_crm_opportunity.user_area_stamped,
    prep_crm_opportunity.user_segment_region_stamped_grouped,
    prep_crm_opportunity.user_segment_geo_region_area_stamped,
    prep_crm_opportunity.crm_opp_owner_user_role_type_stamped,
    prep_crm_opportunity.user_business_unit_stamped,
    prep_crm_opportunity.crm_opp_owner_stamped_name,
    prep_crm_opportunity.crm_account_owner_stamped_name,
    prep_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
    prep_crm_opportunity.sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
    prep_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped,
    prep_crm_opportunity.sao_crm_opp_owner_geo_stamped,
    prep_crm_opportunity.sao_crm_opp_owner_region_stamped,
    prep_crm_opportunity.sao_crm_opp_owner_area_stamped,
    prep_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped,
    prep_crm_opportunity.opportunity_category,
    prep_crm_opportunity.opportunity_health,
    prep_crm_opportunity.risk_type,
    prep_crm_opportunity.risk_reasons,
    prep_crm_opportunity.tam_notes,
    prep_crm_opportunity.primary_solution_architect,
    prep_crm_opportunity.product_details,
    prep_crm_opportunity.product_category,
    prep_crm_opportunity.products_purchased,
    prep_crm_opportunity.opportunity_deal_size,
    prep_crm_opportunity.payment_schedule,
    prep_crm_opportunity.comp_y2_iacv,
    prep_crm_opportunity.comp_new_logo_override,
    prep_crm_opportunity.is_pipeline_created_eligible,
    prep_crm_opportunity.next_steps,
    prep_crm_opportunity.auto_renewal_status,
    prep_crm_opportunity.qsr_notes,
    prep_crm_opportunity.qsr_status,
    prep_crm_opportunity.manager_confidence,
    prep_crm_opportunity.renewal_risk_category,
    prep_crm_opportunity.renewal_swing_arr,
    prep_crm_opportunity.renewal_manager,
    prep_crm_opportunity.renewal_forecast_health,
    prep_crm_opportunity.renewal_ownership,
    prep_crm_opportunity.sales_segment,
    prep_crm_opportunity.parent_segment,
    prep_crm_opportunity.days_in_0_pending_acceptance,
    prep_crm_opportunity.days_in_1_discovery,
    prep_crm_opportunity.days_in_2_scoping,
    prep_crm_opportunity.days_in_3_technical_evaluation,
    prep_crm_opportunity.days_in_4_proposal,
    prep_crm_opportunity.days_in_5_negotiating,
    prep_crm_opportunity.stage_0_pending_acceptance_date,
    prep_crm_opportunity.stage_1_discovery_date,
    prep_crm_opportunity.stage_2_scoping_date,
    prep_crm_opportunity.stage_3_technical_evaluation_date,
    prep_crm_opportunity.stage_4_proposal_date,
    prep_crm_opportunity.stage_5_negotiating_date,
    prep_crm_opportunity.stage_6_awaiting_signature_date,
    prep_crm_opportunity.stage_6_closed_won_date,
    prep_crm_opportunity.stage_6_closed_lost_date,
    prep_crm_opportunity.division_sales_segment_stamped,
    prep_crm_opportunity.dr_partner_deal_type,
    prep_crm_opportunity.dr_partner_engagement,
    prep_crm_opportunity.dr_deal_id,
    prep_crm_opportunity.dr_primary_registration,
    prep_crm_opportunity.channel_type,
    prep_crm_opportunity.partner_account,
    prep_crm_opportunity.dr_status,
    prep_crm_opportunity.distributor,
    prep_crm_opportunity.influence_partner,
    prep_crm_opportunity.is_focus_partner,
    prep_crm_opportunity.fulfillment_partner,
    prep_crm_opportunity.platform_partner,
    prep_crm_opportunity.partner_track,
    prep_crm_opportunity.resale_partner_track,
    prep_crm_opportunity.is_public_sector_opp,
    prep_crm_opportunity.is_registration_from_portal,
    prep_crm_opportunity.calculated_discount,
    prep_crm_opportunity.partner_discount,
    prep_crm_opportunity.partner_discount_calc,
    prep_crm_opportunity.partner_margin_percentage,
    prep_crm_opportunity.comp_channel_neutral,
    prep_crm_opportunity.cp_champion,
    prep_crm_opportunity.cp_close_plan,
    prep_crm_opportunity.cp_decision_criteria,
    prep_crm_opportunity.cp_decision_process,
    prep_crm_opportunity.cp_economic_buyer,
    prep_crm_opportunity.cp_help,
    prep_crm_opportunity.cp_identify_pain,
    prep_crm_opportunity.cp_metrics,
    prep_crm_opportunity.cp_partner,
    prep_crm_opportunity.cp_paper_process,
    prep_crm_opportunity.cp_review_notes,
    prep_crm_opportunity.cp_risks,
    prep_crm_opportunity.cp_use_cases,
    prep_crm_opportunity.cp_value_driver,
    prep_crm_opportunity.cp_why_do_anything_at_all,
    prep_crm_opportunity.cp_why_gitlab,
    prep_crm_opportunity.cp_why_now,
    prep_crm_opportunity.cp_score,
    prep_crm_opportunity.sa_tech_evaluation_close_status,
    prep_crm_opportunity.sa_tech_evaluation_end_date,
    prep_crm_opportunity.sa_tech_evaluation_start_date,
    prep_crm_opportunity.fpa_master_bookings_flag,
    prep_crm_opportunity.downgrade_reason,
    prep_crm_opportunity.ssp_id,
    prep_crm_opportunity.ga_client_id,
    prep_crm_opportunity.vsa_readout,
    prep_crm_opportunity.vsa_start_date_net_arr,
    prep_crm_opportunity.vsa_start_date,
    prep_crm_opportunity.vsa_url,
    prep_crm_opportunity.vsa_status,
    prep_crm_opportunity.vsa_end_date,
    prep_crm_opportunity.military_invasion_comments,
    prep_crm_opportunity.pre_military_invasion_arr,
    prep_crm_opportunity.military_invasion_risk_scale,
    prep_crm_opportunity.downgrade_details,
    prep_crm_opportunity.won_arr_basis_for_clari,
    prep_crm_opportunity.arr_basis_for_clari,
    prep_crm_opportunity.forecasted_churn_for_clari,
    prep_crm_opportunity.override_arr_basis_clari,
    prep_crm_opportunity.intended_product_tier,
    prep_crm_opportunity.ptc_predicted_arr,
    prep_crm_opportunity.ptc_predicted_renewal_risk_category,
    prep_crm_opportunity._last_dbt_run,
    prep_crm_opportunity.days_since_last_activity,
    prep_crm_opportunity.is_deleted,
    prep_crm_opportunity.last_activity_date,
    prep_crm_opportunity.sales_last_activity_date,
    prep_crm_opportunity.record_type_id,
    prep_crm_opportunity.dbt_scd_id,
    prep_crm_opportunity.dbt_valid_from,
    prep_crm_opportunity.dbt_valid_to,
    prep_crm_opportunity.is_live,
    prep_crm_opportunity.primary_key,
    prep_crm_opportunity.created_date_id,
    prep_crm_opportunity.sales_accepted_date_id,
    prep_crm_opportunity.close_date_id,
    prep_crm_opportunity.stage_0_pending_acceptance_date_id,
    prep_crm_opportunity.stage_1_discovery_date_id,
    prep_crm_opportunity.stage_2_scoping_date_id,
    prep_crm_opportunity.stage_3_technical_evaluation_date_id,
    prep_crm_opportunity.stage_4_proposal_date_id,
    prep_crm_opportunity.stage_5_negotiating_date_id,
    prep_crm_opportunity.stage_6_awaiting_signature_date_id,
    prep_crm_opportunity.stage_6_closed_won_date_id,
    prep_crm_opportunity.stage_6_closed_lost_date_id,
    prep_crm_opportunity.technical_evaluation_date_id,
    prep_crm_opportunity.last_activity_date_id,
    prep_crm_opportunity.sales_last_activity_date_id,
    prep_crm_opportunity.subscription_start_date_id,
    prep_crm_opportunity.subscription_end_date_id,
    prep_crm_opportunity.sales_qualified_date_id,
    prep_crm_opportunity.close_fiscal_quarter_date,
    prep_crm_opportunity.close_day_of_fiscal_quarter_normalised,
    prep_crm_opportunity.close_fiscal_year,
    prep_crm_opportunity.arr_created_date_id,
    prep_crm_opportunity.arr_created_date,
    prep_crm_opportunity.arr_created_fiscal_quarter_name,
    prep_crm_opportunity.arr_created_fiscal_quarter_date,
    prep_crm_opportunity.created_fiscal_quarter_name,
    prep_crm_opportunity.created_fiscal_quarter_date,
    prep_crm_opportunity.subscription_start_date_fiscal_quarter_name,
    prep_crm_opportunity.subscription_start_date_fiscal_quarter_date,
    prep_crm_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    prep_crm_opportunity.opportunity_based_iacv_to_net_arr_ratio,
    prep_crm_opportunity.calculated_from_ratio_net_arr,
    prep_crm_opportunity.net_arr,
    prep_crm_opportunity.landing_quarter_relative_to_arr_created_date,
    prep_crm_opportunity.landing_quarter_relative_to_snapshot_date,
    prep_crm_opportunity.snapshot_to_close_diff,
    prep_crm_opportunity.arr_created_to_close_diff,


    -- Flags
    prep_crm_opportunity.is_abm_tier_sao,
    prep_crm_opportunity.is_abm_tier_closed_won,
    prep_crm_opportunity.is_risky,
    prep_crm_opportunity.opportunity_term,
    prep_crm_opportunity.is_active,
    prep_crm_opportunity.is_won,
    prep_crm_opportunity.is_stage_1_plus,
    prep_crm_opportunity.is_stage_3_plus,
    prep_crm_opportunity.is_stage_4_plus,
    prep_crm_opportunity.is_lost,
    prep_crm_opportunity.is_renewal,
    prep_crm_opportunity.is_decommissed,
    prep_crm_opportunity.is_sao,
    prep_crm_opportunity.is_sdr_sao,
    prep_crm_opportunity.is_net_arr_closed_deal,
    prep_crm_opportunity.is_new_logo_first_order,
    prep_crm_opportunity.is_booked_net_arr,
    prep_crm_opportunity.is_net_arr_pipeline_created,
    prep_crm_opportunity.is_win_rate_calc,
    prep_crm_opportunity.is_closed_won,
    prep_crm_opportunity.is_eligible_open_pipeline,
    prep_crm_opportunity.is_eligible_sao,
    prep_crm_opportunity.is_eligible_asp_analysis,
    prep_crm_opportunity.is_eligible_age_analysis,
    prep_crm_opportunity.is_eligible_net_arr,
    prep_crm_opportunity.is_eligible_churn_contraction,
    prep_crm_opportunity.is_duplicate,
    prep_crm_opportunity.is_credit,
    prep_crm_opportunity.is_contract_reset,

    prep_crm_opportunity.alliance_type_current,
    prep_crm_opportunity.alliance_type_short_current,
    prep_crm_opportunity.alliance_type,
    prep_crm_opportunity.alliance_type_short,
    prep_crm_opportunity.resale_partner_name,
    prep_crm_opportunity.dim_quote_id,
    prep_crm_opportunity.quote_start_date,
    prep_crm_opportunity.dim_crm_person_id,
    prep_crm_opportunity.sfdc_contact_id,
    prep_crm_opportunity.record_type_name,
    prep_crm_opportunity.count_crm_attribution_touchpoints,
    prep_crm_opportunity.count_campaigns,
    prep_crm_opportunity.weighted_linear_iacv,
    prep_crm_opportunity.closed_buckets,
    prep_crm_opportunity.churn_contraction_net_arr_bucket,
    prep_crm_opportunity.sdr_or_bdr,
    prep_crm_opportunity.stage_category,
    prep_crm_opportunity.deal_group,
    prep_crm_opportunity.deal_category,
    prep_crm_opportunity.reason_for_loss_staged,
    prep_crm_opportunity.reason_for_loss_calc,
    prep_crm_opportunity.churn_contraction_type,
    prep_crm_opportunity.renewal_timing_status,
    prep_crm_opportunity.churned_contraction_net_arr_bucket,
    prep_crm_opportunity.deal_path_engagement,
    prep_crm_opportunity.deal_size,
    prep_crm_opportunity.calculated_deal_size,
    prep_crm_opportunity.stage_name_3plus,
    prep_crm_opportunity.stage_name_4plus,
    prep_crm_opportunity.calculated_deal_count,
    prep_crm_opportunity.churned_contraction_deal_count,
    prep_crm_opportunity.booked_churned_contraction_deal_count,
    prep_crm_opportunity.booked_churned_contraction_net_arr,
    prep_crm_opportunity.churned_contraction_net_arr,
    prep_crm_opportunity.calculated_partner_track,
    prep_crm_opportunity.is_excluded_from_pipeline_created,
    prep_crm_opportunity.calculated_age_in_days,
    prep_crm_opportunity.created_and_won_same_quarter_net_arr,
    prep_crm_opportunity.is_comp_new_logo_override,
    prep_crm_opportunity.created_in_snapshot_quarter_net_arr,
    prep_crm_opportunity.created_in_snapshot_quarter_deal_count,
    prep_crm_opportunity.competitors_other_flag,
    prep_crm_opportunity.competitors_gitlab_core_flag,
    prep_crm_opportunity.competitors_none_flag,
    prep_crm_opportunity.competitors_github_enterprise_flag,
    prep_crm_opportunity.competitors_bitbucket_server_flag,
    prep_crm_opportunity.competitors_unknown_flag,
    prep_crm_opportunity.competitors_github_flag,
    prep_crm_opportunity.competitors_gitlab_flag,
    prep_crm_opportunity.competitors_jenkins_flag,
    prep_crm_opportunity.competitors_azure_devops_flag,
    prep_crm_opportunity.competitors_svn_flag,
    prep_crm_opportunity.competitors_bitbucket_flag,
    prep_crm_opportunity.competitors_atlassian_flag,
    prep_crm_opportunity.competitors_perforce_flag,
    prep_crm_opportunity.competitors_visual_studio_flag,
    prep_crm_opportunity.competitors_azure_flag,
    prep_crm_opportunity.competitors_amazon_code_commit_flag,
    prep_crm_opportunity.competitors_circleci_flag,
    prep_crm_opportunity.competitors_bamboo_flag,
    prep_crm_opportunity.competitors_aws_flag,
    prep_crm_opportunity.cycle_time_in_days,
    prep_crm_opportunity.created_arr_in_snapshot_quarter,
    prep_crm_opportunity.closed_won_opps_in_snapshot_quarter,
    prep_crm_opportunity.closed_opps_in_snapshot_quarter,
    prep_crm_opportunity.booked_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.created_deals_in_snapshot_quarter,
    prep_crm_opportunity.cycle_time_in_days_in_snapshot_quarter,
    prep_crm_opportunity.booked_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.open_1plus_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.open_3plus_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.open_4plus_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.open_1plus_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.open_3plus_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.open_4plus_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.positive_booked_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.positive_booked_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.positive_open_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.positive_open_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.closed_deals_in_snapshot_quarter,
    prep_crm_opportunity.closed_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.created_arr,
    prep_crm_opportunity.closed_won_opps,
    prep_crm_opportunity.closed_opps,
    prep_crm_opportunity.created_deals,
    prep_crm_opportunity.positive_booked_deal_count,
    prep_crm_opportunity.positive_booked_net_arr,
    prep_crm_opportunity.positive_open_deal_count,
    prep_crm_opportunity.positive_open_net_arr,
    prep_crm_opportunity.closed_deals,
    prep_crm_opportunity.closed_net_arr,
    prep_crm_opportunity.open_1plus_net_arr,
    prep_crm_opportunity.open_3plus_net_arr,
    prep_crm_opportunity.open_4plus_net_arr,
    prep_crm_opportunity.booked_net_arr,
    prep_crm_opportunity.open_1plus_deal_count,
    prep_crm_opportunity.open_3plus_deal_count,
    prep_crm_opportunity.open_4plus_deal_count,
    prep_crm_opportunity.booked_deal_count
  FROM prep_crm_opportunity
  LEFT JOIN prep_crm_user_hierarchy
    ON prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk = prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  LEFT JOIN prep_crm_account
    ON prep_crm_opportunity.dim_crm_account_id = prep_crm_account.dim_crm_account_id
      AND prep_crm_opportunity.snapshot_id = prep_crm_account.snapshot_id
  LEFT JOIN sales_rep
    ON prep_crm_opportunity.dim_crm_user_id = sales_rep.dim_crm_user_id
      AND prep_crm_opportunity.snapshot_id = sales_rep.snapshot_id
  LEFT JOIN sales_rep AS sales_rep_account
    ON prep_crm_account.dim_crm_user_id = sales_rep_account.dim_crm_user_id
      AND prep_crm_account.snapshot_id = sales_rep_account.snapshot_id
  LEFT JOIN crm_account_dimensions
    ON prep_crm_opportunity.dim_crm_account_id = crm_account_dimensions.dim_crm_account_id
  LEFT JOIN sales_qualified_source
    ON prep_crm_opportunity.sales_qualified_source = sales_qualified_source.sales_qualified_source_name
  LEFT JOIN order_type
    ON prep_crm_opportunity.order_type = order_type.order_type_name
  LEFT JOIN order_type AS order_type_current
    ON prep_crm_opportunity.order_type_current = order_type_current.order_type_name
  LEFT JOIN deal_path
    ON prep_crm_opportunity.deal_path = deal_path.deal_path_name
  LEFT JOIN sales_segment
    ON prep_crm_opportunity.sales_segment = sales_segment.sales_segment_name
  LEFT JOIN dr_partner_engagement
    ON prep_crm_opportunity.dr_partner_engagement = dr_partner_engagement.dr_partner_engagement_name
  LEFT JOIN channel_type
    ON prep_crm_opportunity.channel_type = channel_type.channel_type_name
  LEFT JOIN prep_date 
    ON prep_date.date_id = prep_crm_opportunity.close_date_id
  WHERE prep_crm_opportunity.is_live = 0

  {% if is_incremental() %}
  
    AND prep_crm_opportunity.snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

  {% endif %}


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-02-23",
    updated_date="2024-06-07"
) }}
