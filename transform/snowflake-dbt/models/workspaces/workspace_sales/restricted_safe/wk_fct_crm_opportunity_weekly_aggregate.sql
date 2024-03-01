WITH actuals AS (

  SELECT * 
  FROM {{ ref('wk_fct_crm_opportunity_5th_day_weekly_snapshot') }}

),

aggregate_data AS (

  SELECT

    --dates
    snapshot_date,

    -- attributes
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_order_type_live_id,
    dim_crm_opp_owner_stamped_hierarchy_sk,
    sales_qualified_source_name,
    order_type,
    order_type_live,
    order_type_grouped,
    stage_name,
    deal_path_name,
    sales_type,
    calculated_deal_size,
    deal_size,
  

    -- numbers for current week
    SUM(closed_lost_opps_in_snapshot_week)                AS closed_lost_opps_in_snapshot_week,
    SUM(closed_won_opps_in_snapshot_week)                 AS closed_won_opps_in_snapshot_week,
    SUM(closed_opps_in_snapshot_week)                     AS closed_opps_in_snapshot_week,
    SUM(open_pipeline_in_snapshot_week)                   AS open_pipeline_in_snapshot_week,
    SUM(pipeline_created_in_snapshot_week)                AS pipeline_created_in_snapshot_week,
    SUM(created_arr_in_snapshot_week)                     AS created_arr_in_snapshot_week,
    SUM(created_net_arr_in_snapshot_week)                 AS created_net_arr_in_snapshot_week,
    SUM(created_deal_count_in_snapshot_week)              AS created_deal_count_in_snapshot_week,
    SUM(closed_net_arr_in_snapshot_week)                  AS closed_net_arr_in_snapshot_week,
    SUM(closed_deal_count_in_snapshot_week)               AS closed_deal_count_in_snapshot_week,
    SUM(closed_new_logo_count_in_snapshot_week)           AS closed_new_logo_count_in_snapshot_week,
    SUM(closed_cycle_time_in_snapshot_week)               AS closed_cycle_time_in_snapshot_week,
    SUM(booked_net_arr_in_snapshot_week)                  AS booked_net_arr_in_snapshot_week,
    SUM(calculated_deal_count_in_snapshot_week)           AS calculated_deal_count_in_snapshot_week,

    -- Additive fields
    SUM(segment_order_type_iacv_to_net_arr_ratio)         AS segment_order_type_iacv_to_net_arr_ratio,
    SUM(calculated_from_ratio_net_arr)                    AS calculated_from_ratio_net_arr,
    SUM(net_arr)                                          AS net_arr,
    SUM(raw_net_arr)                                      AS raw_net_arr,
    SUM(created_and_won_same_quarter_net_arr_combined)    AS created_and_won_same_quarter_net_arr_combined,
    SUM(new_logo_count)                                   AS new_logo_count,
    SUM(amount)                                           AS amount,
    SUM(recurring_amount)                                 AS recurring_amount,
    SUM(true_up_amount)                                   AS true_up_amount,
    SUM(proserv_amount)                                   AS proserv_amount,
    SUM(other_non_recurring_amount)                       AS other_non_recurring_amount,
    SUM(arr_basis)                                        AS arr_basis,
    SUM(arr)                                              AS arr,
    SUM(count_crm_attribution_touchpoints)                AS count_crm_attribution_touchpoints,
    SUM(weighted_linear_iacv)                             AS weighted_linear_iacv,
    SUM(count_campaigns)                                  AS count_campaigns,
    SUM(probability)                                      AS probability,
    SUM(days_in_sao)                                      AS days_in_sao,
    SUM(open_1plus_deal_count)                            AS open_1plus_deal_count,
    SUM(open_3plus_deal_count)                            AS open_3plus_deal_count,
    SUM(open_4plus_deal_count)                            AS open_4plus_deal_count,
    SUM(booked_deal_count)                                AS booked_deal_count,
    SUM(churned_contraction_deal_count)                   AS churned_contraction_deal_count,
    SUM(open_1plus_net_arr)                               AS open_1plus_net_arr,
    SUM(open_3plus_net_arr)                               AS open_3plus_net_arr,
    SUM(open_4plus_net_arr)                               AS open_4plus_net_arr,
    SUM(booked_net_arr)                                   AS booked_net_arr,
    SUM(churned_contraction_net_arr)                      AS churned_contraction_net_arr,
    SUM(calculated_deal_count)                            AS calculated_deal_count,
    SUM(booked_churned_contraction_deal_count)            AS booked_churned_contraction_deal_count,
    SUM(booked_churned_contraction_net_arr)               AS booked_churned_contraction_net_arr,
    SUM(renewal_amount)                                   AS renewal_amount,
    SUM(total_contract_value)                             AS total_contract_value,
    SUM(days_in_stage)                                    AS days_in_stage,
    SUM(calculated_age_in_days)                           AS calculated_age_in_days,
    SUM(days_since_last_activity)                         AS days_since_last_activity,
    SUM(pre_military_invasion_arr)                        AS pre_military_invasion_arr,
    SUM(won_arr_basis_for_clari)                          AS won_arr_basis_for_clari,
    SUM(arr_basis_for_clari)                              AS arr_basis_for_clari,
    SUM(forecasted_churn_for_clari)                       AS forecasted_churn_for_clari,
    SUM(override_arr_basis_clari)                         AS override_arr_basis_clari,
    SUM(vsa_start_date_net_arr)                           AS vsa_start_date_net_arr,
    SUM(cycle_time_in_days_combined)                      AS cycle_time_in_days_combined
  FROM actuals
  GROUP BY ALL

)

SELECT * 
FROM aggregate_data
WHERE NOT is_current_snapshot_quarter
