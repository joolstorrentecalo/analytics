{{ simple_cte([
    ('rpt_arr_snapshot_combined_8th_calendar_day','rpt_arr_snapshot_combined_8th_calendar_day'),
    ('rpt_arr_snapshot_combined_5th_calendar_day','rpt_arr_snapshot_combined_5th_calendar_day')
]) }},

final AS (

    SELECT
      arr_month,
      is_arr_month_finalized,
      fiscal_quarter_name_fy,
      fiscal_year,
      subscription_start_month,
      subscription_end_month,
      dim_billing_account_id,
      sold_to_country,
      billing_account_name,
      billing_account_number,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_geo,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      subscription_sales_type,
      product_name,
      product_name_grouped,
      product_rate_plan_name,
      product_rate_plan_charge_name,
      product_deployment_type,
      product_tier_name,
      product_delivery_type,
      product_ranking,
      service_type,
      unit_of_measure,
      mrr,
      arr,
      quantity,
      is_arpu,
      is_licensed_user,
      parent_account_cohort_month,
      months_since_parent_account_cohort_start,
      arr_band_calc,
      parent_crm_account_employee_count_band
    FROM rpt_arr_snapshot_combined_8th_calendar_day
    WHERE arr_month < '2024-03-01'

    UNION ALL

    SELECT
      arr_month,
      is_arr_month_finalized,
      fiscal_quarter_name_fy,
      fiscal_year,
      subscription_start_month,
      subscription_end_month,
      dim_billing_account_id,
      sold_to_country,
      billing_account_name,
      billing_account_number,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_geo,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      subscription_sales_type,
      product_name,
      product_name_grouped,
      product_rate_plan_name,
      product_rate_plan_charge_name,
      product_deployment_type,
      product_tier_name,
      product_delivery_type,
      product_ranking,
      service_type,
      unit_of_measure,
      mrr,
      arr,
      quantity,
      is_arpu,
      is_licensed_user,
      parent_account_cohort_month,
      months_since_parent_account_cohort_start,
      arr_band_calc,
      parent_crm_account_employee_count_band,
  -- add amounts for churn,expansion,contraction,new
      LAG(arr) 
        OVER (PARTITION BY dim_parent_crm_account_id, arr_month ORDER BY arr_month)
      END                                                                    AS prior_month_arr,
      arr - prior_month_arr                                                  AS month_customer_level_arr_change,
      LAG(arr) 
        OVER (PARTITION BY dim_parent_crm_account_id, fiscal_quarter_name_fy ORDER BY arr_month)
      END                                                                    AS prior_quarter_arr,
      arr - prior_quarter_arr                                                AS quarter_customer_level_arr_change,
      LAG(arr) 
        OVER (PARTITION BY dim_parent_crm_account_id, fiscal_year ORDER BY arr_month)
      END                                                                    AS prior_year_arr,
      arr - prior_year_arr                                                   AS year_customer_level_arr_change,
      CASE
        WHEN month_customer_level_arr_change IS NULL THEN 'New'
        WHEN prior_month_arr = 0 AND prior_month_arr >0 THEN  'Churn'
        WHEN month_customer_level_arr_change >0 AND prior_month_arr >0 THEN 'Expansion'
        WHEN month_customer_level_arr_change <0 prior_month_arr >0 THEN 'Contraction'
        WHEN month_customer_level_arr_change = 0 THEN 'No Impact'
        ELSE 'Other'
      END                                                                    AS month_arr_change_type,
            CASE
        WHEN quarter_customer_level_arr_change IS NULL THEN 'New'
        WHEN prior_quarter_arr = 0 AND prior_month_arr >0 THEN  'Churn'
        WHEN quarter_customer_level_arr_change >0 AND prior_quarter_arr >0 THEN 'Expansion'
        WHEN quarter_customer_level_arr_change <0 prior_quarter_arr >0 THEN 'Contraction'
        WHEN quarter_customer_level_arr_change = 0 THEN 'No Impact'
        ELSE 'Other'
      END                                                                    AS quarter_arr_change_type,
            CASE
        WHEN year_customer_level_arr_change IS NULL THEN 'New'
        WHEN prior_year_arr = 0 AND prior_year_arr >0 THEN  'Churn'
        WHEN year_customer_level_arr_change >0 AND prior_year_arr >0 THEN 'Expansion'
        WHEN year_customer_level_arr_change <0 prior_year_arr >0 THEN 'Contraction'
        WHEN year_customer_level_arr_change = 0 THEN 'No Impact'
        ELSE 'Other'
      END                                                                    AS year_arr_change_type
    FROM rpt_arr_snapshot_combined_5th_calendar_day
    WHERE arr_month >= '2024-03-01'
    
)

SELECT *
FROM final