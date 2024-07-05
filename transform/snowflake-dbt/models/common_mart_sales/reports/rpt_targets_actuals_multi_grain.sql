{{ config(
    tags=["mnpi_exception"]
) }}


WITH unioned AS (

  {{ union_tables(
    [
        ref('mart_crm_opportunity_7th_day_weekly_snapshot'),
        ref('mart_crm_opportunity_7th_day_weekly_snapshot_aggregate'),
        ref('mart_targets_actuals_7th_day_weekly_snapshot')
    ],
    filters={
        'mart_crm_opportunity_7th_day_weekly_snapshot': 'is_current_snapshot_quarter = true',
        'mart_crm_opportunity_7th_day_weekly_snapshot_aggregate': 'is_current_snapshot_quarter = false'
    }
) }}

)

SELECT 
  unioned.*,
  NULL AS day_of_week,
  NULL AS first_day_of_fiscal_year,
  NULL AS first_day_of_week,
  NULL AS fiscal_month_name_fy,
  NULL AS fiscal_quarter_name_fy,
  NULL AS last_day_of_fiscal_quarter,
  NULL AS last_day_of_fiscal_year,
  NULL AS last_day_of_month,
  NULL AS last_day_of_week,
  NULL AS dim_order_type_current_id,
  MAX(CASE 
        WHEN source != 'targets_actuals' AND snapshot_date < CURRENT_DATE() THEN snapshot_date 
        ELSE NULL 
    END) OVER ()                                                        AS max_snapshot_date, -- We want to ensure we have the max_snapshot_date that comes from the actuals in every row but excluding the future dates we have in the targets data 
  FLOOR((DATEDIFF(day, current_first_day_of_fiscal_quarter, max_snapshot_date) / 7)) 
                                                                        AS most_recent_snapshot_week
FROM unioned 
WHERE snapshot_fiscal_quarter_date >= DATEADD(QUARTER, -9, CURRENT_DATE())

