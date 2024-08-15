{{ simple_cte([
    ('live_actuals', 'mart_crm_opportunity'),
    ('dim_date', 'dim_date'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_order_type','dim_order_type'),
    ('dim_deal_path', 'dim_deal_path'),
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy')

    ])

}},

sales_type AS (

  SELECT DISTINCT sales_type
  FROM live_actuals

),

stage_name AS (

  SELECT DISTINCT stage_name
  FROM live_actuals

),

base AS (

  SELECT DISTINCT
    dim_date.current_day_name,
    dim_date.current_date_actual,
    dim_date.current_fiscal_year,
    dim_date.current_first_day_of_fiscal_year,
    dim_date.current_fiscal_quarter_name_fy,
    dim_date.current_first_day_of_month,
    dim_date.current_first_day_of_fiscal_quarter,
    dim_date.current_day_of_month,
    dim_date.current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year,
    dim_date.current_first_day_of_week,
    dim_date.current_week_of_fiscal_quarter_normalised,
    dim_date.current_week_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy                                           AS snapshot_fiscal_quarter_name,
    dim_date.fiscal_quarter_name_fy                                           AS snapshot_fiscal_quarter_name_fy,
    dim_date.fiscal_quarter                                                   AS snapshot_fiscal_quarter,
    dim_date.fiscal_year                                                      AS snapshot_fiscal_year,
    dim_date.first_day_of_fiscal_quarter                                      AS snapshot_first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter                                       AS snapshot_last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year                                         AS snapshot_first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year                                          AS snapshot_last_day_of_fiscal_year,
    dim_date.first_day_of_fiscal_quarter                                      AS snapshot_fiscal_quarter_date,
    dim_date.fiscal_quarter_number_absolute                                   AS snapshot_fiscal_quarter_number_absolute,
    dim_date.last_month_of_fiscal_quarter                                     AS snapshot_last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year                                        AS snapshot_last_month_of_fiscal_year,
    dim_date.days_in_fiscal_quarter_count                                     AS snapshot_days_in_fiscal_quarter_count,
    dim_sales_qualified_source.sales_qualified_source_name,
    dim_sales_qualified_source.sales_qualified_source_grouped,
    dim_order_type.order_type_name,
    dim_order_type.order_type_grouped,
    dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk,
    dim_crm_user_hierarchy.crm_user_sales_segment,
    dim_crm_user_hierarchy.crm_user_sales_segment_grouped,
    dim_crm_user_hierarchy.crm_user_business_unit,
    dim_crm_user_hierarchy.crm_user_geo,
    dim_crm_user_hierarchy.crm_user_region,
    dim_crm_user_hierarchy.crm_user_area,
    dim_crm_user_hierarchy.crm_user_sales_segment_region_grouped,
    dim_crm_user_hierarchy.crm_user_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5,
    dim_deal_path.deal_path_name,
    sales_type.sales_type,
    stage_name.stage_name
  FROM dim_date
  INNER JOIN dim_sales_qualified_source
  INNER JOIN dim_order_type
  INNER JOIN dim_deal_path
  INNER JOIN sales_type
  INNER JOIN stage_name
  INNER JOIN dim_crm_user_hierarchy
  WHERE current_first_day_of_fiscal_quarter > first_day_of_fiscal_quarter
    AND first_day_of_fiscal_quarter >= DATEADD(QUARTER, -9, CURRENT_DATE())

),

booked_arr AS (

  SELECT
    live_actuals.dim_crm_current_account_set_hierarchy_sk,
    live_actuals.close_fiscal_quarter_date,
    live_actuals.sales_qualified_source_name,
    live_actuals.sales_qualified_source_grouped,
    live_actuals.order_type,
    live_actuals.order_type_grouped,
    live_actuals.deal_path_name,
    live_actuals.sales_type,
    live_actuals.stage_name,
    SUM(live_actuals.booked_net_arr)                                 AS total_booked_arr
  FROM live_actuals
  {{ dbt_utils.group_by(n=9) }}

),

created_arr AS (

  SELECT
    live_actuals.dim_crm_current_account_set_hierarchy_sk,
    live_actuals.arr_created_fiscal_quarter_date,
    live_actuals.sales_qualified_source_name,
    live_actuals.sales_qualified_source_grouped,
    live_actuals.order_type,
    live_actuals.order_type_grouped,
    live_actuals.deal_path_name,
    live_actuals.sales_type,
    live_actuals.stage_name,
    SUM(live_actuals.created_arr)                                    AS total_created_arr
  FROM live_actuals
  {{ dbt_utils.group_by(n=9) }}
),

final AS (

  SELECT
    base.dim_crm_user_hierarchy_sk                                   AS dim_crm_current_account_set_hierarchy_sk,
    base.crm_user_sales_segment                                      AS report_segment,
    base.crm_user_business_unit                                      AS report_business_unit,
    base.crm_user_geo                                                AS report_geo,
    base.crm_user_region                                             AS report_region,
    base.crm_user_area                                               AS report_area,
    base.crm_user_role_name                                          AS report_role_name,
    base.crm_user_role_level_1                                       AS report_role_level_1,
    base.crm_user_role_level_2                                       AS report_role_level_2,
    base.crm_user_role_level_3                                       AS report_role_level_3,
    base.crm_user_role_level_4                                       AS report_role_level_4,
    base.crm_user_role_level_5                                       AS report_role_level_5,
    base.current_day_name,
    base.current_date_actual,
    base.current_fiscal_year,
    base.current_first_day_of_fiscal_year,
    base.current_fiscal_quarter_name_fy,
    base.current_first_day_of_month,
    base.current_first_day_of_fiscal_quarter,
    base.current_day_of_month,
    base.current_day_of_fiscal_quarter,
    base.current_day_of_fiscal_year,
    base.current_first_day_of_week,
    base.current_week_of_fiscal_quarter_normalised,
    base.current_week_of_fiscal_quarter,
    base.snapshot_fiscal_quarter,
    base.snapshot_fiscal_year,
    base.snapshot_first_day_of_fiscal_year,
    base.snapshot_last_day_of_fiscal_year,
    base.snapshot_fiscal_quarter_name,
    base.snapshot_fiscal_quarter_name_fy,
    base.snapshot_fiscal_quarter_date,
    base.snapshot_fiscal_quarter_number_absolute,
    base.snapshot_last_month_of_fiscal_quarter,
    base.snapshot_last_month_of_fiscal_year,
    '94'                                                             AS snapshot_day_of_fiscal_quarter, 
    base.sales_qualified_source_name                                 AS sales_qualified_source_live,
    base.sales_qualified_source_grouped                              AS sales_qualified_source_grouped_live,
    base.order_type_name                                             AS order_type_live,
    base.order_type_grouped                                          AS order_type_grouped_live,
    base.deal_path_name                                              AS deal_path_live,
    base.sales_type,
    base.stage_name, 
    'final_bookings'                                                 AS source,
    booked_arr.total_booked_arr                                      AS booked_net_arr_in_snapshot_quarter,
    created_arr.total_created_arr                                    AS created_arr_in_snapshot_quarter
  FROM base
  LEFT JOIN created_arr
    ON base.snapshot_fiscal_quarter_date = created_arr.arr_created_fiscal_quarter_date
      AND base.sales_qualified_source_name = created_arr.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = created_arr.sales_qualified_source_grouped
      AND base.order_type_name = created_arr.order_type
      AND base.order_type_grouped = created_arr.order_type_grouped
      AND base.deal_path_name = created_arr.deal_path_name
      AND base.sales_type = created_arr.sales_type
      AND base.stage_name = created_arr.stage_name
      AND base.dim_crm_user_hierarchy_sk = created_arr.dim_crm_current_account_set_hierarchy_sk
  LEFT JOIN booked_arr
    ON base.snapshot_fiscal_quarter_date = booked_arr.close_fiscal_quarter_date
      AND base.sales_qualified_source_name = booked_arr.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = booked_arr.sales_qualified_source_grouped
      AND base.order_type_name = booked_arr.order_type
      AND base.order_type_grouped = booked_arr.order_type_grouped
      AND base.deal_path_name = booked_arr.deal_path_name
      AND base.sales_type = booked_arr.sales_type
      AND base.stage_name = booked_arr.stage_name
      AND base.dim_crm_user_hierarchy_sk = booked_arr.dim_crm_current_account_set_hierarchy_sk
  WHERE (booked_arr.total_booked_arr IS NOT NULL OR created_arr.total_created_arr IS NOT NULL)

)

SELECT *
FROM final
