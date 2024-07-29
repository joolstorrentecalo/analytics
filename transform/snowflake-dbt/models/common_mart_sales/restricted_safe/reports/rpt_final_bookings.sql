{{ simple_cte([
    ('live_actuals', 'mart_crm_opportunity'),
    ('dim_date', 'dim_date'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_order_type','dim_order_type'),
    ('dim_deal_path', 'dim_deal_path')
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
    fiscal_quarter_name_fy         AS snapshot_fiscal_quarter_name,
    fiscal_quarter_name_fy         AS snapshot_fiscal_quarter_name_fy,
    fiscal_quarter                 AS snapshot_fiscal_quarter,
    fiscal_year                    AS snapshot_fiscal_year,
    first_day_of_fiscal_quarter    AS snapshot_first_day_of_fiscal_quarter,
    last_day_of_fiscal_quarter     AS snapshot_last_day_of_fiscal_quarter,
    first_day_of_fiscal_year       AS snapshot_first_day_of_fiscal_year,
    last_day_of_fiscal_year        AS snapshot_last_day_of_fiscal_year,
    first_day_of_fiscal_quarter    AS snapshot_fiscal_quarter_date,
    fiscal_quarter_number_absolute AS snapshot_fiscal_quarter_number_absolute,
    last_month_of_fiscal_quarter   AS snapshot_last_month_of_fiscal_quarter,
    last_month_of_fiscal_year      AS snapshot_last_month_of_fiscal_year,
    days_in_fiscal_quarter_count   AS snapshot_days_in_fiscal_quarter_count,
    dim_sales_qualified_source.sales_qualified_source_name,
    dim_sales_qualified_source.sales_qualified_source_grouped,
    dim_order_type.order_type_name,
    dim_order_type.order_type_grouped,
    dim_deal_path.deal_path_name,
    sales_type.sales_type,
    stage_name.stage_name
  FROM dim_date
  INNER JOIN dim_sales_qualified_source
  INNER JOIN dim_order_type
  INNER JOIN dim_deal_path
  INNER JOIN sales_type
  INNER JOIN stage_name
  WHERE current_first_day_of_fiscal_quarter > first_day_of_fiscal_quarter
    AND first_day_of_fiscal_quarter >= DATEADD(QUARTER, -9, CURRENT_DATE())

),

booked_arr AS (

  SELECT
    live_actuals.close_fiscal_quarter_date,
    live_actuals.sales_qualified_source_name,
    live_actuals.sales_qualified_source_grouped,
    live_actuals.order_type,
    live_actuals.order_type_grouped,
    live_actuals.deal_path_name,
    live_actuals.sales_type,
    live_actuals.stage_name,
    SUM(live_actuals.booked_net_arr) AS total_booked_arr
  FROM live_actuals
  {{ dbt_utils.group_by(n=8) }}

),

created_arr AS (

  SELECT
    live_actuals.arr_created_fiscal_quarter_date,
    live_actuals.sales_qualified_source_name,
    live_actuals.sales_qualified_source_grouped,
    live_actuals.order_type,
    live_actuals.order_type_grouped,
    live_actuals.deal_path_name,
    live_actuals.sales_type,
    live_actuals.stage_name,
    SUM(live_actuals.created_arr) AS total_created_arr
  FROM live_actuals
  {{ dbt_utils.group_by(n=8) }}
),

final AS (

  SELECT
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
    '94'                                           AS snapshot_day_of_fiscal_quarter, 
    base.sales_qualified_source_name               AS sales_qualified_source_live,
    base.sales_qualified_source_grouped            AS sales_qualified_source_grouped_live,
    base.order_type_name                           AS order_type_live,
    base.order_type_grouped                        AS order_type_grouped_live,
    base.deal_path_name                            AS deal_path_live,
    base.sales_type,
    base.stage_name, 
    'final_bookings'                               AS source,
    booked_arr.total_booked_arr                    AS booked_net_arr_in_snapshot_quarter,
    created_arr.total_created_arr                  AS created_arr_in_snapshot_quarter
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
  LEFT JOIN booked_arr
    ON base.snapshot_fiscal_quarter_date = booked_arr.close_fiscal_quarter_date
      AND base.sales_qualified_source_name = booked_arr.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = booked_arr.sales_qualified_source_grouped
      AND base.order_type_name = booked_arr.order_type
      AND base.order_type_grouped = booked_arr.order_type_grouped
      AND base.deal_path_name = booked_arr.deal_path_name
      AND base.sales_type = booked_arr.sales_type
      AND base.stage_name = booked_arr.stage_name
  WHERE (booked_arr.total_booked_arr IS NOT NULL OR created_arr.total_created_arr IS NOT NULL)

)

SELECT *
FROM final
