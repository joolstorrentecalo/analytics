WITH targets AS (

  SELECT *
  FROM {{ ref('fct_sales_funnel_target_daily') }}

),

actuals AS (

  SELECT *
  FROM {{ ref('fct_crm_opportunity_daily_snapshot') }}

),

prep_date AS (

  SELECT *
  FROM {{ ref('prep_date') }}


),

spine AS (

  {{ date_spine_7th_day() }}

),

total_targets AS (

  SELECT
    prep_date.fiscal_quarter_name_fy      AS fiscal_quarter_name,
    prep_date.first_day_of_fiscal_quarter AS fiscal_quarter_date,
    targets.dim_crm_user_hierarchy_sk     AS dim_crm_current_account_set_hierarchy_sk,
    targets.dim_sales_qualified_source_id,
    targets.dim_order_type_id,
    SUM(daily_allocated_target)           AS total_quarter_target
  FROM targets
  LEFT JOIN prep_date
    ON targets.target_date_id = prep_date.date_id
  WHERE kpi_name = 'Net ARR'
  {{ dbt_utils.group_by(n=5) }}
),


daily_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.snapshot_date,
    actuals.snapshot_id,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,
    SUM(booked_net_arr_in_snapshot_quarter)          AS booked_net_arr_in_snapshot_quarter,
    SUM(open_1plus_net_arr_in_snapshot_quarter)      AS open_1plus_net_arr_in_snapshot_quarter,
    SUM(open_3plus_net_arr_in_snapshot_quarter)      AS open_3plus_net_arr_in_snapshot_quarter,
    SUM(open_4plus_net_arr_in_snapshot_quarter)      AS open_4plus_net_arr_in_snapshot_quarter

  FROM actuals
  {{ dbt_utils.group_by(n=7) }}
),

quarterly_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,
    SUM(actuals.booked_net_arr_in_snapshot_quarter)  AS total_booked_net_arr
  FROM actuals
  WHERE snapshot_date = snapshot_last_day_of_fiscal_quarter
  {{ dbt_utils.group_by(n=5) }}

),

combined_data AS ( 

    SELECT 
        dim_crm_current_account_set_hierarchy_sk,
        dim_sales_qualified_source_id,
        dim_order_type_id,
        fiscal_quarter_name,
        fiscal_quarter_date
    FROM total_targets

    UNION

    SELECT 
        dim_crm_current_account_set_hierarchy_sk,
        dim_sales_qualified_source_id,
        dim_order_type_id,
        snapshot_fiscal_quarter_name,
        snapshot_fiscal_quarter_date
    FROM quarterly_actuals

    UNION 

    SELECT 
        dim_crm_current_account_set_hierarchy_sk,
        dim_sales_qualified_source_id,
        dim_order_type_id,
        snapshot_fiscal_quarter_name,
        snapshot_fiscal_quarter_date
    FROM daily_actuals

),

base AS (

  /*
    Cross join all dimensions (hierarchy, qualified source, order type) and
    the dates to create a comprehensive set of all possible combinations of these dimensions and dates.
    This exhaustive combination is essential for scenarios where we need to account for all possible configurations in our analysis,
    ensuring that no combination is overlooked.

    When we eventually join this set of combinations with the quarterly actuals,
    it ensures that even the newly introduced dimensions are accounted for.
  */

  SELECT   
    combined_data.dim_crm_current_account_set_hierarchy_sk,
    combined_data.dim_sales_qualified_source_id,
    combined_data.dim_order_type_id,     
    spine.date_id,
    spine.day_7 as date_actual,
    combined_data.fiscal_quarter_date,
    combined_data.fiscal_quarter_name
  FROM combined_data
  INNER JOIN spine
      ON combined_data.fiscal_quarter_name = spine.fiscal_quarter_name

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['base.date_id', 'base.dim_crm_current_account_set_hierarchy_sk', 'base.dim_order_type_id','base.dim_sales_qualified_source_id']) }} AS targets_actuals_weekly_snapshot_pk,
    base.date_id,
    base.date_actual,
    base.fiscal_quarter_name,
    base.fiscal_quarter_date,
    base.dim_crm_current_account_set_hierarchy_sk,
    base.dim_order_type_id,
    base.dim_sales_qualified_source_id,
    SUM(total_targets.total_quarter_target)                   AS total_quarter_target,
    SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)     AS coverage_booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter) AS coverage_open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr_in_snapshot_quarter) AS coverage_open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr_in_snapshot_quarter) AS coverage_open_4plus_net_arr,
    SUM(quarterly_actuals.total_booked_net_arr)               AS total_booked_net_arr
  FROM base
  LEFT JOIN total_targets
    ON base.fiscal_quarter_name = total_targets.fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = total_targets.dim_sales_qualified_source_id
      AND base.dim_crm_current_account_set_hierarchy_sk = total_targets.dim_crm_current_account_set_hierarchy_sk
      AND base.dim_order_type_id = total_targets.dim_order_type_id
  LEFT JOIN daily_actuals
    ON base.date_id = daily_actuals.snapshot_id
      AND base.dim_sales_qualified_source_id = daily_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_current_account_set_hierarchy_sk = daily_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.dim_order_type_id = daily_actuals.dim_order_type_id
  LEFT JOIN quarterly_actuals
    ON base.fiscal_quarter_name = quarterly_actuals.snapshot_fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = quarterly_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_current_account_set_hierarchy_sk = quarterly_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.dim_order_type_id = quarterly_actuals.dim_order_type_id
  {{ dbt_utils.group_by(n=8) }}

)

SELECT *
FROM final