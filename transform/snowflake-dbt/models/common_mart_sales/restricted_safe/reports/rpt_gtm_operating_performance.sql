{{ simple_cte([
    ('rpt_scaffold','rpt_scaffold_sales_funnel'),
    ('dim_date','dim_date'),
    ('fct_sales_funnel_actual', 'fct_sales_funnel_actual'),
    ('fct_sales_funnel_target_daily', 'fct_sales_funnel_target_daily'),
    ('dim_sales_funnel_kpi', 'dim_sales_funnel_kpi'),
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
    ('dim_order_type', 'dim_order_type'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source')
]) }}

, actuals_day_aggregate AS (
    SELECT
      fct_sales_funnel_actual.actual_date_id,
      fct_sales_funnel_actual.dim_hierarchy_sk,
      fct_sales_funnel_actual.dim_order_type_id,
      fct_sales_funnel_actual.dim_sales_qualified_source_id,
      fct_sales_funnel_actual.dim_sales_funnel_kpi_sk,  
      SUM(fct_sales_funnel_actual.net_arr)                                  AS net_arr,
      SUM(fct_sales_funnel_actual.new_logo_count)                           AS new_logo_count,
      COUNT(DISTINCT dim_crm_opportunity_id)                                AS sao_count,
      COUNT(DISTINCT (
        CASE WHEN new_logo_count >= 0 THEN dim_crm_opportunity_id END)) 
      -
      COUNT(DISTINCT (
        CASE WHEN new_logo_count = -1 THEN dim_crm_opportunity_id END))     AS deal_count
    FROM  fct_sales_funnel_actual
    {{ dbt_utils.group_by(n=5) }}
)

, scaffold AS (
    SELECT
      -- date details
      dim_date.date_actual,
      dim_date.fiscal_year,
      dim_date.fiscal_quarter_name_fy,
      dim_date.first_day_of_year,
      dim_date.day_of_month,
      dim_date.day_of_fiscal_quarter,
      dim_date.day_of_year,
      dim_date.day_of_fiscal_year,
      dim_date.first_day_of_fiscal_quarter,
      dim_date.first_day_of_fiscal_year,
      -- foreign  keys
      rpt_scaffold.dim_sales_funnel_kpi_sk,
      rpt_scaffold.dim_hierarchy_sk,
      rpt_scaffold.dim_order_type_id,
      rpt_scaffold.dim_sales_qualified_source_id,
      -- targets
      fct_sales_funnel_target_daily.target_date,
      fct_sales_funnel_target_daily.report_target_date,
      fct_sales_funnel_target_daily.daily_allocated_target,
      fct_sales_funnel_target_daily.mtd_allocated_target,
      fct_sales_funnel_target_daily.qtd_allocated_target,
      fct_sales_funnel_target_daily.ytd_allocated_target,
      target_qtd.target_date                                AS ttd_target_date,
      target_qtd.report_target_date                         AS ttd_report_target_date,
      target_qtd.daily_allocated_target                     AS ttd_daily_allocated_target,
      target_qtd.mtd_allocated_target                       AS ttd_mtd_allocated_target,
      target_qtd.qtd_allocated_target                       AS ttd_qtd_allocated_target,
      target_qtd.ytd_allocated_target                       AS ttd_ytd_allocated_target,
      -- measures
      actuals_day_aggregate.net_arr,
      actuals_day_aggregate.new_logo_count,
      actuals_day_aggregate.sao_count,
      actuals_day_aggregate.deal_count
    FROM rpt_scaffold
    INNER JOIN dim_date 
      ON rpt_scaffold.date_id = dim_date.date_id
    LEFT JOIN actuals_day_aggregate
      ON rpt_scaffold.date_id = actuals_day_aggregate.actual_date_id
      AND rpt_scaffold.dim_hierarchy_sk = actuals_day_aggregate.dim_hierarchy_sk
      AND rpt_scaffold.dim_order_type_id = actuals_day_aggregate.dim_order_type_id
      AND rpt_scaffold.dim_sales_qualified_source_id = actuals_day_aggregate.dim_sales_qualified_source_id
      AND rpt_scaffold.dim_sales_funnel_kpi_sk = actuals_day_aggregate.dim_sales_funnel_kpi_sk
    LEFT JOIN fct_sales_funnel_target_daily
      ON rpt_scaffold.date_id = fct_sales_funnel_target_daily.target_date_id
      AND rpt_scaffold.dim_hierarchy_sk = fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk
      AND rpt_scaffold.dim_order_type_id = fct_sales_funnel_target_daily.dim_order_type_id
      AND rpt_scaffold.dim_sales_qualified_source_id = fct_sales_funnel_target_daily.dim_sales_qualified_source_id
      AND rpt_scaffold.dim_sales_funnel_kpi_sk = fct_sales_funnel_target_daily.dim_sales_funnel_kpi_sk
    LEFT JOIN fct_sales_funnel_target_daily AS target_qtd
      ON dim_date.date_actual = target_qtd.report_target_date
      AND rpt_scaffold.dim_hierarchy_sk = target_qtd.dim_crm_user_hierarchy_sk
      AND rpt_scaffold.dim_order_type_id = target_qtd.dim_order_type_id
      AND rpt_scaffold.dim_sales_qualified_source_id = target_qtd.dim_sales_qualified_source_id
      AND rpt_scaffold.dim_sales_funnel_kpi_sk = target_qtd.dim_sales_funnel_kpi_sk
)

, final AS (
    SELECT
      -- date details
      scaffold.date_actual,
      scaffold.day_of_month,
      scaffold.day_of_fiscal_quarter,
      scaffold.day_of_year,
      scaffold.day_of_fiscal_year,
      scaffold.first_day_of_year,
      scaffold.first_day_of_fiscal_quarter,
      scaffold.first_day_of_fiscal_year,
      scaffold.fiscal_year,
      scaffold.fiscal_quarter_name_fy,
      
      -- logical info
      dim_sales_funnel_kpi.sales_funnel_kpi_name,
      dim_crm_user_hierarchy.crm_user_role_name,
      dim_crm_user_hierarchy.crm_user_role_level_1,
      dim_crm_user_hierarchy.crm_user_role_level_2,
      dim_crm_user_hierarchy.crm_user_role_level_3,
      dim_crm_user_hierarchy.crm_user_role_level_4,
      dim_crm_user_hierarchy.crm_user_role_level_5,
      dim_crm_user_hierarchy.crm_user_area,
      dim_crm_user_hierarchy.crm_user_business_unit,
      dim_crm_user_hierarchy.crm_user_geo,
      dim_crm_user_hierarchy.crm_user_region,
      dim_crm_user_hierarchy.crm_user_sales_segment,
      dim_order_type.order_type_name,
      dim_order_type.order_type_grouped,
      dim_sales_qualified_source.sales_qualified_source_name,
      
      -- measures
      scaffold.net_arr,
      scaffold.new_logo_count,
      scaffold.sao_count,
      scaffold.deal_count,

      -- targets
      scaffold.target_date,
      scaffold.report_target_date,
      scaffold.daily_allocated_target,
      scaffold.mtd_allocated_target,
      scaffold.qtd_allocated_target,
      scaffold.ytd_allocated_target,
      scaffold.ttd_target_date,
      scaffold.ttd_report_target_date,
      scaffold.ttd_daily_allocated_target,
      scaffold.ttd_mtd_allocated_target,
      scaffold.ttd_qtd_allocated_target,
      scaffold.ttd_ytd_allocated_target

    FROM scaffold
    INNER JOIN dim_sales_funnel_kpi
      ON scaffold.dim_sales_funnel_kpi_sk = dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk
    LEFT JOIN dim_crm_user_hierarchy
      ON scaffold.dim_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
    INNER JOIN dim_order_type
      ON scaffold.dim_order_type_id = dim_order_type.dim_order_type_id
    INNER JOIN dim_sales_qualified_source
      ON scaffold.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
)

SELECT 
   *
FROM final
