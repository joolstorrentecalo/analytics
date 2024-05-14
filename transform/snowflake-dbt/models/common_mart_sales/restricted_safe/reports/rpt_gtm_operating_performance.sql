{{ simple_cte([
    ('rpt_scaffold','rpt_scaffold_sales_funnel'),
    ('dim_date','dim_date'),
    ('fct_sales_funnel_actual', 'fct_sales_funnel_actual'),
    ('fct_sales_funnel_target_daily', 'fct_sales_funnel_target_daily'),
    ('dim_sales_funnel_kpi', 'dim_sales_funnel_kpi'),
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
    ('dim_order_type', 'dim_order_type'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('mart_crm_opportunity', 'mart_crm_opportunity')
]) }}

, scaffold AS (
    SELECT
      dim_date.date_actual,
      dim_date.fiscal_year,
      dim_date.fiscal_quarter_name_fy,
      rpt_scaffold.dim_sales_funnel_kpi_sk,
      rpt_scaffold.dim_hierarchy_sk,
      rpt_scaffold.dim_order_type_id,
      rpt_scaffold.dim_sales_qualified_source_id,
      fct_sales_funnel_actual.dim_crm_opportunity_id,
      fct_sales_funnel_actual.net_arr,
      fct_sales_funnel_actual.new_logo_count,
      fct_sales_funnel_target_daily.daily_allocated_target
    FROM rpt_scaffold
    INNER JOIN dim_date on rpt_scaffold.date_id = dim_date.date_id
    LEFT JOIN fct_sales_funnel_actual
      ON rpt_scaffold.date_id = fct_sales_funnel_actual.actual_date_id
      AND rpt_scaffold.dim_hierarchy_sk = fct_sales_funnel_actual.dim_hierarchy_sk
      AND rpt_scaffold.dim_order_type_id = fct_sales_funnel_actual.dim_order_type_id
      AND rpt_scaffold.dim_sales_qualified_source_id = fct_sales_funnel_actual.dim_sales_qualified_source_id
      AND rpt_scaffold.dim_sales_funnel_kpi_sk = fct_sales_funnel_actual.dim_sales_funnel_kpi_sk
    LEFT JOIN fct_sales_funnel_target_daily
      ON rpt_scaffold.date_id = fct_sales_funnel_target_daily.target_date_id
      AND rpt_scaffold.dim_hierarchy_sk = fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk
      AND rpt_scaffold.dim_order_type_id = fct_sales_funnel_target_daily.dim_order_type_id
      AND rpt_scaffold.dim_sales_qualified_source_id = fct_sales_funnel_target_daily.dim_sales_qualified_source_id
      AND rpt_scaffold.dim_sales_funnel_kpi_sk = fct_sales_funnel_target_daily.dim_sales_funnel_kpi_sk
)

, granular AS (
    SELECT
      scaffold.date_actual,
      scaffold.fiscal_year,
      scaffold.fiscal_quarter_name_fy,
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
      dim_sales_qualified_source.sales_qualified_source_name,
      CASE WHEN mart_crm_opportunity.product_category = 'Dedicated - Ultimate' THEN 'Dedicated - Ultimate'
        WHEN mart_crm_opportunity.product_category IN ('Self-Managed - Premium', 'SaaS - Premium','Premium - 1 Year','Premium') THEN 'Premium'
        WHEN mart_crm_opportunity.product_category IN ('Ultimate' , 'Self-Managed - Ultimate' , 'SaaS - Ultimate') THEN 'Ultimate'
        ELSE 'Other' 
      END                                                           AS product_category_modified,
      CASE WHEN scaffold.new_logo_count >= 0
        THEN scaffold.dim_crm_opportunity_id 
      END                                                           AS first_number_deals,
      CASE WHEN scaffold.new_logo_count = -1
        THEN scaffold.dim_crm_opportunity_id 
      END                                                           AS second_number_deals,
      scaffold.net_arr,
      scaffold.new_logo_count,
      scaffold.daily_allocated_target,
      scaffold.dim_crm_opportunity_id
FROM scaffold
INNER JOIN dim_sales_funnel_kpi
  ON scaffold.dim_sales_funnel_kpi_sk = dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk
INNER JOIN dim_crm_user_hierarchy
  ON scaffold.dim_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
INNER JOIN dim_order_type
  ON scaffold.dim_order_type_id = dim_order_type.dim_order_type_id
INNER JOIN dim_sales_qualified_source
  ON scaffold.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
INNER JOIN mart_crm_opportunity
  ON scaffold.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
)

SELECT 
   *
FROM granular