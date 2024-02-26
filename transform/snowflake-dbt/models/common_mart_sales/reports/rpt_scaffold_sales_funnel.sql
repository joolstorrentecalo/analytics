{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('fct_sales_funnel_target_daily', 'fct_sales_funnel_target_daily'),
    ('fct_sales_funnel_actual', 'fct_sales_funnel_actual')
]) }}

SELECT
  actual_date_id                  AS date_id,
  dim_sales_funnel_kpi_sk,
  dim_hierarchy_sk,
  dim_order_type_id,
  dim_sales_qualified_source_id
FROM fct_sales_funnel_actual

UNION

SELECT 
  target_date_id,
  dim_sales_funnel_kpi_sk,
  dim_crm_user_hierarchy_sk,
  dim_order_type_id,
  dim_sales_qualified_source_id
FROM fct_sales_funnel_target_daily
