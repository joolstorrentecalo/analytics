{{ simple_cte([
    ('dim_crm_user_hierarchy', 'wk_dim_crm_user_hierarchy'),
    ('dim_order_type','dim_order_type'),
    ('fct_sales_funnel_target', 'wk_fct_sales_funnel_partner_alliance_target'),
    ('dim_alliance_type', 'dim_alliance_type_scd'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_channel_type', 'dim_channel_type')
]) }}

, final AS (

    SELECT
      fct_sales_funnel_target.sales_funnel_partner_alliance_target_id,
      fct_sales_funnel_target.first_day_of_month      AS target_month,
      fct_sales_funnel_target.kpi_name,
      dim_crm_user_hierarchy.crm_user_sales_segment,
      dim_crm_user_hierarchy.crm_user_sales_segment_grouped,
      dim_crm_user_hierarchy.crm_user_geo,
      dim_crm_user_hierarchy.crm_user_region,
      dim_crm_user_hierarchy.crm_user_area,
      dim_crm_user_hierarchy.crm_user_sales_segment_region_grouped,
      dim_order_type.order_type_name,
      dim_order_type.order_type_grouped,
      dim_sales_qualified_source.sales_qualified_source_name,
      dim_sales_qualified_source.sqs_bucket_engagement,
      dim_channel_type.channel_type_name,
      dim_alliance_type.alliance_type_name,
      dim_alliance_type.alliance_type_short_name,
      fct_sales_funnel_target.allocated_target
    FROM fct_sales_funnel_target
    LEFT JOIN dim_alliance_type
      ON fct_sales_funnel_target.dim_alliance_type_id = dim_alliance_type.dim_alliance_type_id
    LEFT JOIN dim_sales_qualified_source
      ON fct_sales_funnel_target.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_channel_type
      ON fct_sales_funnel_target.dim_channel_type_id = dim_channel_type.dim_channel_type_id
    LEFT JOIN dim_order_type
      ON fct_sales_funnel_target.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_user_hierarchy
      ON fct_sales_funnel_target.dim_crm_user_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
        AND fct_sales_funnel_target.fiscal_year = dim_crm_user_hierarchy.fiscal_year

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-02-01",
    updated_date="2023-02-01",
  ) }}
