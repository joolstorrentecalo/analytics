{{ config(
    materialized='table'
) }}

WITH final AS (

    SELECT
        metric_day,
        metric_week,
        metric_quarter,
        metric_fiscal_year,
        segment,
        region,
        geo,
        country,
        is_first_order,
        sales_qualified_source_name,
        metric,
        target_value,
        COUNT(DISTINCT metric_value) AS aggregated_metric_value
    FROM {{ref('wk_rpt_core_marketing_metrics_raw')}}
    {{dbt_utils.group_by(n=12)}} 

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-09-03",
    updated_date="2024-09-03",
) }}