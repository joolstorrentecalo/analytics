{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_sales_funnel_target AS (

    SELECT kpi_name
    FROM {{ ref('prep_sales_funnel_target') }}

), prep_sales_funnel_partner_alliance_target AS (

    SELECT kpi_name
    FROM {{ ref('prep_sales_funnel_partner_alliance_target') }}

), unioned AS (

    SELECT kpi_name
    FROM prep_sales_funnel_target

    UNION

    SELECT kpi_name
    fROM prep_sales_funnel_partner_alliance_target

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['kpi_name'])}} AS dim_sales_funnel_kpi_sk,
  kpi_name                                   AS sales_funnel_kpi_name
FROM unioned
