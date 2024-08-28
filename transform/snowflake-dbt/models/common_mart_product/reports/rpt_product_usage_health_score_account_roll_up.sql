{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH final AS (

  SELECT DISTINCT
    reporting_month,
    dim_crm_account_id_mart_arr_all AS dim_crm_account_id,
    max_ping_created_at,
    account_arr_reporting_usage_data,
    account_ultimate_arr_reporting_usage_data,
    pct_of_account_arr_reporting_usage_data,
    pct_of_account_ultimate_arr_reporting_usage_data,
    account_weighted_license_utilization,
    account_weighted_user_engagement,
    account_weighted_scm_utilization,
    account_weighted_ci_utilization,
    account_weighted_cd_utilization,
    account_weighted_security_utilization,
    account_level_license_utilization_color,
    account_level_user_engagement_color,
    account_level_scm_color,
    account_level_ci_color,
    account_level_cd_color,
    account_level_security_color,
    CASE WHEN account_level_license_utilization_color = 'Green' THEN 88
      WHEN account_level_license_utilization_color = 'Yellow' THEN 63
      WHEN account_level_license_utilization_color = 'Red' THEN 25
    END                     AS gs_license_utilization_color_value,
    CASE WHEN account_level_user_engagement_color = 'Green' THEN 88
      WHEN account_level_user_engagement_color = 'Yellow' THEN 63
      WHEN account_level_user_engagement_color = 'Red' THEN 25
    END                     AS gs_user_engagement_color_value,
    CASE WHEN account_level_scm_color = 'Green' THEN 88
      WHEN account_level_scm_color = 'Yellow' THEN 63
      WHEN account_level_scm_color = 'Red' THEN 25
    END                     AS gs_scm_color_value,
    CASE WHEN account_level_ci_color = 'Green' THEN 88
      WHEN account_level_ci_color = 'Yellow' THEN 63
      WHEN account_level_ci_color = 'Red' THEN 25
    END                     AS gs_ci_color_value,
    CASE WHEN account_level_cd_color = 'Green' THEN 88
      WHEN account_level_cd_color = 'Yellow' THEN 63
      WHEN account_level_cd_color = 'Red' THEN 25
    END                     AS gs_cd_color_value,
    CASE WHEN account_level_security_color = 'Green' THEN 88
      WHEN account_level_security_color = 'Yellow' THEN 63
      WHEN account_level_security_color = 'Red' THEN 25
    END                     AS gs_security_color_value
  FROM {{ ref('rpt_product_usage_health_score_account_calcs') }}

)

SELECT *
FROM final
