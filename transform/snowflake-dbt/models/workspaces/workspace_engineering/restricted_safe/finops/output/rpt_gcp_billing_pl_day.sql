{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_infra_mapping_day') }}


),

combined_pl_mapping AS (

  SELECT * FROM {{ ref('combined_pl_mapping') }}

),

overlaps AS (

  SELECT
    service_base.day                                                    AS date_day,
    service_base.gcp_project_id,
    service_base.gcp_service_description,
    service_base.gcp_sku_description,
    service_base.infra_label,
    combined_pl_mapping.pl_category,
    service_base.usage_unit,
    service_base.pricing_unit,
    service_base.usage_amount,
    service_base.usage_amount_in_pricing_units,
    service_base.cost_before_credits,
    service_base.net_cost * COALESCE(combined_pl_mapping.pl_percent, 1) AS net_cost,
    combined_pl_mapping.from_mapping,
    DENSE_RANK() OVER (PARTITION BY service_base.day,
      service_base.gcp_project_id,
      service_base.gcp_service_description,
      service_base.gcp_sku_description,
      service_base.infra_label
      ORDER BY
        (CASE WHEN combined_pl_mapping.gcp_project_id IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.gcp_service_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.gcp_sku_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.infra_label IS NOT NULL THEN 1 ELSE 0 END) DESC
    )                                                                   AS priority
  FROM
    service_base
  LEFT JOIN combined_pl_mapping ON combined_pl_mapping.date_day = service_base.day
    AND COALESCE(combined_pl_mapping.gcp_project_id, service_base.gcp_project_id) = service_base.gcp_project_id
    AND COALESCE(combined_pl_mapping.gcp_service_description, service_base.gcp_service_description) = service_base.gcp_service_description
    AND COALESCE(combined_pl_mapping.gcp_sku_description, service_base.gcp_sku_description) = service_base.gcp_sku_description
    AND COALESCE(combined_pl_mapping.infra_label, service_base.infra_label) = service_base.infra_label
)

SELECT * EXCLUDE(priority) FROM overlaps WHERE priority = 1



