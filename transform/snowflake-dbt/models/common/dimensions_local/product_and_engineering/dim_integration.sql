{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_service_id"
    })
}}

WITH final AS (

    SELECT 
      dim_integration_sk,
      integration_id,
      dim_project_id,
      ultimate_parent_namespace_id,
      dim_plan_id,
      created_date_id,
      is_active,
      created_at,
      updated_at
    FROM {{ ref('prep_integration') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-07-27",
    updated_date="2023-07-27"
) }}