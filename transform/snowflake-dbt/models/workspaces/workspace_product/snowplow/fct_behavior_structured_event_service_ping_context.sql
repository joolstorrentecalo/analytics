{{
  config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=["mnpi_exception"]
  )
}}

WITH clicks AS (
  SELECT
    behavior_structured_event_pk,
    behavior_at,
    contexts
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2022-11-01' -- no events added to SP context before Nov 2022
  AND is_staging_event = FALSE
),

flattened AS (
  SELECT
    clicks.behavior_structured_event_pk,
    clicks.behavior_at,
    flat_contexts.value['data']['event_name']::VARCHAR AS redis_event_name,
    flat_contexts.value['data']['key_path']::VARCHAR AS key_path,
    flat_contexts.value['data']['data_source']::VARCHAR AS data_source
  FROM clicks,
  LATERAL FLATTEN(input => TRY_PARSE_JSON(clicks.contexts), path => 'data') AS flat_contexts
  WHERE flat_contexts.value['schema']::VARCHAR LIKE 'iglu:com.gitlab/gitlab_service_ping/jsonschema/%'
    {% if is_incremental() %}
    
        AND clicks.behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
    
    {% endif %}
)

{{ dbt_audit(
    cte_ref="flattened",
    created_by="@mdrussell",
    updated_by="@utkarsh060",
    created_date="2022-12-21",
    updated_date="2024-03-22"
) }}
