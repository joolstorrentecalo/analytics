{{
  config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=["mnpi_exception", "product"]
  )
}}

WITH clicks AS (
  SELECT
    behavior_structured_event_pk,
    behavior_at,
    contexts
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2023-08-01' -- no events added to context before Aug 2023
),

flattened AS (
  SELECT
    clicks.behavior_structured_event_pk,
    clicks.behavior_at,
    flat_contexts.value                                                     AS code_suggestions_context,
    flat_contexts.value['data']['request_counts']['requests']::INT          AS requests,
    flat_contexts.value['data']['request_counts']['errors']::INT            AS errors,
    flat_contexts.value['data']['request_counts']['accepts']::INT           AS accepts,
    flat_contexts.value['data']['request_counts']['lang']::VARCHAR          AS lang,
    flat_contexts.value['data']['request_counts']['model_engine']::INT      AS model_engine,
    flat_contexts.value['data']['request_counts']['model_name']::INT        AS model_name,
    flat_contexts.value['data']['prefix_link']::INT                         AS prefix_link,
    flat_contexts.value['data']['suffix_length']::INT                       AS suffix_length,
    flat_contexts.value['data']['language']::VARCHAR                        AS language,
    flat_contexts.value['data']['user_agent']::VARCHAR                      AS user_agent,
    flat_contexts.value['data']['gitlab_realm']::VARCHAR                    AS delivery_type,
    flat_contexts.value['data']['api_status_code']::INT                     AS api_status_code
  FROM clicks,
  LATERAL FLATTEN(input => TRY_PARSE_JSON(clicks.contexts), path => 'data') AS flat_contexts
  WHERE flat_contexts.value['schema']::VARCHAR LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%'
    {% if is_incremental() %}
    
        AND clicks.behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
    
    {% endif %}
)

{{ dbt_audit(
    cte_ref="flattened",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2023-09-25",
    updated_date="2023-09-25"
) }}
