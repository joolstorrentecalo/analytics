{{ config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    cluster_by=['behavior_at::DATE']
  ) 

}}

{{ simple_cte([
    ('fct_behavior_structured_event_code_suggestions_context', 'fct_behavior_structured_event_code_suggestions_context'),
    ('fct_behavior_structured_event_ide_extension_version', 'fct_behavior_structured_event_ide_extension_version'),
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('dim_behavior_event', 'dim_behavior_event')
]) }},

code_suggestions_context AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_behavior_structured_event_code_suggestions_context'), except=["CREATED_BY", 
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_behavior_structured_event_code_suggestions_context
),

ide_extension_version_context AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_behavior_structured_event_ide_extension_version'), except=["BEHAVIOR_AT", 
    "CREATED_BY", "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_behavior_structured_event_ide_extension_version

),

joined_code_suggestions_contexts AS (

  /*
  All Code Suggestions-related events have the code_suggestions_context, but only a subset 
  have the ide_extension_version_context.
  */

  SELECT
    code_suggestions_context.*,
    ide_extension_version_context.ide_extension_version_context,
    ide_extension_version_context.extension_name,
    ide_extension_version_context.extension_version,
    ide_extension_version_context.ide_name,
    ide_extension_version_context.ide_vendor,
    ide_extension_version_context.ide_version
  FROM code_suggestions_context
  LEFT JOIN ide_extension_version_context
    ON code_suggestions_context.behavior_structured_event_pk = ide_extension_version_context.behavior_structured_event_pk

),

code_suggestions_joined_to_fact_and_dim AS (

  SELECT
    joined_code_suggestions_contexts.*,
    fct_behavior_structured_event.app_id,
    fct_behavior_structured_event.contexts,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_label,
    dim_behavior_event.event_property
  FROM joined_code_suggestions_contexts
  INNER JOIN fct_behavior_structured_event
    ON joined_code_suggestions_contexts.behavior_structured_event_pk = fct_behavior_structured_event.behavior_structured_event_pk
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  WHERE fct_behavior_structured_event.behavior_at >= '2023-08-28' --first day with events

),

filtered_code_suggestion_events AS (

  SELECT
    behavior_structured_event_pk,
    behavior_at,
    behavior_at::DATE AS behavior_date,
    app_id,
    event_category,
    event_action,
    event_label,
    event_property,
    language,
    delivery_type,
    model_engine,
    model_name,
    prefix_length,
    suffix_length,
    user_agent,
    api_status_code,
    extension_name,
    extension_version,
    ide_name,
    ide_vendor,
    ide_version,
    contexts,
    code_suggestions_context,
    ide_extension_version_context
  FROM code_suggestions_joined_to_fact_and_dim
  WHERE app_id IN ('gitlab_ai_gateway', 'gitlab_ide_extension') --"official" Code Suggestions app_ids
    --Need to exclude VS Code 3.76.0 (which sent duplicate events)
    AND user_agent NOT LIKE '%3.76.0 VSCode%' --exclude events which carry the version in user_agent from the code_suggestions_context
    AND IFF(ide_name = 'Visual Studio Code', extension_version != '3.76.0')--exclude events from with version from the ide_extension_version context

)

{{ dbt_audit(
    cte_ref="filtered_code_suggestion_events",
    created_by="@cbraza",
    updated_by="@cbraza",
    created_date="2023-10-09",
    updated_date="2023-10-09"
) }}
