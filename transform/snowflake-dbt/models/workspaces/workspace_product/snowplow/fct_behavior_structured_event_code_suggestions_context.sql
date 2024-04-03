{{
  config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=["mnpi_exception", "product"],
    on_schema_change='sync_all_columns'
  )
}}


{{ simple_cte([
    ('bdg_namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_subscription', 'dim_subscription'),
    ('zuora_product_rate_plan_source', 'zuora_product_rate_plan_source'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_installation', 'dim_installation'),
    ('fct_ping_instance_metric', 'fct_ping_instance_metric')
]) }},

clicks AS (
  SELECT
    behavior_structured_event_pk,
    behavior_at,
    is_staging_event,
    contexts
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2023-08-01' -- no events added to context before Aug 2023
    AND has_code_suggestions_context = TRUE

), code_suggestion_context AS (

  SELECT
    clicks.behavior_structured_event_pk,
    clicks.behavior_at,
    clicks.is_staging_event,
    flat_contexts.value                                                             AS code_suggestions_context,
    flat_contexts.value['data']['model_engine']::VARCHAR                            AS model_engine,
    flat_contexts.value['data']['model_name']::VARCHAR                              AS model_name,
    flat_contexts.value['data']['prefix_length']::INT                               AS prefix_length,
    flat_contexts.value['data']['suffix_length']::INT                               AS suffix_length,
    flat_contexts.value['data']['language']::VARCHAR                                AS language,
    CASE
      WHEN flat_contexts.value['data']['gitlab_realm']::VARCHAR IN (
        'SaaS',
        'saas'
      ) THEN 'SaaS'
      WHEN flat_contexts.value['data']['gitlab_realm']::VARCHAR IN (
        'Self-Managed',
        'self-managed'
      ) THEN 'Self-Managed'
      WHEN flat_contexts.value['data']['gitlab_realm']::VARCHAR IS NULL THEN NULL
      ELSE flat_contexts.value['data']['gitlab_realm']::VARCHAR
    END                                                                             AS delivery_type,
    flat_contexts.value['data']['api_status_code']::INT                             AS api_status_code,
    flat_contexts.value['data']['gitlab_saas_namespace_ids']::VARCHAR               AS saas_namespace_ids,
    flat_contexts.value['data']['gitlab_saas_duo_pro_namespace_ids']::VARCHAR       AS duo_namespace_ids,
    COALESCE(
        IFF(duo_namespace_ids = '[]', NULL, duo_namespace_ids),
        IFF(saas_namespace_ids = '[]', NULL, saas_namespace_ids)
        )                                                                           AS namespace_ids,
    flat_contexts.value['data']['gitlab_instance_id']::VARCHAR                      AS instance_id,
    flat_contexts.value['data']['gitlab_host_name']::VARCHAR                        AS host_name,
    flat_contexts.value['data']['is_streaming']::BOOLEAN                            AS is_streaming
  FROM clicks,
  LATERAL FLATTEN(input => TRY_PARSE_JSON(clicks.contexts), path => 'data') AS flat_contexts
  WHERE flat_contexts.value['schema']::VARCHAR LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%'
    {% if is_incremental() %}
    
        AND clicks.behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
    
    {% endif %}

)

, flattened_namespaces AS (

  SELECT 
    code_suggestion_context.behavior_structured_event_pk,
    flattened_namespace.value::VARCHAR                                 AS namespace_id
  FROM code_suggestion_context,
  LATERAL FLATTEN (input => TRY_PARSE_JSON(code_suggestion_context.namespace_ids)) AS flattened_namespace
  WHERE namespace_ids IS NOT NULL
    AND namespace_ids != '[]'

)

, deduped_namespace_bdg AS (

  /*
  Limit the subscriptions we connect to code suggestions to only the paid tiers and filter to the most recent subscription version.
  This will give us the most recent subscription-namespace connection available in the data.
  */

  SELECT
    bdg_namespace_order_subscription.dim_subscription_id                            AS dim_latest_subscription_id,
    bdg_namespace_order_subscription.subscription_name                              AS latest_subscription_name,
    bdg_namespace_order_subscription.dim_crm_account_id,
    bdg_namespace_order_subscription.dim_billing_account_id,
    bdg_namespace_order_subscription.dim_namespace_id
  FROM bdg_namespace_order_subscription
  INNER JOIN dim_subscription
    ON bdg_namespace_order_subscription.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN zuora_product_rate_plan_source AS order_product_rate_plan
    ON bdg_namespace_order_subscription.product_rate_plan_id_order = order_product_rate_plan.product_rate_plan_id
  LEFT JOIN zuora_product_rate_plan_source AS subscription_product_rate_plan
    ON bdg_namespace_order_subscription.product_rate_plan_id_subscription = subscription_product_rate_plan.product_rate_plan_id
  WHERE bdg_namespace_order_subscription.product_tier_name_subscription IN ('SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id ORDER BY subscription_version DESC) = 1
  
)

, dim_namespace_w_bdg AS (

  SELECT
    dim_namespace.dim_namespace_id,
    dim_namespace.dim_product_tier_id   AS dim_latest_product_tier_id,
    deduped_namespace_bdg.dim_latest_subscription_id,
    deduped_namespace_bdg.dim_crm_account_id,
    deduped_namespace_bdg.dim_billing_account_id,
    deduped_namespace_bdg.latest_subscription_name
  FROM deduped_namespace_bdg
  INNER JOIN dim_namespace
    ON dim_namespace.dim_namespace_id = deduped_namespace_bdg.dim_namespace_id
 
)

, code_suggestions_with_ultimate_parent_namespaces_and_crm_accounts AS (

  SELECT
    flattened_namespaces.behavior_structured_event_pk,
    flattened_namespaces.namespace_id                       AS namespace_id,
    dim_namespace.ultimate_parent_namespace_id,
    dim_namespace_w_bdg.latest_subscription_name            AS subscription_name,
    dim_namespace.namespace_is_internal,
    dim_crm_account.dim_crm_account_id                      AS dim_crm_account_id,
    dim_crm_account.dim_parent_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_crm_account.parent_crm_account_name
  FROM flattened_namespaces
  LEFT JOIN dim_namespace
    ON flattened_namespaces.namespace_id = dim_namespace.namespace_id
  LEFT JOIN dim_namespace_w_bdg
    ON flattened_namespaces.namespace_id = dim_namespace_w_bdg.dim_namespace_id
  LEFT JOIN dim_crm_account
    ON dim_namespace_w_bdg.dim_crm_account_id = dim_crm_account.dim_crm_account_id

)

, code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas AS (

  SELECT
    behavior_structured_event_pk,
    ARRAY_AGG(DISTINCT ultimate_parent_namespace_id) WITHIN GROUP (ORDER BY ultimate_parent_namespace_id)   AS ultimate_parent_namespace_ids,
    ARRAY_AGG(DISTINCT subscription_name) WITHIN GROUP (ORDER BY subscription_name ASC)                     AS subscription_names,
    ARRAY_AGG(DISTINCT dim_crm_account_id) WITHIN GROUP (ORDER BY dim_crm_account_id ASC)                   AS dim_crm_account_ids,
    ARRAY_AGG(DISTINCT dim_parent_crm_account_id) WITHIN GROUP (ORDER BY dim_parent_crm_account_id ASC)     AS dim_parent_crm_account_ids,
    ARRAY_AGG(DISTINCT crm_account_name) WITHIN GROUP (ORDER BY crm_account_name)                           AS crm_account_names,
    ARRAY_AGG(DISTINCT parent_crm_account_name) WITHIN GROUP (ORDER BY parent_crm_account_name)             AS parent_crm_account_names,
    ARRAY_SIZE(ultimate_parent_namespace_ids)                                                               AS count_ultimate_parent_namespace_ids,
    ARRAY_SIZE(subscription_names)                                                                          AS count_subscription_names,
    ARRAY_SIZE(dim_crm_account_ids)                                                                         AS count_dim_crm_account_ids,
    ARRAY_SIZE(dim_parent_crm_account_ids)                                                                  AS count_dim_parent_crm_account_ids,
    ARRAY_SIZE(crm_account_names)                                                                           AS count_crm_account_names,
    ARRAY_SIZE(parent_crm_account_names)                                                                    AS count_parent_crm_account_names,
    MAX(namespace_is_internal)                                                                              AS namespace_is_internal
  FROM code_suggestions_with_ultimate_parent_namespaces_and_crm_accounts
  GROUP BY ALL

)

, context_with_installation_id AS (

  SELECT
    code_suggestion_context.behavior_structured_event_pk,
    code_suggestion_context.instance_id,
    dim_installation.dim_installation_id,
    dim_installation.host_name
  FROM code_suggestion_context
  LEFT JOIN dim_installation
    ON dim_installation.dim_instance_id = code_suggestion_context.instance_id
  WHERE code_suggestion_context.instance_id IS NOT NULL
    AND code_suggestion_context.instance_id != 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'

)

, bdg_latest_instance_subscription AS (

  SELECT 
    fct_ping_instance_metric.dim_installation_id, 
    fct_ping_instance_metric.dim_subscription_id,
    dim_subscription.subscription_name,
    dim_subscription.dim_crm_account_id,
    dim_crm_account.dim_parent_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_crm_account.parent_crm_account_name
  FROM fct_ping_instance_metric
  LEFT JOIN dim_subscription
    ON fct_ping_instance_metric.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN dim_crm_account
    ON dim_subscription.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_installation_id ORDER BY dim_ping_date_id DESC) = 1

)

, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm AS (

  SELECT
    context_with_installation_id.behavior_structured_event_pk,
    ARRAY_AGG(DISTINCT context_with_installation_id.dim_installation_id) WITHIN GROUP (ORDER BY context_with_installation_id.dim_installation_id ASC)                     AS dim_installation_ids,
    ARRAY_AGG(DISTINCT context_with_installation_id.host_name) WITHIN GROUP (ORDER BY context_with_installation_id.host_name)                                             AS host_names,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.subscription_name) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.subscription_name ASC)                 AS subscription_names,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.dim_crm_account_id) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.dim_crm_account_id ASC)               AS dim_crm_account_ids,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.dim_parent_crm_account_id) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.dim_parent_crm_account_id ASC) AS dim_parent_crm_account_ids,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.crm_account_name) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.crm_account_name)                       AS crm_account_names,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.parent_crm_account_name) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.parent_crm_account_name)         AS parent_crm_account_names,
    ARRAY_SIZE(dim_installation_ids)                                                                                                                                      AS count_dim_installation_ids,
    ARRAY_SIZE(subscription_names)                                                                                                                                        AS count_subscription_names,
    ARRAY_SIZE(dim_crm_account_ids)                                                                                                                                       AS count_dim_crm_account_ids,
    ARRAY_SIZE(dim_parent_crm_account_ids)                                                                                                                                AS count_dim_parent_crm_account_ids,
    ARRAY_SIZE(crm_account_names)                                                                                                                                         AS count_crm_account_names,
    ARRAY_SIZE(parent_crm_account_names)                                                                                                                                  AS count_parent_crm_account_names,
    ARRAY_SIZE(host_names)                                                                                                                                                AS count_host_names
  FROM context_with_installation_id
  LEFT JOIN bdg_latest_instance_subscription
    ON context_with_installation_id.dim_installation_id = bdg_latest_instance_subscription.dim_installation_id
  GROUP BY ALL
  
)

, combined AS (

  SELECT 
    code_suggestion_context.*,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.ultimate_parent_namespace_ids,[])                                                                                                                                 AS ultimate_parent_namespace_ids,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.subscription_names,code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.subscription_names),[])                                                AS subscription_names, 
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_crm_account_ids,code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_crm_account_ids),[])                                              AS dim_crm_account_ids, 
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_parent_crm_account_ids,code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_parent_crm_account_ids),[])                                AS dim_parent_crm_account_ids,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.crm_account_names,code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.crm_account_names),[])                                                  AS crm_account_names,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.parent_crm_account_names,code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.parent_crm_account_names),[])                                    AS parent_crm_account_names,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_installation_ids,[])                                                                                                                                            AS dim_installation_ids,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.host_names,[])                                                                                                                                                      AS host_names,
    IFF(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_dim_crm_account_ids, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_dim_crm_account_ids) = 1, 
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_crm_account_ids,0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_crm_account_ids,0))::VARCHAR, NULL)                          AS dim_crm_account_id,
    IFF(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_dim_parent_crm_account_ids, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_dim_parent_crm_account_ids) = 1, 
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_parent_crm_account_ids,0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_parent_crm_account_ids,0))::VARCHAR, NULL)            AS dim_parent_crm_account_id,
    IFF(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_subscription_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_subscription_names) = 1, 
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.subscription_names,0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.subscription_names,0))::VARCHAR, NULL)                            AS subscription_name,
    IFF(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_crm_account_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_crm_account_names) = 1, 
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.crm_account_names,0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.crm_account_names,0))::VARCHAR, NULL)                              AS crm_account_name,
    IFF(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_parent_crm_account_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_parent_crm_account_names) = 1, 
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.parent_crm_account_names,0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.parent_crm_account_names,0))::VARCHAR, NULL)                AS parent_crm_account_name,
    IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_ultimate_parent_namespace_ids = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.ultimate_parent_namespace_ids,0)::VARCHAR, NULL)         AS ultimate_parent_namespace_id,
    IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_dim_installation_ids = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_installation_ids,0)::VARCHAR, NULL)                               AS dim_installation_id,
    IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_host_names = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.host_names,0)::VARCHAR, NULL)                                                   AS installation_host_name,
    code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.namespace_is_internal                                                                                                                                                      AS namespace_is_internal
  FROM code_suggestion_context
  LEFT JOIN code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas
    ON code_suggestion_context.behavior_structured_event_pk = code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.behavior_structured_event_pk
  LEFT JOIN code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm 
    ON code_suggestion_context.behavior_structured_event_pk = code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.behavior_structured_event_pk
)

{{ dbt_audit(
    cte_ref="combined",
    created_by="@mdrussell",
    updated_by="@utkarsh060",
    created_date="2023-09-25",
    updated_date="2024-04-02"
) }}
