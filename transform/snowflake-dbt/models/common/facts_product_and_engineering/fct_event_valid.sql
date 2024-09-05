{% set filter_date = (run_started_at - modules.datetime.timedelta(weeks=160)).strftime('%Y-%m-%d') %}

{{
    config(
        materialized='incremental',
        tags=['mnpi_exception', 'product'],
        unique_key='event_pk',
        on_schema_change='sync_all_columns',
        full_refresh=only_force_full_refresh()
    )
}}

{{ simple_cte([
    ('fct_event', 'fct_event'),
    ('dim_user', 'dim_user'),
    ('xmau_metrics', 'map_gitlab_dotcom_xmau_metrics'),
    ('namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_project', 'dim_project'),
    ('dim_date', 'dim_date'),
    ('dim_user_hist','dim_user_hist'),
    ('dim_namespace', 'dim_namespace')
    ])
}},

namespace_bdg AS (

  SELECT
    namespace_order_subscription.dim_subscription_id AS dim_latest_subscription_id,
    namespace_order_subscription.order_id,
    namespace_order_subscription.dim_crm_account_id,
    namespace_order_subscription.dim_billing_account_id,
    namespace_order_subscription.dim_namespace_id,
    namespace_order_subscription.product_tier_name_subscription,
    dim_subscription.subscription_version,
    dim_subscription.subscription_updated_date,
    dim_subscription.dim_subscription_id_original
  FROM namespace_order_subscription
  INNER JOIN dim_subscription
    ON namespace_order_subscription.dim_subscription_id = dim_subscription.dim_subscription_id

),

{% if is_incremental() %}
updated_subscriptions AS (
  SELECT
    dim_namespace_id
  FROM namespace_bdg
  QUALIFY MAX(subscription_updated_date) OVER (PARTITION BY dim_subscription_id_original) > (SELECT MAX(dimensions_checked_at) FROM {{ this }} )
),

updated_users AS (
  SELECT
    user_id,
    dim_user_sk
  FROM dim_user_hist
  WHERE COALESCE(dbt_valid_to, dbt_valid_from) > (SELECT MAX(dimensions_checked_at) FROM {{ this }} ) -- updated_at > (SELECT MAX(dimensions_checked_at) FROM {{ this }} )
  QUALIFY count(DISTINCT is_blocked_user) OVER (PARTITION BY user_id) > 1
),

updated_namespaces AS (
  SELECT 
    dim_namespace_id
  FROM dim_namespace
  WHERE (
    updated_at > (SELECT MAX(dimensions_checked_at) FROM {{ this }} ) 
    OR creator_id IN (SELECT user_id FROM updated_users)
    )
),

{% endif %}

fct_event_valid AS (
    
    /*
    fct_event_valid is at the atomic grain of event_id and event_created_at timestamp. All other derived facts in the GitLab.com usage events 
    lineage are built from this derived fact. This CTE pulls in ALL of the columns from the fct_event as a base data set. It uses the dbt_utils.star function 
    to select all columns except the meta data table related columns from the fct_event. The CTE also filters out imported projects and events with 
    data quality issues by filtering out negative days since user creation at event date. It keeps events with a NULL days since user creation to capture events
    that do not have a user. fct_event_valid also filters out events from blocked users with a join back to dim_user. The table also filters to a rolling 36 months of data 
    for performance optimization.
    */

    SELECT
      fct_event.dim_user_sk,
      fct_event.dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.
      {{ dbt_utils.star(from=ref('fct_event'), except=["DIM_USER_SK", "DIM_USER_ID", "CREATED_BY",
          "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }},
      xmau_metrics.group_name,
      xmau_metrics.section_name,
      xmau_metrics.stage_name,
      xmau_metrics.smau AS is_smau,
      xmau_metrics.gmau AS is_gmau,
      xmau_metrics.is_umau
    FROM fct_event
    LEFT JOIN xmau_metrics
      ON fct_event.event_name = xmau_metrics.common_events_to_include
    LEFT JOIN dim_user
      ON fct_event.dim_user_sk = dim_user.dim_user_sk
    WHERE fct_event.event_created_at >= '{{ filter_date }}' --DATEADD(MONTH, -36, DATE_TRUNC(MONTH,CURRENT_DATE))
      AND (fct_event.days_since_user_creation_at_event_date >= 0
           OR fct_event.days_since_user_creation_at_event_date IS NULL)
      AND (dim_user.is_blocked_user = FALSE 
           OR dim_user.is_blocked_user IS NULL)
    {% if is_incremental() %}

      AND (fct_event.event_created_at > (SELECT MAX(event_created_at) FROM {{ this }})
      -- Added to capture changes to the latest subscription
      OR fct_event.dim_ultimate_parent_namespace_id IN (SELECT * FROM updated_subscriptions)
      OR fct_event.dim_user_sk  IN (SELECT dim_user_sk FROM updated_users )
      OR fct_event.dim_ultimate_parent_namespace_id  IN (SELECT dim_namespace_id FROM updated_namespaces )
      )

    {% endif %}
),

deduped_namespace_bdg AS (

  SELECT
    dim_latest_subscription_id,
    order_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_namespace_id
  FROM namespace_bdg
  WHERE product_tier_name_subscription IN ('SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id ORDER BY subscription_version DESC, subscription_updated_date DESC) = 1

),

dim_namespace_w_bdg AS (

  SELECT
    dim_namespace.dim_namespace_id,
    dim_namespace.dim_product_tier_id AS dim_latest_product_tier_id,
    deduped_namespace_bdg.dim_latest_subscription_id,
    deduped_namespace_bdg.order_id,
    deduped_namespace_bdg.dim_crm_account_id,
    deduped_namespace_bdg.dim_billing_account_id
  FROM deduped_namespace_bdg
  INNER JOIN dim_namespace
    ON dim_namespace.dim_namespace_id = deduped_namespace_bdg.dim_namespace_id

),

paid_flag_by_day AS (

  SELECT
    dim_ultimate_parent_namespace_id,
    plan_was_paid_at_event_timestamp AS plan_was_paid_at_event_date,
    plan_id_at_event_timestamp AS plan_id_at_event_date,
    plan_name_at_event_timestamp AS plan_name_at_event_date,
    event_created_at,
    event_date
  FROM fct_event_valid
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_ultimate_parent_namespace_id, event_date
      ORDER BY event_created_at DESC) = 1

),

fct_event_w_flags AS (

  SELECT 
    fct_event_valid.event_pk,
    fct_event_valid.event_id,
    fct_event_valid.dim_event_date_id,
    fct_event_valid.dim_ultimate_parent_namespace_id,
    fct_event_valid.dim_project_id,
    fct_event_valid.dim_user_sk,
    fct_event_valid.dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be deprecated in a future MR.
    fct_event_valid.is_null_user,
    fct_event_valid.event_created_at,
    fct_event_valid.event_date,
    fct_event_valid.group_name,
    fct_event_valid.section_name,
    fct_event_valid.stage_name,
    fct_event_valid.is_smau,
    fct_event_valid.is_gmau,
    fct_event_valid.is_umau,
    fct_event_valid.parent_id,
    fct_event_valid.parent_type,
    fct_event_valid.event_name,
    fct_event_valid.days_since_user_creation_at_event_date,
    fct_event_valid.days_since_namespace_creation_at_event_date,
    fct_event_valid.days_since_project_creation_at_event_date,
    fct_event_valid.data_source,
    dim_namespace_w_bdg.dim_latest_product_tier_id,
    dim_namespace_w_bdg.dim_latest_subscription_id,
    dim_namespace_w_bdg.order_id,
    dim_namespace_w_bdg.dim_crm_account_id,
    dim_namespace_w_bdg.dim_billing_account_id,
    COALESCE(paid_flag_by_day.plan_was_paid_at_event_date, FALSE) AS plan_was_paid_at_event_date,
    COALESCE(paid_flag_by_day.plan_id_at_event_date, 34) AS plan_id_at_event_date,
    COALESCE(paid_flag_by_day.plan_name_at_event_date, 'free') AS plan_name_at_event_date,
    dim_namespace.namespace_type AS ultimate_parent_namespace_type,
    dim_namespace.namespace_is_internal,
    dim_namespace.namespace_creator_is_blocked,
    dim_namespace.created_at AS namespace_created_at,
    CAST(dim_namespace.created_at AS DATE) AS namespace_created_date,
    dim_user.created_at AS user_created_at,
    COALESCE(dim_project.is_learn_gitlab, FALSE) AS project_is_learn_gitlab,
    COALESCE(dim_project.is_imported, FALSE) AS project_is_imported,
    dim_date.first_day_of_month AS event_calendar_month,
    dim_date.quarter_name AS event_calendar_quarter,
    dim_date.year_actual AS event_calendar_year,
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS dimensions_checked_at
  FROM fct_event_valid
  LEFT JOIN dim_namespace_w_bdg
    ON fct_event_valid.dim_ultimate_parent_namespace_id = dim_namespace_w_bdg.dim_namespace_id
  LEFT JOIN paid_flag_by_day
    ON fct_event_valid.dim_ultimate_parent_namespace_id = paid_flag_by_day.dim_ultimate_parent_namespace_id
      AND fct_event_valid.event_date = paid_flag_by_day.event_date
  LEFT JOIN dim_namespace
    ON fct_event_valid.dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id
  LEFT JOIN dim_user
    ON fct_event_valid.dim_user_sk = dim_user.dim_user_sk
  LEFT JOIN dim_project
    ON fct_event_valid.dim_project_id = dim_project.dim_project_id
  LEFT JOIN dim_date
    ON fct_event_valid.dim_event_date_id = dim_date.date_id

),

gitlab_dotcom_fact AS (

  SELECT
    --Primary Key
    event_pk,

    --Natural Key
    event_id,
    
    --Foreign Keys
    dim_event_date_id,
    dim_ultimate_parent_namespace_id,
    dim_project_id,
    dim_user_sk,
    dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.
    dim_latest_product_tier_id,
    dim_latest_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    order_id,
    
    --Time attributes
    event_created_at,
    event_date,
    
    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    is_null_user,
    group_name,
    section_name,
    stage_name,
    is_smau,
    is_gmau,
    is_umau,
    parent_id,
    parent_type,
    event_name,
    plan_id_at_event_date,
    plan_name_at_event_date,
    plan_was_paid_at_event_date,
    days_since_user_creation_at_event_date,
    days_since_namespace_creation_at_event_date,
    days_since_project_creation_at_event_date,
    data_source,
    -- dimensions 
    ultimate_parent_namespace_type,
    namespace_is_internal,
    namespace_creator_is_blocked,
    namespace_created_at,
    namespace_created_date,
    user_created_at,
    project_is_learn_gitlab,
    project_is_imported,
    event_calendar_month,
    event_calendar_quarter,
    event_calendar_year,
    dimensions_checked_at
  FROM fct_event_w_flags

)

{{ dbt_audit(
    cte_ref="gitlab_dotcom_fact",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2022-04-09",
    updated_date="2023-07-21"
) }}
