{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date')
]) }},

dotcom_prep AS (

  SELECT
    event_label,
    behavior_structured_event_pk,
    CASE
      WHEN plan_name = 'opensource' THEN 'Free'
      WHEN plan_name = 'free' THEN 'Free'
      WHEN plan_name = 'premium' THEN 'Premium'
      WHEN plan_name = 'ultimate_trial' THEN 'Trial'
      WHEN plan_name = 'ultimate' THEN 'Ultimate'
      WHEN plan_name = 'All' THEN 'All'
      WHEN plan_name = 'ultimate_trial_paid_customer' THEN 'Trial by Paid Customer'
      WHEN plan_name = 'premium_trial' THEN 'Trial'
      WHEN plan_name = 'starter' THEN 'Starter'
      WHEN plan_name = 'default' THEN 'Free'
    END                                                 AS plan_name,
    CASE
      WHEN gsc_is_gitlab_team_member IN ('false', 'e08c592bd39b012f7c83bbc0247311b238ee1caa61be28ccfd412497290f896a')
        THEN 'External'
      WHEN gsc_is_gitlab_team_member IN ('true', '5616b37fa230003bc8510af409bf3f5970e6d5027cc282b0ab3080700d92e7ad')
        THEN 'Internal'
      ELSE 'Unknown'
    END                                                 AS internal_or_external,
    behavior_date,
    gsc_pseudonymized_user_id,
    DATE_TRUNC(WEEK, behavior_date)                     AS current_week,
    DATE_TRUNC(WEEK, DATEADD(DAY, 7, behavior_date))    AS next_week,
    DATE_TRUNC(MONTH, behavior_date)                    AS current_month,
    DATE_TRUNC(MONTH, DATEADD(MONTH, 1, behavior_date)) AS next_month
  FROM {{ ref('mart_behavior_structured_event') }}
  WHERE event_action = 'execute_llm_method'
    AND behavior_date BETWEEN '2023-04-21' AND CURRENT_DATE
    AND event_category = 'Llm::ExecuteMethodService'
    AND event_label != 'code_suggestions'

),

ai_features AS (
  SELECT DISTINCT event_label
  FROM
    dotcom_prep
  UNION ALL
  SELECT 'All'
),

plans AS (
  SELECT DISTINCT
    CASE
      WHEN plan_name = 'opensource' THEN 'Free'
      WHEN plan_name = 'free' THEN 'Free'
      WHEN plan_name = 'premium' THEN 'Premium'
      WHEN plan_name = 'ultimate_trial' THEN 'Trial'
      WHEN plan_name = 'ultimate' THEN 'Ultimate'
      WHEN plan_name = 'All' THEN 'All'
      WHEN plan_name = 'ultimate_trial_paid_customer' THEN 'Trial by Paid Customer'
      WHEN plan_name = 'premium_trial' THEN 'Trial'
      WHEN plan_name = 'starter' THEN 'Starter'
      WHEN plan_name = 'default' THEN 'Free'
    END AS plan
  FROM
    {{ ref('dim_plan') }}
  UNION ALL
  SELECT 'All'
),

int_ext_all AS (
  SELECT DISTINCT internal_or_external
  FROM
    dotcom_prep
  UNION ALL
  SELECT 'All'
),

prep AS (
  SELECT

    p.gsc_pseudonymized_user_id,
    p.behavior_structured_event_pk,
    f.event_label,
    i.internal_or_external,
    plans.plan AS plan_name,
    p.behavior_date,
    p.current_week,
    p.next_week,
    p.current_month,
    p.next_month
  FROM
    dotcom_prep AS p
  LEFT JOIN
    ai_features AS f
    ON
      p.event_label = f.event_label OR f.event_label = 'All'
  LEFT JOIN
    int_ext_all AS i
    ON
      p.internal_or_external = i.internal_or_external OR i.internal_or_external = 'All'
  LEFT JOIN
    plans ON p.plan_name = plans.plan OR plans.plan = 'All'
),

dau AS (

  SELECT
    behavior_date                             AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'DAU'                                     AS metric,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS metric_value
  FROM prep
  WHERE
    behavior_date < CURRENT_DATE
  GROUP BY ALL

),

wau AS (

  SELECT
    DATE_TRUNC(WEEK, behavior_date)           AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'WAU'                                     AS metric,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS metric_value
  FROM prep
  GROUP BY ALL

),

mau AS (

  SELECT
    DATE_TRUNC(MONTH, behavior_date)          AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'MAU'                                     AS metric,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS metric_value
  FROM prep
  GROUP BY ALL

),

weekly_retention_grouped AS (
  SELECT
    prep.current_week,
    prep.event_label,
    prep.plan_name,
    prep.internal_or_external,
    'Weekly Retention'                                                                              AS metric,
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON prep.event_label = e2.event_label
      AND prep.gsc_pseudonymized_user_id = e2.gsc_pseudonymized_user_id
      AND prep.next_week = e2.current_week
      AND prep.plan_name = e2.plan_name
      AND prep.internal_or_external = e2.internal_or_external
  WHERE
    prep.next_week < DATE_TRUNC(WEEK, CURRENT_DATE())
    AND
    prep.gsc_pseudonymized_user_id IS NOT NULL
  GROUP BY ALL

),

monthly_retention_grouped AS (
  SELECT
    prep.current_month,
    prep.event_label,
    prep.plan_name,
    prep.internal_or_external,
    'Monthly Retention'                                                                             AS metric,
    (COUNT(DISTINCT e2.gsc_pseudonymized_user_id) / COUNT(DISTINCT prep.gsc_pseudonymized_user_id)) AS retention_rate
  FROM prep
  LEFT JOIN prep AS e2
    ON prep.event_label = e2.event_label
      AND prep.gsc_pseudonymized_user_id = e2.gsc_pseudonymized_user_id
      AND prep.next_month = e2.current_month
      AND prep.plan_name = e2.plan_name
      AND prep.internal_or_external = e2.internal_or_external
  WHERE
    prep.next_month < DATE_TRUNC(MONTH, CURRENT_DATE())
    AND
    prep.gsc_pseudonymized_user_id IS NOT NULL
  GROUP BY ALL

),

daily_event AS (

  SELECT
    behavior_date                                AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'Daily Event Count'                          AS metric,
    COUNT(DISTINCT behavior_structured_event_pk) AS metric_value
  FROM prep
  WHERE
    behavior_date < CURRENT_DATE
  GROUP BY ALL

),

weekly_event AS (

  SELECT
    DATE_TRUNC(WEEK, behavior_date)              AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'Weekly Event Count'                         AS metric,
    COUNT(DISTINCT behavior_structured_event_pk) AS metric_value
  FROM prep
  GROUP BY ALL

),

monthly_event AS (

  SELECT
    DATE_TRUNC(MONTH, behavior_date)             AS _date,
    event_label,
    plan_name,
    internal_or_external,
    'Monthly Event Count'                        AS metric,
    COUNT(DISTINCT behavior_structured_event_pk) AS metric_value
  FROM prep
  GROUP BY ALL

),

metrics AS (

  SELECT *
  FROM dau

  UNION ALL

  SELECT *
  FROM wau

  UNION ALL

  SELECT *
  FROM mau

  UNION ALL

  SELECT *
  FROM weekly_retention_grouped

  UNION ALL

  SELECT *
  FROM monthly_retention_grouped

  UNION ALL

  SELECT *
  FROM daily_event

  UNION ALL

  SELECT *
  FROM weekly_event

  UNION ALL

  SELECT *
  FROM monthly_event

),

metric_prep AS (

  SELECT *
  FROM
    {{ ref('mart_ping_instance_metric') }}
  WHERE major_minor_version_id >= 1611
    AND metric_value > 0
    AND ping_created_date_month > '2024-01-01'
    AND ping_deployment_type != 'GitLab.com'
    AND
    (
      (metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly' AND is_last_ping_of_month = TRUE)
      OR
      (metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly' AND is_last_ping_of_week = TRUE)
    )

),

sm_expanded AS (
  SELECT
    CASE
      WHEN metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
        THEN
          DATE_TRUNC(WEEK, ping_created_date_week::DATE)
      ELSE
        ping_created_date_month::DATE
    END                                                                                                                    AS date_day,
    f.event_label                                                                                                          AS ai_feature,
    plans.plan,
    i.internal_or_external,
    ping_deployment_type                                                                                                   AS delivery_type,
    SUM(COALESCE(metric_value, 0)::INT)                                                                                    AS metric_value,
    CASE
      WHEN metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_monthly' THEN 'MAU'
      WHEN metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly' THEN 'WAU' END
      AS metric
  FROM metric_prep
  LEFT JOIN
    ai_features AS f
    ON
      f.event_label = 'chat' OR f.event_label = 'All'
  LEFT JOIN
    int_ext_all AS i
    ON i.internal_or_external = 'External' OR i.internal_or_external = 'All'
  LEFT JOIN
    plans ON metric_prep.ping_product_tier = plans.plan OR plans.plan = 'All'
  GROUP BY ALL
),

mart_tokens AS (
  SELECT * FROM
    {{ ref ('mart_behavior_structured_event') }}
  WHERE event_action IN ('tokens_per_user_request_response', 'tokens_per_user_request_prompt')
    AND behavior_at >= '2023-10-01'

),

source_tokens_all_feats_except_cs AS (
  SELECT
    behavior_date,
    event_action,
    event_label    AS feature,
    event_category AS model,
    event_property AS request_id,
    event_value    AS token_amount,
    gsc_is_gitlab_team_member
  FROM
    mart_tokens
  WHERE
    app_id = 'gitlab'
),

source_token_cs AS (

  SELECT
    behavior_date,
    event_action,
    event_label    AS feature,
    event_category AS model,
    event_property AS request_id,
    event_value    AS token_amount,
    gsc_is_gitlab_team_member
  FROM mart_tokens
  WHERE
    event_category = 'code_suggestions'
    AND event_label IN ('code_generation', 'code_completion')

),

source_token AS (

  SELECT * FROM source_tokens_all_feats_except_cs
  UNION ALL
  SELECT * FROM source_token_cs

),

token_usage AS (

  SELECT
    DATE_TRUNC('day', behavior_date)                                                    AS date_day,
    feature                                                                             AS ai_feature,
    '?'                                                                                 AS plan,
    gsc_is_gitlab_team_member                                                           AS internal_or_external,
    SUM(token_amount)                                                                   AS metric_value,
    CASE WHEN event_action LIKE '%prompt%' THEN 'Input Tokens' ELSE 'Output Tokens' END AS metric
  FROM source_token
  GROUP BY ALL
),


unify AS (

  SELECT
    dim_date.date_day::DATE AS date_day,
    metrics.event_label     AS ai_feature,
    metrics.plan_name       AS plan,
    metrics.internal_or_external,
    'Gitlab.com'            AS delivery_type,
    metrics.metric_value,
    metrics.metric
  FROM dim_date
  LEFT JOIN metrics
    ON dim_date.date_day = metrics._date
  WHERE dim_date.date_day BETWEEN '2023-04-21' AND CURRENT_DATE

  UNION ALL

  SELECT
    dim_date.date_day::DATE AS date_day,
    metrics.event_label     AS ai_feature,
    metrics.plan_name,
    metrics.internal_or_external,
    'All'                   AS delivery_type,
    metrics.metric_value,
    metrics.metric
  FROM dim_date
  LEFT JOIN metrics
    ON dim_date.date_day = metrics._date
  WHERE dim_date.date_day BETWEEN '2023-04-21' AND CURRENT_DATE

  UNION ALL

  SELECT
    date_day,
    ai_feature,
    plan,
    internal_or_external,
    delivery_type,
    metric_value,
    metric
  FROM sm_expanded

  UNION ALL

  SELECT
    date_day,
    ai_feature,
    plan,
    internal_or_external,
    'All' AS delivery_type,
    metric_value,
    metric
  FROM sm_expanded

  UNION ALL

  SELECT
    date_day,
    ai_feature,
    plan,
    internal_or_external,
    '?' AS delivery_type,
    metric_value,
    metric
  FROM token_usage

),


dedup AS (

  SELECT
    date_day,
    ai_feature,
    plan,
    internal_or_external,
    delivery_type,
    SUM(metric_value) AS metric_value,
    metric
  FROM unify
  WHERE date_day < CURRENT_DATE()
    AND metric_value IS NOT NULL
    AND metric IS NOT NULL
  GROUP BY ALL

)

SELECT *
FROM dedup
