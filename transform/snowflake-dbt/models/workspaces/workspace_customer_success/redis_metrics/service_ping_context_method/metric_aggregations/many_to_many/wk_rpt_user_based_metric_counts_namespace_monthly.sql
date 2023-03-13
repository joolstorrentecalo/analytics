{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH events AS (
  SELECT
    *
  FROM {{ ref('wk_mart_snowplow_events_service_ping_metrics') }}
  -- only include non-aggregated metrics or aggregated metrics of 'OR' aggregate operator.
  -- more details here: https://gitlab.com/gitlab-org/gitlab/-/issues/376244#note_1167575425
  WHERE (aggregate_operator = 'OR'
    OR aggregate_operator IS NULL)
    AND time_frame = '28d'
),

dates AS (
  SELECT
    *
  FROM {{ ref('dim_date') }}
),

final AS (
  SELECT
    DATE_TRUNC('month', events.behavior_at) AS date_month,
    events.ultimate_parent_namespace_id,
    events.metrics_path,
    COUNT(DISTINCT events.gsc_pseudonymized_user_id) AS distinct_users_whole_month,
    COUNT(DISTINCT IFF(dates.days_until_last_day_of_month <= 27, events.gsc_pseudonymized_user_id, NULL)) AS distinct_users_last_28d_month
  FROM events
  LEFT JOIN dates ON dates.date_actual = DATE_TRUNC('day', events.behavior_at)
  {{ dbt_utils.group_by(n = 3) }}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2023-03-08"
) }}
