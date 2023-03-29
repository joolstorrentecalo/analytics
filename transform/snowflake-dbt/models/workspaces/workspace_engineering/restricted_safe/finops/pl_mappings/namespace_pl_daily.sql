WITH date_spine AS (

  SELECT date_day FROM {{ ref('dim_date') }}
  WHERE date_day < GETDATE() AND date_day >= '2020-01-01'
),

plan_ids AS (

  SELECT DISTINCT
    gitlab_plan_id,
    CASE WHEN gitlab_plan_title IN ('Premium (Formerly Silver)', 'Ultimate (Formerly Gold)', 'Bronze') THEN 'paid'
      WHEN gitlab_plan_title IN ('Free', 'Premium Trial', 'Ultimate Trial', 'Open Source Program') THEN 'free'
      WHEN gitlab_plan_title IS NULL THEN 'internal'
      ELSE 'internal' END AS gitlab_plan_title
  FROM {{ ref('dim_namespace') }}
-- add namespace_is_internal to internal identification
),

dim_namespace_plan_hist AS (

  SELECT * FROM {{ ref('dim_namespace_plan_hist') }}

),

internal AS (
  SELECT DISTINCT namespace_id AS ultimate_parent_namespace_id
  FROM {{ ref('internal_gitlab_namespaces') }}
  WHERE namespace_id IS NOT NULL
)

SELECT
  date_spine.date_day,
  hist.dim_namespace_id,
  hist.dim_plan_id,
  CASE WHEN internal.ultimate_parent_namespace_id IS NOT NULL THEN 'Internal' ELSE COALESCE(plan_ids.gitlab_plan_title, 'Internal') END AS finance_pl
FROM date_spine
LEFT JOIN dim_namespace_plan_hist hist ON date_spine.date_day BETWEEN hist.valid_from AND COALESCE(hist.valid_to, GETDATE())
LEFT JOIN plan_ids ON plan_ids.gitlab_plan_id = hist.dim_plan_id
LEFT JOIN internal ON hist.dim_namespace_id = internal.ultimate_parent_namespace_id
