WITH ns_type AS (

  SELECT * FROM {{ ref ('namespace_pl_daily') }}

),

projects as (SELECT * FROM {{ ref('gitlab_dotcom_projects_xf') }}),

project_statistics as (

    SELECT * FROM {{ ref('gitlab_dotcom_project_statistic_snapshots_daily') }} 
),

namespaces_child as (

    SELECT * FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

),

storage AS (
  SELECT
    project_statistics.snapshot_day,
    namespaces_child.namespace_ultimate_parent_id                       AS namespace_id,
    SUM(COALESCE(project_statistics.repository_size, 0) / POW(1024, 3)) AS repo_size_gb
  FROM
     projects
  LEFT JOIN
    project_statistics
    ON
      projects.project_id = project_statistics.project_id
  INNER JOIN
    namespaces_child
    ON
      projects.namespace_id = namespaces_child.namespace_id
  -- WHERE
  --   date_trunc('month', project_statistics.snapshot_day) = '2023-02-01'
  GROUP BY
    1, 2
)



SELECT
  storage.snapshot_day,
  coalesce(ns_type.finance_pl, 'Internal') as finance_pl,
  SUM(repo_size_gb) AS repo_size_gb,
  100 * RATIO_TO_REPORT(SUM(repo_size_gb)) OVER (PARTITION BY snapshot_day) AS percent_repo_size_gb
FROM
  storage
LEFT JOIN
  ns_type
  ON
    storage.namespace_id = ns_type.dim_namespace_id
    AND
    storage.snapshot_day = ns_type.date_day
GROUP BY
  1, 2
ORDER BY
  1
