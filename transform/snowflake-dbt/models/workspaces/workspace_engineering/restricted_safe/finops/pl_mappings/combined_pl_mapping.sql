WITH date_spine AS (

  SELECT date_day FROM {{ ref('dim_date') }}
  WHERE date_day < GETDATE() AND date_day >= '2020-01-01'
),

infralabel_pl AS (

  SELECT
    date_spine.date_day,
    NULL                      AS gcp_project_id,
    NULL                      AS gcp_service_description,
    NULL                      AS gcp_sku_description,
    infralabel_pl.infra_label,
    NULL                     AS env_label,
    NULL AS runner_label,
    lower(infralabel_pl.type)       AS pl_category,
    infralabel_pl.allocation AS pl_percent,
    'infralabel_pl'          AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref('infralabel_pl') }}

),

projects_pl AS (

  SELECT
    date_spine.date_day,
    projects_pl.project_id AS gcp_project_id,
    NULL                   AS gcp_service_description,
    NULL                   AS gcp_sku_description,
    NULL                   AS infra_label,
    NULL                     AS env_label,
    NULL AS runner_label,
    lower(projects_pl.type)       AS pl_category,
    projects_pl.allocation AS pl_percent,
    'projects_pl'          AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('projects_pl') }}

),

repo_storage_pl_daily AS (

  WITH sku_list AS (SELECT 'SSD backed PD Capacity' AS sku
    UNION ALL
    SELECT 'Balanced PD Capacity'
    UNION ALL
    SELECT 'Storage PD Snapshot in US'
    UNION ALL
    SELECT 'Storage PD Capacity')

  SELECT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    'Compute Engine'                           AS gcp_service_description,
    sku_list.sku                               AS gcp_sku_description,
    'gitaly'                                   AS infra_label,
    NULL                                       AS env_label,
    NULL AS runner_label,
    lower(repo_storage_pl_daily.finance_pl)    AS pl_category,
    repo_storage_pl_daily.percent_repo_size_gb AS pl_percent,
    'repo_storage_pl_daily'                    AS from_mapping
  FROM {{ ref ('repo_storage_pl_daily') }}
  CROSS JOIN sku_list
),

sandbox_projects_pl AS (

  SELECT
    date_spine.date_day,
    sandbox_projects_pl.gcp_project_id   AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                     AS env_label,
    NULL AS runner_label,
    lower(sandbox_projects_pl.classification) AS pl_category,
    1                                  AS pl_percent,
    'sandbox_projects_pl'              AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('sandbox_projects_pl') }}
),

container_registry_pl_daily AS (

  SELECT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    'Cloud Storage'                           AS gcp_service_description,
    'Standard Storage US Multi-region'        AS gcp_sku_description,
    'registry'                                   AS infra_label,
    NULL                     AS env_label,
    NULL AS runner_label,
    lower(container_registry_pl_daily.finance_pl)           AS pl_category,
    container_registry_pl_daily.percent_container_registry_size AS pl_percent,
    'container_registry_pl_daily'                               AS from_mapping
  FROM {{ ref ('container_registry_pl_daily') }}
  WHERE snapshot_day > '2022-06-10'

),

build_artifacts_pl_daily AS (

  SELECT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    'Cloud Storage'                           AS gcp_service_description,
    'Standard Storage US Multi-region'        AS gcp_sku_description,
    'build_artifacts'                                   AS infra_label,
    NULL                    AS env_label,
    NULL AS runner_label,
    lower(build_artifacts_pl_daily.finance_pl)           AS pl_category,
    build_artifacts_pl_daily.percent_build_artifacts_size AS pl_percent,
    'build_artifacts_pl_daily'                            AS from_mapping
  FROM {{ ref ('build_artifacts_pl_daily') }}

),

build_artifacts_pl_dev_daily AS (

  SELECT DISTINCT
    snapshot_day                               AS date_day,
    'gitlab-production'                        AS gcp_project_id,
    'Cloud Storage'                           AS gcp_service_description,
    'Standard Storage US Multi-region'        AS gcp_sku_description,
    'build_artifacts'                                   AS infra_label,
    'dev'                                     AS env_label,
    NULL AS runner_label,
    'Internal'                                AS pl_category,
    1 AS pl_percent,
    'build_artifacts_pl_dev_daily'                    AS from_mapping
  FROM {{ ref ('build_artifacts_pl_daily') }}

),

single_sku_pl AS (

  SELECT
    date_spine.date_day,
    NULL   AS gcp_project_id,
    single_sku_pl.service_description        AS gcp_service_description,
    single_sku_pl.sku_description       AS gcp_sku_description,
    NULL                               AS infra_label,
    NULL                    AS env_label,
    NULL AS runner_label,
    lower(single_sku_pl.type) AS pl_category,
    single_sku_pl.allocation             AS pl_percent,
    'single_sku_pl'              AS from_mapping
  FROM date_spine
  CROSS JOIN {{ ref ('single_sku_pl') }}

),

runner_shared_gitlab_org AS (
-- shared gitlab org runner
  SELECT DISTINCT
    reporting_day               AS date_day,
    'gitlab-ci-155816'          AS gcp_project_id,
    NULL                        AS gcp_service_description,
    NULL                        AS gcp_sku_description,
    NULL                        AS infra_label,
    NULL                        AS env_label,
    '1 - shared gitlab org runners' AS runner_label,
    ci_runners_pl_daily.pl      AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 1'        AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  where runner_label = '1 - shared gitlab org runners'

),

runner_saas_small AS (
-- small saas runners in gitlab-ci-plan-free-*
  SELECT DISTINCT
    reporting_day               AS date_day,
    NULL                        AS gcp_project_id,
    NULL                        AS gcp_service_description,
    NULL                        AS gcp_sku_description,
    NULL                        AS infra_label,
    NULL                        AS env_label,
    '2 - shared saas runners - small' AS runner_label,
    ci_runners_pl_daily.pl      AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 2'        AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  where runner_label = '2 - shared saas runners - small'

),

runner_saas_medium AS (

  SELECT DISTINCT
    reporting_day               AS date_day,
    NULL                        AS gcp_project_id,
    NULL                        AS gcp_service_description,
    NULL                        AS gcp_sku_description,
    NULL                        AS infra_label,
    NULL                        AS env_label,
    '3 - shared saas runners - medium' AS runner_label,
    ci_runners_pl_daily.pl      AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 3'        AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  where runner_label = '3 - shared saas runners - medium'

),

runner_saas_large AS (

  SELECT DISTINCT
    reporting_day               AS date_day,
    NULL                        AS gcp_project_id,
    NULL                        AS gcp_service_description,
    NULL                        AS gcp_sku_description,
    NULL                        AS infra_label,
    NULL                        AS env_label,
    '4 - shared saas runners - large' AS runner_label,
    ci_runners_pl_daily.pl      AS pl_category,
    ci_runners_pl_daily.pct_ci_minutes AS pl_percent,
    'ci_runner_pl_daily - 4'        AS from_mapping
  FROM {{ ref ('ci_runners_pl_daily') }}
  where runner_label = '4 - shared saas runners - large'

),

cte_append AS
  (SELECT *
   FROM infralabel_pl
   UNION ALL 
   SELECT *
   FROM projects_pl
   UNION ALL 
   SELECT *
   FROM repo_storage_pl_daily
   UNION ALL 
   SELECT *
   FROM sandbox_projects_pl
   UNION ALL 
   SELECT *
   FROM container_registry_pl_daily
   UNION ALL 
   SELECT *
   FROM build_artifacts_pl_daily
   UNION ALL 
   SELECT *
   FROM build_artifacts_pl_dev_daily
   UNION ALL 
   SELECT *
   FROM single_sku_pl
   UNION ALL
   SELECT *
   FROM runner_shared_gitlab_org
   UNION ALL
   SELECT * 
   FROM runner_saas_small
   UNION ALL
   SELECT *
   FROM runner_saas_medium
   UNION ALL
   SELECT *
   FROM runner_saas_large)

SELECT date_day,
       gcp_project_id,
       gcp_service_description,
       gcp_sku_description,
       infra_label,
       env_label,
       runner_label,
       pl_category,
       pl_percent,
       LISTAGG(DISTINCT from_mapping, ' || ') WITHIN GROUP (
       ORDER BY from_mapping ASC) AS from_mapping
FROM cte_append
{{ dbt_utils.group_by(n=9) }}
