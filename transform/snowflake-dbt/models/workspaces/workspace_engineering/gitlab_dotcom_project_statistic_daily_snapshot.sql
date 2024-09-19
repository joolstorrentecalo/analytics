{{ config(materialized='table') }}

WITH date_details AS (

    SELECT *
    FROM {{ ref("dim_date") }}
    -- reduce size of results significantly
    WHERE date_actual > '2020-03-01'
      AND date_actual <  {{ dbt.current_timestamp() }}::DATE

), project_snapshots AS (

   SELECT
     *,
     IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
   FROM {{ ref('gitlab_dotcom_project_statistics_snapshots_base') }}
   QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id, valid_from::DATE ORDER BY valid_from DESC) = 1

), project_snapshots_daily AS (

    SELECT
      date_details.date_actual AS snapshot_day,
      project_snapshots.project_statistics_id,
      project_snapshots.project_id,
      project_snapshots.namespace_id,
      project_snapshots.commit_count,
      project_snapshots.storage_size,
      project_snapshots.repository_size,
      project_snapshots.container_registry_size,
      project_snapshots.lfs_objects_size,
      project_snapshots.build_artifacts_size,
      project_snapshots.packages_size,
      project_snapshots.wiki_size,
      project_snapshots.shared_runners_seconds,
      project_snapshots.last_update_started_at
    FROM project_snapshots
    INNER JOIN date_details
      ON date_details.date_actual >= project_snapshots.valid_from::DATE
      AND date_details.date_actual < project_snapshots.valid_to_::DATE

)

SELECT *
FROM project_snapshots_daily
