{{ config({
    "post-hook": "{{ missing_member_column(primary_key = 'dim_installation_id', not_null_test_cols = []) }}"
}) }}

{{ simple_cte([

  ('prep_host', 'prep_host'),
  ('prep_ping_instance', 'prep_ping_instance'),
  ('prep_ping_instance_flattened', 'prep_ping_instance_flattened')]

)}},

installation_agg AS (

  SELECT
    dim_installation_id,
    MIN(TRY_TO_TIMESTAMP(TRIM(metric_value::VARCHAR,'UTC'))) AS installation_creation_date
  FROM prep_ping_instance_flattened
  WHERE ping_created_at > '2023-03-15' --filtering out records that came before GitLab v15.10, when metric was released. Filter in place for full refresh runs.
    AND metrics_path IN (
      'installation_creation_date_approximation',
      'installation_creation_date'
    )
    AND metric_value != 0 -- 0, when cast to timestamp, returns 1970-01-01

    -- filtering out these two installations per this discussion: 
    -- https://gitlab.com/gitlab-data/analytics/-/merge_requests/8416#note_1473684265
    AND dim_installation_id NOT IN (
      'cc89cba853f6caf1a100259b1048704f',
      '5f03898242847ecbaff5b5f8973d5910'
    )
  {{ dbt_utils.group_by(n = 1) }}
),

most_recent_ping AS (

-- Accounting for installation that change from one delivery/deployment type to another.
-- Typically, this is a Self-Managed installation that converts to Dedicated.

  SELECT
    dim_installation_id,
    ping_delivery_type,
    ping_deployment_type
  FROM prep_ping_instance
  QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_installation_id ORDER BY ping_created_at DESC) = 1

),

joined AS (

  SELECT DISTINCT
    -- Primary Key
    prep_ping_instance.dim_installation_id,

    -- Natural Keys 
    prep_ping_instance.dim_instance_id,
    prep_ping_instance.dim_host_id,

    -- Dimensional contexts  
    prep_host.host_name,
    installation_agg.installation_creation_date,
    most_recent_ping.ping_delivery_type   AS product_delivery_type,
    most_recent_ping.ping_deployment_type AS product_deployment_type 

  FROM prep_ping_instance
  INNER JOIN prep_host 
    ON prep_ping_instance.dim_host_id = prep_host.dim_host_id
  LEFT JOIN installation_agg 
    ON prep_ping_instance.dim_installation_id = installation_agg.dim_installation_id
  LEFT JOIN most_recent_ping
    ON most_recent_ping.dim_installation_id = prep_ping_instance.dim_installation_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mdrussell",
    created_date="2021-05-20",
    updated_date="2024-09-09"
) }}
