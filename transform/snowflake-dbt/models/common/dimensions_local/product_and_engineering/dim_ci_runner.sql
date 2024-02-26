WITH prep_ci_runner AS (

  SELECT
    dim_ci_runner_id,
    -- FOREIGN KEYS
    created_date_id,
    created_at,
    updated_at,
    ci_runner_description,

    --- CI Runner Manager Mapping
    CASE
      --- Private Runners
      WHEN ci_runner_description LIKE '%private%manager%'
        THEN 'private-runner-mgr'
      --- Linux Runners
      WHEN ci_runner_description LIKE 'shared-runners-manager%'
        THEN 'linux-runner-mgr'
      WHEN ci_runner_description LIKE '%.shared.runners-manager.%'
        THEN 'linux-runner-mgr'
      WHEN ci_runner_description LIKE '%saas-linux-%-amd64%'
        AND ci_runner_description NOT LIKE '%shell%'
        THEN 'linux-runner-mgr'
      --- Internal GitLab Runners
      WHEN ci_runner_description LIKE 'gitlab-shared-runners-manager%'
        THEN 'gitlab-internal-runner-mgr'
      --- Window Runners 
      WHEN ci_runner_description LIKE 'windows-shared-runners-manager%'
        THEN 'windows-runner-mgr'
      --- Shared Runners
      WHEN ci_runner_description LIKE '%.shared-gitlab-org.runners-manager.%'
        THEN 'shared-gitlab-org-runner-mgr'
      --- macOS Runners
      WHEN ci_runner_description LIKE '%macOS%'
        THEN 'macos-runner-mgr'
      --- Other
      ELSE 'Other'
    END         AS ci_runner_manager,

    --- CI Runner Machine Type Mapping
    CASE
      --- SaaS Linux Runners
      WHEN ci_runner_description LIKE '%.shared.runners-manager.%'
        THEN 'SaaS Runner Linux - Small'
      WHEN ci_runner_description LIKE '%.saas-linux-small-amd64.runners-manager.gitlab.com%'
        THEN 'SaaS Runner Linux - Small'
      WHEN ci_runner_description LIKE '%saas-linux-medium-amd64%'
        THEN 'SaaS Runner Linux - Medium'
      WHEN ci_runner_description LIKE '%saas-linux-large-amd64%'
        THEN 'SaaS Runner Linux - Large'
      WHEN ci_runner_description LIKE '%saas-linux-xlarge-amd64%'
        THEN 'SaaS Runner Linux - XLarge'
      WHEN ci_runner_description LIKE '%saas-linux-2xlarge-amd64%'
        THEN 'SaaS Runner Linux - 2XLarge'
      --- MacOS Runners
      WHEN ci_runner_description LIKE '%macos%'
        THEN 'SaaS Runners macOS - Medium - amd64'
      --- Window Runners 
      WHEN ci_runner_description LIKE 'windows-shared-runners-manager%'
        THEN 'SaaS Runners Windows - Medium'
      ELSE 'Other'
    END         AS ci_runner_machine_type,
    contacted_at,
    is_active,
    ci_runner_version,
    revision,
    platform,
    is_untagged,
    is_locked,
    access_level,
    maximum_timeout,
    runner_type AS ci_runner_type,
    CASE runner_type
      WHEN 1 THEN 'shared'
      WHEN 2 THEN 'group-runner-hosted runners'
      WHEN 3 THEN 'project-runner-hosted runners'
    END         AS ci_runner_type_summary,
    public_projects_minutes_cost_factor,
    private_projects_minutes_cost_factor

  FROM {{ ref('prep_ci_runner') }}

)

{{ dbt_audit(
    cte_ref="prep_ci_runner",
    created_by="@snalamaru",
    updated_by="@nhervas",
    created_date="2021-06-23",
    updated_date="2024-01-30"
) }}
