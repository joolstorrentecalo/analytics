{{ config(
    tags=["product"]
) }}

{{ simple_cte([
  ('cluster_agents','gitlab_dotcom_cluster_agents_source'),
    ('prep_project', 'prep_project')
  ])
}}

, renamed AS (

    SELECT
      cluster_agents.cluster_agent_id,
      prep_project.dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      cluster_agents.created_by_user_id AS dim_user_id,
      cluster_agents.cluster_agent_name,
      cluster_agents.created_at,
      cluster_agents.updated_at
    FROM cluster_agents
    LEFT JOIN prep_project
      ON cluster_agents.project_id = prep_project.dim_project_id
      
)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2022-11-16",
    updated_date="2022-11-16"
) }}
