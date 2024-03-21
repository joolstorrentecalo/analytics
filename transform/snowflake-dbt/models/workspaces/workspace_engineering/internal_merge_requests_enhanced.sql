WITH internal_merge_requests AS (

  SELECT *
  FROM {{ ref('internal_merge_requests') }}

),

cte_ns_explode AS (

  SELECT
    namespace_id,
    ultimate_parent_id,
    upstream_lineage,
    s.value AS lineage_namespace,
    s.index AS rn
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }},
    LATERAL FLATTEN(upstream_lineage, OUTER => TRUE) AS s
  WHERE is_current

),

cte_ns_get_path AS (

  SELECT
    a.namespace_id,
    a.ultimate_parent_id,
    a.upstream_lineage,
    lineage_namespace,
    rn,
    b.namespace_path
  FROM cte_ns_explode AS a
  LEFT JOIN {{ ref('dim_namespace') }} AS b ON a.lineage_namespace = b.dim_namespace_id

),

cte_ns_restructure AS (

  SELECT
    namespace_id,
    ultimate_parent_id,
    upstream_lineage,
    ARRAY_AGG(namespace_path) WITHIN GROUP (ORDER BY rn) AS regroup
  FROM cte_ns_get_path
  GROUP BY
    namespace_id,
    ultimate_parent_id,
    upstream_lineage

),

namespaces AS (

  SELECT
    namespace_id,
    ultimate_parent_id,
    upstream_lineage,
    ARRAY_TO_STRING(regroup, '/') AS full_group_path
  FROM cte_ns_restructure

),

product_categories_yml_base AS (

  SELECT DISTINCT
    LOWER(group_name)                                                         AS group_name,
    LOWER(stage_section)                                                      AS section_name,
    LOWER(stage_display_name)                                                 AS stage_name,
    IFF(group_name LIKE '%::%', SPLIT_PART(LOWER(group_name), '::', 1), NULL) AS root_name
  FROM {{ ref('stages_groups_yaml_source') }}
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {{ ref('stages_groups_yaml_source') }})

),

product_categories_yml AS (

  SELECT
    group_name,
    section_name,
    stage_name
  FROM product_categories_yml_base
  UNION ALL
  SELECT DISTINCT
    root_name AS group_name,
    section_name,
    stage_name
  FROM product_categories_yml_base
  WHERE root_name IS NOT NULL

),

bot_users AS (

  SELECT dim_user_id
  FROM {{ ref('dim_user') }}
  WHERE email_domain LIKE '%noreply.gitlab.com'

),

milestones AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_milestones') }}

),

first_commit AS (

  SELECT
    merge_request_id,
    MIN(created_at) AS first_commit_created_at
  FROM {{ ref('internal_merge_request_diffs') }}
  GROUP BY 1

),

reviews_by_users AS (

  SELECT
    merge_request_id,
    ARRAY_AGG(DISTINCT author_id) AS review_user_id
  FROM {{ ref('wk_gitlab_dotcom_reviews') }}
  GROUP BY 1

),

base AS (

  SELECT
    internal_merge_requests.merge_request_id                                                                                                                                                                                                                                                                            AS merge_request_id,
    internal_merge_requests.merge_request_iid                                                                                                                                                                                                                                                                           AS merge_request_iid,
    internal_merge_requests.author_id                                                                                                                                                                                                                                                                                   AS author_id,
    IFF(
      bots.dim_user_id IS NOT NULL OR internal_merge_requests.author_id = 1786152 OR ARRAY_CONTAINS('automation:bot-authored'::VARIANT, internal_merge_requests.labels),
      TRUE, FALSE
    )                                                                                                                                                                                                                                                                                                                   AS is_created_by_bot,
    internal_merge_requests.assignee_id                                                                                                                                                                                                                                                                                 AS assignee_id,
    internal_merge_requests.project_id                                                                                                                                                                                                                                                                                  AS project_id,
    internal_merge_requests.target_project_id                                                                                                                                                                                                                                                                           AS target_project_id,
    internal_merge_requests.merge_request_state                                                                                                                                                                                                                                                                         AS merge_request_state,
    internal_merge_requests.created_at                                                                                                                                                                                                                                                                                  AS created_at,
    internal_merge_requests.updated_at                                                                                                                                                                                                                                                                                  AS updated_at,
    internal_merge_requests.merged_at                                                                                                                                                                                                                                                                                   AS merged_at,
    DATE_TRUNC('month', internal_merge_requests.created_at)::DATE                                                                                                                                                                                                                                                       AS created_month,
    DATE_TRUNC('month', internal_merge_requests.merged_at)::DATE                                                                                                                                                                                                                                                        AS merge_month,
    ROUND(TIMESTAMPDIFF(HOURS, internal_merge_requests.merge_request_created_at, internal_merge_requests.merged_at) / 24, 2)                                                                                                                                                                                            AS days_to_merge,
    internal_merge_requests.merge_request_title                                                                                                                                                                                                                                                                         AS merge_request_title,
    internal_merge_requests.merge_request_description                                                                                                                                                                                                                                                                   AS merge_request_description,
    internal_merge_requests.milestone_id                                                                                                                                                                                                                                                                                AS milestone_id,
    internal_merge_requests.milestone_title                                                                                                                                                                                                                                                                             AS milestone_title,
    internal_merge_requests.milestone_description                                                                                                                                                                                                                                                                       AS milestone_description,
    milestones.start_date                                                                                                                                                                                                                                                                                               AS milestone_start_date,
    milestones.due_date                                                                                                                                                                                                                                                                                                 AS milestone_due_date,
    internal_merge_requests.namespace_id                                                                                                                                                                                                                                                                                AS namespace_id,
    internal_merge_requests.ultimate_parent_id                                                                                                                                                                                                                                                                          AS ultimate_parent_id,
    internal_merge_requests.labels                                                                                                                                                                                                                                                                                      AS labels,
    ARRAY_TO_STRING(internal_merge_requests.labels, '|')                                                                                                                                                                                                                                                                AS masked_label_title,
    ARRAY_CONTAINS('community contribution'::VARIANT, internal_merge_requests.labels)                                                                                                                                                                                                                                   AS is_community_contribution,
    ARRAY_CONTAINS('security'::VARIANT, internal_merge_requests.labels)                                                                                                                                                                                                                                                 AS is_security,
    COALESCE(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bpriority::([0-9]+)'), 'priority::', ''), 'undefined')                                                                                                                                                                      AS priority_label,
    COALESCE(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bseverity::([0-9]+)'), 'severity::', ''), 'undefined')                                                                                                                                                                      AS severity_label,
    CASE
      WHEN ARRAY_CONTAINS('group::gitaly::cluster'::VARIANT, internal_merge_requests.labels)
        THEN 'gitaly::cluster'
      WHEN ARRAY_CONTAINS('group::gitaly::git'::VARIANT, internal_merge_requests.labels)
        THEN 'gitaly::git'
      WHEN ARRAY_CONTAINS('group::distribution::build'::VARIANT, internal_merge_requests.labels)
        THEN 'distribution::build'
      WHEN ARRAY_CONTAINS('group::distribution::deploy'::VARIANT, internal_merge_requests.labels)
        THEN 'distribution::deploy'
      WHEN ARRAY_CONTAINS('group::distribution::operate'::VARIANT, internal_merge_requests.labels)
        THEN 'distribution::operate'
      ELSE
        IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bgroup::*([^,]*)'), 'group::', '') IN (SELECT group_name FROM product_categories_yml), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bgroup::*([^,]*)'), 'group::', ''), 'undefined')
    END                                                                                                                                                                                                                                                                                                                 AS group_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bsection::*([^,]*)'), 'section::', '') IN (SELECT section_name FROM product_categories_yml), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bsection::*([^,]*)'), 'section::', ''), 'undefined') AS section_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', '') IN (SELECT stage_name FROM product_categories_yml), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', ''), 'undefined')       AS stage_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\btype::*([^,]*)'), 'type::', '') IN ('bug', 'feature', 'maintenance'), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\btype::*([^,]*)'), 'type::', ''), 'undefined')
      AS type_label,
    CASE
      WHEN type_label = 'bug'
        THEN REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bbug::*([^,]*)')
      WHEN type_label = 'maintenance'
        THEN REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bmaintenance::*([^,]*)')
      WHEN type_label = 'feature'
        THEN REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bfeature::*([^,]*)')
      ELSE 'undefined'
    END                                                                                                                                                                                                                                                                                                                 AS subtype_label,
    projects.visibility_level                                                                                                                                                                                                                                                                                           AS visibility_level,
    projects.project_path,
    ns.full_group_path,
    'https://gitlab.com/' || ns.full_group_path || '/' || projects.project_path || '/-/merge_requests/' || internal_merge_requests.merge_request_iid                                                                                                                                                                    AS url,
    IFF(ARRAY_CONTAINS('infradev'::VARIANT, internal_merge_requests.labels), TRUE, FALSE)                                                                                                                                                                                                                               AS is_infradev,
    ARRAY_CONTAINS('customer'::VARIANT, internal_merge_requests.labels)                                                                                                                                                                                                                                                 AS is_customer_related,
    internal_merge_requests.is_part_of_product,
    ROUND(TIMESTAMPDIFF(HOURS, first_commit.first_commit_created_at, internal_merge_requests.merged_at) / 24, 2)                                                                                                                                                                                                        AS days_from_first_commit_to_merge,
    review_user_id
  FROM internal_merge_requests
  LEFT JOIN {{ ref('dim_project') }} AS projects
    ON internal_merge_requests.target_project_id = projects.dim_project_id
  LEFT JOIN bot_users AS bots
    ON internal_merge_requests.author_id = bots.dim_user_id
  LEFT JOIN namespaces AS ns
    ON projects.dim_namespace_id = ns.namespace_id
  LEFT JOIN milestones
    ON internal_merge_requests.milestone_id = milestones.milestone_id
  LEFT JOIN first_commit ON internal_merge_requests.merge_request_id = first_commit.merge_request_id
  LEFT JOIN reviews_by_users ON internal_merge_requests.merge_request_id = reviews_by_users.merge_request_id

)

SELECT *
FROM base
