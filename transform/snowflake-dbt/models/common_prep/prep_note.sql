{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_note_id",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_epic', 'prep_epic'),
    ('prep_namespace', 'prep_namespace'),
    ('gitlab_dotcom_system_note_metadata_source', 'gitlab_dotcom_system_note_metadata_source')
]) }},

gitlab_dotcom_notes_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notes_source') }}
  {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})

  {% endif %}

),

joined AS (

  SELECT
    gitlab_dotcom_notes_source.note_id                                      AS dim_note_id,
    gitlab_dotcom_notes_source.note_author_id                               AS author_id,
    gitlab_dotcom_notes_source.project_id                                   AS dim_project_id,
    COALESCE(
      prep_project.ultimate_parent_namespace_id,
      prep_namespace.ultimate_parent_namespace_id
    )                                                                       AS ultimate_parent_namespace_id,
    gitlab_dotcom_notes_source.noteable_id,
    prep_date.date_id                                                       AS created_date_id,
    COALESCE(prep_namespace_plan_hist.dim_plan_id, 34)                      AS dim_plan_id,
    IFF(gitlab_dotcom_notes_source.noteable_type = '', NULL, noteable_type) AS noteable_type,
    gitlab_dotcom_notes_source.created_at,
    gitlab_dotcom_notes_source.updated_at,
    gitlab_dotcom_notes_source.note,
    gitlab_dotcom_notes_source.attachment,
    gitlab_dotcom_notes_source.line_code,
    gitlab_dotcom_notes_source.commit_id,
    gitlab_dotcom_notes_source.system                                       AS is_system_note,
    gitlab_dotcom_notes_source.note_updated_by_id,
    gitlab_dotcom_notes_source.position                                     AS position_number,
    gitlab_dotcom_notes_source.original_position,
    gitlab_dotcom_notes_source.resolved_at,
    gitlab_dotcom_notes_source.resolved_by_id,
    gitlab_dotcom_notes_source.discussion_id,
    gitlab_dotcom_notes_source.cached_markdown_version,
    gitlab_dotcom_notes_source.resolved_by_push,
    gitlab_dotcom_system_note_metadata_source.action_type,
    gitlab_dotcom_notes_source.pgp_is_deleted                               AS is_deleted,
    gitlab_dotcom_notes_source.pgp_is_deleted_updated_at                    AS is_deleted_updated_at
  FROM gitlab_dotcom_notes_source
  INNER JOIN prep_date
    ON gitlab_dotcom_notes_source.created_at = prep_date.date_day
  LEFT JOIN prep_project
    ON gitlab_dotcom_notes_source.project_id = prep_project.dim_project_id
  LEFT JOIN prep_epic
    ON gitlab_dotcom_notes_source.noteable_id = prep_epic.epic_id
  LEFT JOIN prep_namespace
    ON prep_epic.prep_namespace_sk = prep_namespace.prep_namespace_sk
  LEFT JOIN prep_namespace_plan_hist
    ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.prep_namespace_id
      AND gitlab_dotcom_notes_source.created_at >= prep_namespace_plan_hist.valid_from
      AND gitlab_dotcom_notes_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
  LEFT JOIN gitlab_dotcom_system_note_metadata_source
    ON gitlab_dotcom_notes_source.note_id = gitlab_dotcom_system_note_metadata_source.note_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@lisvinueza",
    created_date="2021-06-22",
    updated_date="2024-08-23"
) }}

