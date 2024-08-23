{{ config({
    "materialized": "incremental",
    "unique_key": "note_id"
    })
}}

WITH all_notes AS (

  SELECT
    id::NUMBER                                            AS note_id,
    note::VARCHAR                                         AS note,
    IFF(noteable_type = '', NULL, noteable_type)::VARCHAR AS noteable_type,
    author_id::NUMBER                                     AS note_author_id,
    created_at::TIMESTAMP                                 AS created_at,
    updated_at::TIMESTAMP                                 AS updated_at,
    project_id::NUMBER                                    AS project_id,
    attachment::VARCHAR                                   AS attachment,
    line_code::VARCHAR                                    AS line_code,
    commit_id::VARCHAR                                    AS commit_id,
    noteable_id::NUMBER                                   AS noteable_id,
    system::BOOLEAN                                       AS system,
    --st_diff (hidden because not relevant to our current analytics needs)
    updated_by_id::NUMBER                                 AS note_updated_by_id,
    --type (hidden because legacy and can be easily confused with noteable_type)
    position::VARCHAR                                     AS position,
    original_position::VARCHAR                            AS original_position,
    resolved_at::TIMESTAMP                                AS resolved_at,
    resolved_by_id::NUMBER                                AS resolved_by_id,
    discussion_id::VARCHAR                                AS discussion_id,
    cached_markdown_version::NUMBER                       AS cached_markdown_version,
    resolved_by_push::BOOLEAN                             AS resolved_by_push,
    note_html::VARCHAR                                    AS note_html,
    pgp_is_deleted::BOOLEAN                               AS pgp_is_deleted,
    pgp_is_deleted_updated_at::TIMESTAMP                  AS pgp_is_deleted_updated_at
  FROM {{ ref('gitlab_dotcom_notes_dedupe_source') }}
  WHERE note_id NOT IN (203215238) --https://gitlab.com/gitlab-data/analytics/merge_requests/1423
),

internal_notes AS (
  SELECT
    id::NUMBER            AS internal_note_id,
    updated_at::TIMESTAMP AS internal_note_updated_at,
    project_id::NUMBER    AS internal_note_project_id,
    line_code::VARCHAR    AS internal_line_code,
    note::VARCHAR         AS internal_note,
    note_html::VARCHAR    AS internal_note_html
  FROM {{ ref('gitlab_dotcom_notes_internal_only_dedupe_source') }}
),

combined AS (
  SELECT
    all_notes.note_id,
    internal_notes.internal_note      AS note,
    all_notes.noteable_type,
    all_notes.note_author_id,
    all_notes.created_at,
    all_notes.updated_at,
    all_notes.project_id,
    all_notes.attachment,
    internal_notes.internal_line_code AS line_code,
    all_notes.commit_id,
    all_notes.noteable_id,
    all_notes.system,
    all_notes.note_updated_by_id,
    all_notes.position,
    all_notes.original_position,
    all_notes.resolved_at,
    all_notes.resolved_by_id,
    all_notes.discussion_id,
    all_notes.cached_markdown_version,
    all_notes.resolved_by_push,
    internal_notes.internal_note_html AS note_html,
    all_notes.pgp_is_deleted,
    all_notes.pgp_is_deleted_updated_at
  FROM all_notes
  LEFT JOIN internal_notes
    ON all_notes.note_id = internal_notes.internal_note_id
)

SELECT * FROM combined
