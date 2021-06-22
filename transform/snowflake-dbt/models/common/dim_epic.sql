WITH prep_epic AS (

    SELECT 
      dim_epic_id,
      author_id,
      group_id,
      ultimate_parent_namespace_id,
      creation_date_id,
      dim_plan_id,
      assignee_id,
      epic_internal_id,
      updated_by_id,
      last_edited_by_id,
      lock_version,
      epic_start_date,
      epic_end_date,
      epic_last_edited_at,
      created_at,
      updated_at,
      epic_title,
      epic_description,
      closed_at,
      state_id,
      parent_id,
      relative_position,
      start_date_sourcing_epic_id,
      external_key,
      is_confidential,
      state,
      epic_title_length,
      epic_description_length
    FROM {{ ref('prep_epic') }}

)

{{ dbt_audit(
    cte_ref="prep_epic",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-22",
    updated_date="2021-06-22"
) }}
