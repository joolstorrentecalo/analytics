{{ config(
    tags=["mnpi_exception"]
) }}

WITH epic_issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_epic_issues') }}

), epics AS (

    SELECT *
    FROM {{ ref('prep_epic') }}

), gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id') }}

), gitlab_dotcom_notes_linked_to_sfdc_account_id AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes_linked_to_sfdc_account_id') }}

), issues AS (

    SELECT *
    FROM {{ ref('prep_issue') }}

), projects AS (

    SELECT *
    FROM {{ ref('prep_project') }}

), namespaces AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}
    WHERE is_currently_valid = TRUE

), sfdc_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_opportunities AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}

), milestones AS (

    SELECT *
    FROM {{ ref('prep_milestone') }}

/* Created 4 Separate CTEs to be unioned */

), sfdc_accounts_from_issue_notes AS (

    SELECT DISTINCT
      'Issue'                    AS noteable_type,
      'Note'                     AS mention_type,
      issues.issue_id            AS noteable_id,
      issues.issue_internal_id   AS noteable_iid,
      issues.issue_title         AS noteable_title,
      issues.created_at          AS noteable_created_at,
      milestones.milestone_id,
      issues.issue_state         AS noteable_state,
      issues.weight,
      issues.labels,
      projects.project_name,
      projects.project_id,
      namespaces.namespace_id,
      namespaces.namespace_name,
      sfdc_accounts.account_id   AS sfdc_account_id,
      sfdc_accounts.account_type AS sfdc_account_type,
      sfdc_accounts.carr_this_account,
      sfdc_accounts.carr_account_family,
      epics.epic_title
    FROM gitlab_dotcom_notes_linked_to_sfdc_account_id
    INNER JOIN issues
      ON gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_id = issues.issue_id
    LEFT JOIN projects
      ON issues.dim_project_sk = projects.dim_project_sk
    LEFT JOIN namespaces
      ON projects.dim_namespace_sk = namespaces.dim_namespace_sk
    LEFT JOIN sfdc_accounts
      ON gitlab_dotcom_notes_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id
    LEFT JOIN epic_issues
      ON issues.issue_id = epic_issues.issue_id
    LEFT JOIN epics
      ON epic_issues.epic_id = epics.epic_id
    LEFT JOIN milestones
      ON issues.dim_milestone_sk = milestones.dim_milestone_sk
    WHERE gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_type = 'Issue'

), sfdc_accounts_from_epic_notes AS (

    SELECT DISTINCT
      'Epic'                     AS noteable_type,
      'Note'                     AS mention_type,
      epics.epic_id              AS noteable_id,
      epics.epic_internal_id     AS noteable_iid,
      epics.epic_title           AS noteable_title,
      epics.created_at           AS noteable_created_at,
      NULL                       AS milestone_id,
      epics.epic_state           AS epic_state,
      NULL                       AS weight,
      epics.labels               AS labels,
      NULL                       AS project_name,
      NULL                       AS project_id,
      namespaces.namespace_id,
      namespaces.namespace_name,
      sfdc_accounts.account_id   AS sfdc_account_id,
      sfdc_accounts.account_type AS sfdc_account_type,
      sfdc_accounts.carr_this_account,
      sfdc_accounts.carr_account_family,
      epics.epic_title --Redundant in this case.
    FROM gitlab_dotcom_notes_linked_to_sfdc_account_id
    INNER JOIN epics
      ON gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_id = epics.epic_id
    LEFT JOIN namespaces
      ON epics.dim_namespace_sk = namespaces.dim_namespace_sk
    LEFT JOIN sfdc_accounts
      ON gitlab_dotcom_notes_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id
    WHERE gitlab_dotcom_notes_linked_to_sfdc_account_id.noteable_type = 'Epic'

), sfdc_accounts_from_issue_descriptions AS (

    SELECT DISTINCT
      'Issue'         AS noteable_type,
      'Description'   AS mention_type,
      issues.issue_id,
      issues.issue_internal_id,
      issues.issue_title,
      issues.created_at,
      milestones.milestone_id,
      issues.issue_state AS issue_state,
      issues.weight,
      issues.labels,
      projects.project_name,
      projects.project_id,
      namespaces.namespace_id,
      namespaces.namespace_name,
      sfdc_accounts.account_id AS sfdc_account_id,
      sfdc_accounts.account_type AS sfdc_account_type,
      sfdc_accounts.carr_this_account,
      sfdc_accounts.carr_account_family,
      epics.epic_title
    FROM gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id
    INNER JOIN issues
      ON gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_id = issues.issue_id
      AND gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_type = 'Issue'
    LEFT JOIN projects
      ON issues.dim_project_sk = projects.dim_project_sk
    LEFT JOIN namespaces
      ON projects.dim_namespace_sk = namespaces.dim_namespace_sk
    LEFT JOIN sfdc_accounts
      ON gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id
    LEFT JOIN epic_issues
      ON issues.issue_id = epic_issues.issue_id
    LEFT JOIN epics
      ON epic_issues.epic_id = epics.epic_id
    LEFT JOIN milestones
      ON issues.dim_milestone_sk = milestones.dim_milestone_sk

), sfdc_accounts_from_epic_descriptions AS (

    SELECT DISTINCT
      'Epic'                     AS noteable_type,
      'Description'              AS mention_type,
      epics.epic_id              AS noteable_id,
      epics.epic_internal_id     AS noteable_iid,
      epics.epic_title           AS noteable_title,
      epics.created_at           AS noteable_created_at,
      NULL                       AS milestone_id,
      epics.epic_state           AS epic_state,
      NULL                       AS weight,
      epics.labels               AS labels,
      NULL                       AS project_name,
      NULL                       AS project_id,
      namespaces.namespace_id,
      namespaces.namespace_name,
      sfdc_accounts.account_id   AS sfdc_account_id,
      sfdc_accounts.account_type AS sfdc_account_type,
      sfdc_accounts.carr_this_account,
      sfdc_accounts.carr_account_family,
      epics.epic_title --Redundant in this case.
    FROM gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id
    INNER JOIN epics
      ON gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_id = epics.epic_id
      AND gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.noteable_type = 'Epic'
    LEFT JOIN namespaces
      ON epics.dim_namespace_sk = namespaces.dim_namespace_sk
    LEFT JOIN sfdc_accounts
      ON gitlab_dotcom_issues_and_epics_linked_to_sfdc_account_id.sfdc_account_id = sfdc_accounts.account_id

), unioned AS (

    /* Notes */
    SELECT *
    FROM sfdc_accounts_from_issue_notes

    UNION

    SELECT *
    FROM sfdc_accounts_from_epic_notes

    /* Descriptions */
    UNION

    SELECT *
    FROM sfdc_accounts_from_issue_descriptions

    UNION

    SELECT *
    FROM sfdc_accounts_from_epic_descriptions

)

SELECT *
FROM unioned
WHERE sfdc_account_id IS NOT NULL
