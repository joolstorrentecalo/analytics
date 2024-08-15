{{ config(
  materialized='table'
) }}


WITH prep_user AS (
  SELECT
    user_id AS gitlab_dotcom_user_id,
    highest_paid_subscription_plan_id,
    account_age,
    is_admin
  FROM {{ ref('prep_user') }}
),

gitlab_dotcom_plans_source AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_plans_source') }}
),

prep_user_plan AS (
  SELECT
    prep_user.*,
    gitlab_dotcom_plans_source.plan_name
  FROM prep_user
  LEFT JOIN gitlab_dotcom_plans_source
    ON prep_user.highest_paid_subscription_plan_id = gitlab_dotcom_plans_source.plan_id
),

mart_marketing_contact AS (
  SELECT
    gitlab_dotcom_user_id,
    email_address,
    pql_namespace_creator_job_description,
    sfdc_parent_sales_segment,
    gitlab_dotcom_last_login_date,
    is_marketo_opted_in,
    has_marketo_unsubscribed,
    gitlab_dotcom_email_opted_in
  FROM {{ ref('mart_marketing_contact') }}
),

mart_marketing_contact_cleaned AS (
  SELECT
    gitlab_dotcom_user_id,
    email_address,
    pql_namespace_creator_job_description AS job_title,
    sfdc_parent_sales_segment             AS sales_segment,
    gitlab_dotcom_last_login_date,
    is_marketo_opted_in,
    has_marketo_unsubscribed,
    gitlab_dotcom_email_opted_in
  FROM mart_marketing_contact
),

joined AS (
  SELECT
    pu.plan_name,
    dmc.gitlab_dotcom_user_id,
    dmc.email_address,
    dmc.job_title,
    pu.account_age,
    pu.is_admin,
    dmc.sales_segment,
    dmc.gitlab_dotcom_last_login_date,
    dmc.is_marketo_opted_in,
    dmc.has_marketo_unsubscribed,
    dmc.gitlab_dotcom_email_opted_in,
    CASE
    -- Check the marketo columns first (SSOT)
      WHEN is_marketo_opted_in = FALSE
        OR has_marketo_unsubscribed = TRUE THEN FALSE
      WHEN is_marketo_opted_in = TRUE
        OR has_marketo_unsubscribed = FALSE THEN TRUE
      -- Check the gitlab columns if the marketo columns are NULL
      WHEN gitlab_dotcom_email_opted_in IS NOT NULL THEN gitlab_dotcom_email_opted_in
      -- Else, CASE will default to NULL
    END AS is_opted_in
  FROM mart_marketing_contact_cleaned AS dmc
  LEFT JOIN prep_user_plan AS pu
    ON dmc.gitlab_dotcom_user_id = pu.gitlab_dotcom_user_id
)

SELECT *
FROM joined
