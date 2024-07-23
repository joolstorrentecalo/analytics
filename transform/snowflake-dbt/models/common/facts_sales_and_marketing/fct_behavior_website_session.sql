{{ config(
        materialized = "incremental",
        unique_key = "fct_behavior_website_session_pk",
        on_schema_change='sync_all_columns',
        tags=['product'],
        cluster_by=['behavior_at::DATE']
) }}

{{ simple_cte([
    ('sessions', 'prep_snowplow_sessions_all')
    ])
}}


SELECT
  -- Primary Key
  fct_behavior_website_session_pk,

  -- Natural Keys
  app_id,
  session_id,

  -- Foreign Keys
  dim_behavior_browser_sk,
  dim_behavior_operating_system_sk,
  dim_behavior_website_page_sk,
  dim_behavior_referrer_page_sk,

  --Time Attributes
  behavior_at,
  session_start,
  session_end,
  session_start_local,
  session_end_local,

  -- User Attributes
  inferred_user_id,
  user_snowplow_domain_id,
  user_snowplow_crossdomain_id,

  -- First Gitlab Standard Context Attributes
  first_gsc_pseudonymized_user_id,
  first_gsc_project_id,
  first_gsc_namespace_id,
  first_gsc_google_analytics_client_id,
  first_gsc_environment,
  first_gsc_is_gitlab_team_member,
  first_gsc_plan,
  first_gsc_source,

  -- Last Gitlab Standard Context Attributes
  last_gsc_pseudonymized_user_id,
  last_gsc_project_id,
  last_gsc_namespace_id,
  last_gsc_google_analytics_client_id,
  last_gsc_environment,
  last_gsc_is_gitlab_team_member,
  last_gsc_plan,
  last_gsc_source,

  -- User Location Attributes
  user_city,
  user_country,
  user_region,
  user_region_name,
  user_timezone_name,

  -- First page Attributes
  first_page_title,
  first_page_url,
  first_page_url_fragment,
  first_page_url_host,
  first_page_url_path,
  first_page_url_port,
  first_page_url_query,
  first_page_url_scheme,

  exit_page_url,

  -- UTM Attributes
  glm_source,
  utm_campaign,
  utm_content,
  utm_medium,
  utm_source,
  utm_term,

  -- Referer Attributes
  referer_url,
  referer_url_query,
  referer_url_fragment,
  referer_url_port,
  referer_medium,

  -- Session Attributes
  session_index,
  session_cookie_index,
  page_views,
  is_user_bounced,
  engaged_seconds,
  engaged_seconds_range
FROM sessions

{% if is_incremental() %}

WHERE session_end > (SELECT max(session_end) FROM {{ this }})

{% endif %}
