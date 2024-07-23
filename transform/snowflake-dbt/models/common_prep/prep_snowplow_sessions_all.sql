{{config({
    "materialized":"view",
    "tags":"product"
  })
}}

-- depends_on: {{ ref('snowplow_sessions') }}

WITH unioned_view AS (

{{ schema_union_all('snowplow_', 'snowplow_sessions', database_name=env_var('SNOWFLAKE_PREP_DATABASE'), excluded_col = ['geo_zipcode', 'geo_latitude', 'geo_longitude', 'ip_address']) }}

)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['session_id']) }}                                         AS fct_behavior_website_session_pk,
  app_id,
  session_id,
  {{ dbt_utils.generate_surrogate_key([
    'browser_name',
    'browser_major_version',
    'browser_minor_version',
    'browser_language'])
  }}                                                                                             AS dim_behavior_browser_sk,
  {{ dbt_utils.generate_surrogate_key([
    'os_name',
    'os_timezone'
  ]) }}                                                                                          AS dim_behavior_operating_system_sk,
  session_start,
  session_end,
  session_start                                                                                  AS behavior_at,
  session_start_local,
  session_end_local,
  inferred_user_id,
  user_snowplow_domain_id,
  user_snowplow_crossdomain_id,
  first_gsc_pseudonymized_user_id,
  first_gsc_project_id,
  first_gsc_namespace_id,
  first_gsc_google_analytics_client_id,
  first_gsc_environment,
  first_gsc_is_gitlab_team_member,
  first_gsc_plan,
  first_gsc_source,
  last_gsc_pseudonymized_user_id,
  last_gsc_project_id,
  last_gsc_namespace_id,
  last_gsc_google_analytics_client_id,
  last_gsc_environment,
  last_gsc_is_gitlab_team_member,
  last_gsc_plan,
  last_gsc_source,
  COALESCE(geo_city, 'Unknown')::VARCHAR                                                         AS user_city,
  COALESCE(geo_country, 'Unknown')::VARCHAR                                                      AS user_country,
  COALESCE(geo_region, 'Unknown')::VARCHAR                                                       AS user_region,
  COALESCE(geo_timezone, 'Unknown')::VARCHAR                                                     AS user_timezone_name,
  COALESCE(geo_region_name, 'Unknown')::VARCHAR                                                  AS user_region_name,
  {{ dbt_utils.generate_surrogate_key(['first_page_url', 'app_id', 'first_page_url_scheme']) }}  AS dim_behavior_website_page_sk,
  first_page_title,
  first_page_url,
  first_page_url_fragment,
  first_page_url_host,
  first_page_url_path,
  first_page_url_port,
  first_page_url_query,
  first_page_url_scheme,
  exit_page_url,
  {{ dbt_utils.get_url_parameter(field='first_page_url_query', url_parameter='glm_source') }}    AS glm_source,
  {{ dbt_utils.get_url_parameter(field='first_page_url_query', url_parameter='utm_campaign') }}  AS utm_campaign,
  {{ dbt_utils.get_url_parameter(field='first_page_url_query', url_parameter='utm_content') }}   AS utm_content,
  {{ dbt_utils.get_url_parameter(field='first_page_url_query', url_parameter='utm_medium') }}    AS utm_medium,
  {{ dbt_utils.get_url_parameter(field='first_page_url_query', url_parameter='utm_source') }}    AS utm_source,
  {{ dbt_utils.get_url_parameter(field='first_page_url_query', url_parameter='utm_term') }}      AS utm_term,
  {{ dbt_utils.generate_surrogate_key(['referer_url', 'app_id', 'referer_url_scheme']) }}        AS dim_behavior_referrer_page_sk,
  referer_url,
  referer_url_query,
  referer_url_fragment,
  referer_url_port,
  referer_medium,
  session_index,
  session_cookie_index,
  page_views,
  user_bounced                                                                                   AS is_user_bounced,
  time_engaged_in_s                                                                              AS engaged_seconds,
  time_engaged_in_s_tier                                                                         AS engaged_seconds_range
FROM unioned_view