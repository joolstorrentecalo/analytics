{{ config(
        materialized = "incremental",
        unique_key = "event_id"
) }}

-- depends_on: {{ ref('snowplow_unnested_events') }}

WITH unioned_view AS (

{{ schema_union_limit('snowplow_', 'snowplow_unnested_events', 'derived_tstamp', 750, database_name=env_var('SNOWFLAKE_PREP_DATABASE'), boolean_filter_statement='is_staging_url = TRUE') }}

)

SELECT
  event_id                                                                                                          AS event_id,
  derived_tstamp::TIMESTAMP                                                                                         AS behavior_at,
  {{ dbt_utils.generate_surrogate_key([
    'event', 
    'event_name', 
    'platform', 
    'gsc_environment', 
    'se_category', 
    'se_action', 
    'se_label', 
    'se_property'
    ]) }}                                                                                                           AS dim_behavior_event_sk,
  event                                                                                                             AS event,
  event_name                                                                                                        AS event_name,
  se_action                                                                                                         AS event_action,
  se_category                                                                                                       AS event_category,
  se_label                                                                                                          AS event_label,
  se_property                                                                                                       AS event_property,
  se_value                                                                                                          AS event_value,
  platform                                                                                                          AS platform,
  gsc_pseudonymized_user_id                                                                                         AS gsc_pseudonymized_user_id,
  page_urlhost                                                                                                      AS page_url_host,
  app_id                                                                                                            AS app_id,
  domain_sessionid                                                                                                  AS session_id,
  lc_targeturl                                                                                                      AS link_click_target_url,
  NULLIF(lc_elementid,'')                                                                                           AS link_click_element_id,
  sf_formid                                                                                                         AS submit_form_id,
  cf_formid                                                                                                         AS change_form_id,
  cf_type                                                                                                           AS change_form_type,
  cf_elementid                                                                                                      AS change_form_element_id,
  ff_elementid                                                                                                      AS focus_form_element_id,
  ff_nodename                                                                                                       AS focus_form_node_name,
  br_family                                                                                                         AS browser_name,
  br_name                                                                                                           AS browser_major_version,
  br_version                                                                                                        AS browser_minor_version,
  br_lang                                                                                                           AS browser_language,
  br_renderengine                                                                                                   AS browser_engine,
  {{ dbt_utils.generate_surrogate_key([
    'br_family', 
    'br_name', 
    'br_version', 
    'br_lang']) 
    }}                                                                                                              AS dim_behavior_browser_sk,
  gsc_environment                                                                                                   AS environment,
  v_tracker                                                                                                         AS tracker_version,
  TRY_PARSE_JSON(contexts)::VARIANT                                                                                 AS contexts,
  dvce_created_tstamp::TIMESTAMP                                                                                    AS dvce_created_tstamp,
  collector_tstamp::TIMESTAMP                                                                                       AS collector_tstamp,
  domain_userid                                                                                                     AS user_snowplow_domain_id,
  domain_sessionidx::INT                                                                                            AS session_index,
  REGEXP_REPLACE(page_url, '^https?:\/\/')                                                                          AS page_url_host_path,
  page_urlscheme                                                                                                    AS page_url_scheme,
  page_urlpath                                                                                                      AS page_url_path,
  {{ clean_url('page_urlpath') }} AS clean_url_path,
  page_urlfragment                                                                                                  AS page_url_fragment,
  page_urlquery                                                                                                     AS page_url_query,
  {{ dbt_utils.generate_surrogate_key([
    'page_url_host_path', 
    'app_id', 
    'page_url_scheme'
    ]) }}                                                                                                           AS dim_behavior_website_page_sk,
  gsc_environment                                                                                                   AS gsc_environment,
  gsc_extra                                                                                                         AS gsc_extra,
  gsc_namespace_id                                                                                                  AS gsc_namespace_id,
  gsc_plan                                                                                                          AS gsc_plan,
  gsc_google_analytics_client_id                                                                                    AS gsc_google_analytics_client_id,
  gsc_project_id                                                                                                    AS gsc_project_id,
  gsc_source                                                                                                        AS gsc_source,
  os_name                                                                                                           AS os_name,
  os_timezone                                                                                                       AS os_timezone,
  os_family                                                                                                         AS os,
  os_manufacturer                                                                                                   AS os_manufacturer,
  {{ dbt_utils.generate_surrogate_key([
    'os_name', 
    'os_timezone'
    ]) }}                                                                                                           AS dim_behavior_operating_system_sk,
  dvce_type                                                                                                         AS device_type,
  dvce_ismobile::BOOLEAN                                                                                            AS is_device_mobile,
  refr_medium                                                                                                       AS referrer_medium,
  refr_urlhost                                                                                                      AS referrer_url_host,
  refr_urlpath                                                                                                      AS referrer_url_path,
  refr_urlscheme                                                                                                    AS referrer_url_scheme,
  refr_urlquery                                                                                                     AS referrer_url_query,
  REGEXP_REPLACE(page_referrer, '^https?:\/\/')                                                                     AS referrer_url_host_path,
  {{ dbt_utils.generate_surrogate_key([
    'referrer_url_host_path', 
    'app_id', 
    'referrer_url_scheme'
    ]) }}                                                                                                           AS dim_behavior_referrer_page_sk,
  IFNULL(geo_city, 'Unknown')::VARCHAR                                                                              AS user_city,
  IFNULL(geo_country, 'Unknown')::VARCHAR                                                                           AS user_country,
  IFNULL(geo_region, 'Unknown')::VARCHAR                                                                            AS user_region,
  IFNULL(geo_region_name, 'Unknown')::VARCHAR                                                                       AS user_region_name,
  IFNULL(geo_timezone, 'Unknown')::VARCHAR                                                                          AS user_timezone_name,
  {{ dbt_utils.generate_surrogate_key([
    'user_city', 
    'user_country',
    'user_region',
    'user_timezone_name'
    ]) }}                                                                                                           AS dim_user_location_sk,
  COALESCE(CONTAINS(contexts, 'iglu:org.w3/PerformanceTiming/'), FALSE)::BOOLEAN                                    AS has_performance_timing_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.snowplowanalytics.snowplow/web_page/'), FALSE)::BOOLEAN                     AS has_web_page_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/ci_build_failed/'), FALSE)::BOOLEAN                                  AS has_ci_build_failed_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/wiki_page_context/'), FALSE)::BOOLEAN                                AS has_wiki_page_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/gitlab_standard/'), FALSE)::BOOLEAN                                  AS has_gitlab_standard_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/email_campaigns/'), FALSE)::BOOLEAN                                  AS has_email_campaigns_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/gitlab_service_ping/'), FALSE)::BOOLEAN                              AS has_gitlab_service_ping_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/design_management_context/'), FALSE)::BOOLEAN                        AS has_design_management_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/customer_standard/'), FALSE)::BOOLEAN                                AS has_customer_standard_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/secure_scan/'), FALSE)::BOOLEAN                                      AS has_secure_scan_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/gitlab_experiment/'), FALSE)::BOOLEAN                                AS has_gitlab_experiment_context,
  COALESCE(CONTAINS(contexts, 'iglu:com.gitlab/subscription_auto_renew/'), FALSE)::BOOLEAN                          AS has_subscription_auto_renew_context,
  {{ dbt_utils.generate_surrogate_key([
    'has_performance_timing_context', 
    'has_web_page_context',
    'has_ci_build_failed_context',
    'has_wiki_page_context',
    'has_gitlab_standard_context',
    'has_email_campaigns_context',
    'has_gitlab_service_ping_context',
    'has_design_management_context',
    'has_customer_standard_context',
    'has_secure_scan_context',
    'has_gitlab_experiment_context', 
    'has_subscription_auto_renew_context'
    ]) }}                                                                                                           AS dim_behavior_contexts_sk
FROM unioned_view
