WITH source AS (

  SELECT *
  FROM {{ source('google_analytics_4_bigquery','events') }}

),

flattened AS (

  SELECT

    value['date_part']::NUMBER                                              AS date_part_nodash,
    value['device']['category']::VARCHAR                                    AS device_category,
    value['device']['is_limited_ad_tracking']::VARCHAR                      AS device_is_limited_ad_tracking,
    value['device']['language']::VARCHAR                                    AS device_language,
    value['device']['operating_system_version']::VARCHAR                    AS device_operating_system_version,
    value['device']['web_info']['browser']::VARCHAR                         AS device_web_info_browser,
    value['device']['web_info']['browser_version']::VARCHAR                 AS device_web_info_browser_version,
    value['device']['web_info']['hostname']::VARCHAR                        AS device_web_info_hostname,
    value['event_bundle_sequence_id']::NUMBER                               AS event_bundle_sequence_id,
    value['event_date']::NUMBER                                             AS event_date,
    value['event_name']::VARCHAR                                            AS event_name,
    parse_ga4_objarray(value['event_params'])                               AS variant__event_params,
    parse_ga4_objarray(value['event_params'])['batch_ordering_id']          AS batch_ordering_id,
    parse_ga4_objarray(value['event_params'])['batch_page_id']              AS batch_page_id,
    parse_ga4_objarray(value['event_params'])['campaign']                   AS campaign,
    parse_ga4_objarray(value['event_params'])['engaged_session_event']      AS engaged_session_event,
    parse_ga4_objarray(value['event_params'])['engagement_time_msec']       AS engagement_time_msec,
    parse_ga4_objarray(value['event_params'])['ga_session_id']              AS ga_session_id,
    parse_ga4_objarray(value['event_params'])['ga_session_number']          AS ga_session_number,
    parse_ga4_objarray(value['event_params'])['medium']                     AS medium,
    parse_ga4_objarray(value['event_params'])['page_exclude_localization']  AS page_exclude_localization,
    parse_ga4_objarray(value['event_params'])['page_location']              AS page_location,
    parse_ga4_objarray(value['event_params'])['page_referrer']              AS page_referrer,
    parse_ga4_objarray(value['event_params'])['page_title']                 AS page_title,
    parse_ga4_objarray(value['event_params'])['session_engaged']            AS session_engaged,
    parse_ga4_objarray(value['event_params'])['source']                     AS source,
    parse_ga4_objarray(value['event_params'])['term']                       AS term,
    TO_TIMESTAMP(value['event_timestamp']::VARCHAR)                         AS event_timestamp,
    TO_TIMESTAMP(value['gcs_export_time']::VARCHAR)                         AS gcs_export_time,
    value['geo']['city']::VARCHAR                                           AS geo_city,
    value['geo']['continent']::VARCHAR                                      AS geo_continent,
    value['geo']['country']::VARCHAR                                        AS geo_country,
    value['geo']['metro']::VARCHAR                                          AS geo_metro,
    value['geo']['region']::VARCHAR                                         AS geo_region,
    value['geo']['sub_continent']::VARCHAR                                  AS geo_sub_continent,
    value['is_active_user']::BOOLEAN                                        AS is_active_user,
    value['items'][0]::VARIANT                                              AS variant__items,
    value['platform']::VARCHAR                                              AS platform,
    value['privacy_info']::VARIANT                                          AS variant__privacy_info,
    value['stream_id']::NUMBER                                              AS stream_id,
    value['traffic_source']['medium']::VARCHAR                              AS traffic_source_medium,
    value['traffic_source']['name']::VARCHAR                                AS traffic_source_name,
    value['traffic_source']['source']::VARCHAR                              AS traffic_source_source,
    TO_TIMESTAMP(value['user_first_touch_timestamp']::VARCHAR)              AS user_first_touch_timestamp,
    value['user_ltv']['currency']::VARCHAR                                  AS user_ltv_currency,
    value['user_ltv']['revenue']::VARCHAR                                   AS user_ltv_revenue,
    parse_ga4_objarray(value['user_properties']::VARIANT)                   AS variant__user_properties,
    parse_ga4_objarray(value['user_properties'])['ssense_employee_range']   AS ssense_employee_range,
    parse_ga4_objarray(value['user_properties'])['ssense_confidence']       AS ssense_confidence,
    parse_ga4_objarray(value['user_properties'])['ssense_country']          AS ssense_country,
    parse_ga4_objarray(value['user_properties'])['ssense_blacklisted']      AS ssense_blacklisted,
    parse_ga4_objarray(value['user_properties'])['ssense_sales_segment']    AS ssense_sales_segment,
    parse_ga4_objarray(value['user_properties'])['ssense_company']          AS ssense_company,
    parse_ga4_objarray(value['user_properties'])['ssense_industry']         AS ssense_industry,
    parse_ga4_objarray(value['user_properties'])['ssense_revenue_range']    AS ssense_revenue_range,
    parse_ga4_objarray(value['user_properties'])['browser_width_height']    AS browser_width_height,
    value['user_pseudo_id']::VARCHAR                                        AS user_pseudo_id,
    date_part::DATE                                                         AS date_part

  FROM source

  {% if is_incremental() %}

    WHERE date_part >= (SELECT MAX(date_part) FROM {{ this }})

  {% endif %}

)

SELECT *
FROM flattened