WITH source AS (

  SELECT *
  FROM {{ source('google_analytics_4_bigquery','events') }}

),

flattened AS (

  SELECT

    value['date_part']::NUMBER                                 AS date_part_nodash,
    value['device']['category']::VARCHAR                       AS device_category,
    value['device']['is_limited_ad_tracking']::VARCHAR         AS device_is_limited_ad_tracking,
    value['device']['language']::VARCHAR                       AS device_language,
    value['device']['operating_system_version']::VARCHAR       AS device_operating_system_version,
    value['device']['web_info']['browser']::VARCHAR            AS device_web_info_browser,
    value['device']['web_info']['browser_version']::VARCHAR    AS device_web_info_browser_version,
    value['device']['web_info']['hostname']::VARCHAR           AS device_web_info_hostname,
    value['event_bundle_sequence_id']::NUMBER                  AS event_bundle_sequence_id,
    value['event_date']::NUMBER                                AS event_date,
    value['event_name']::VARCHAR                               AS event_name,
    value['event_params']::VARIANT                             AS variant__event_params,
    TO_TIMESTAMP(value['event_timestamp']::VARCHAR)            AS event_timestamp,
    TO_TIMESTAMP(value['gcs_export_time']::VARCHAR)            AS gcs_export_time,
    value['geo']['city']::VARCHAR                              AS geo_city,
    value['geo']['continent']::VARCHAR                         AS geo_continent,
    value['geo']['country']::VARCHAR                           AS geo_country,
    value['geo']['metro']::VARCHAR                             AS geo_metro,
    value['geo']['region']::VARCHAR                            AS geo_region,
    value['geo']['sub_continent']::VARCHAR                     AS geo_sub_continent,
    value['is_active_user']::BOOLEAN                           AS is_active_user,
    value['items'][0]::VARIANT                                 AS variant__items,
    value['platform']::VARCHAR                                 AS platform,
    value['privacy_info']::VARIANT                             AS variant__privacy_info,
    value['stream_id']::NUMBER                                 AS stream_id,
    value['traffic_source']['medium']::VARCHAR                 AS traffic_source_medium,
    value['traffic_source']['name']::VARCHAR                   AS traffic_source_name,
    value['traffic_source']['source']::VARCHAR                 AS traffic_source_source,
    TO_TIMESTAMP(value['user_first_touch_timestamp']::VARCHAR) AS user_first_touch_timestamp,
    value['user_ltv']['currency']::VARCHAR                     AS user_ltv_currency,
    value['user_ltv']['revenue']::VARCHAR                      AS user_ltv_revenue,
    value['user_properties']::VARIANT                          AS variant__user_properties,
    value['user_pseudo_id']::VARCHAR                           AS user_pseudo_id,
    date_part::DATE                                            AS date_part

  FROM source

  {% if is_incremental() %}

    WHERE date_part >= (SELECT MAX(date_part) FROM {{ this }})

  {% endif %}

)

SELECT *
FROM flattened
