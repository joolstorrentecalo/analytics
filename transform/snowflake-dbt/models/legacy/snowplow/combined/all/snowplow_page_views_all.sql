{{config({
    "materialized":"view"
  })
}}

-- depends_on: {{ ref('snowplow_page_views') }}

{{ schema_union_all('snowplow_', 'snowplow_page_views', database_name=env_var('SNOWFLAKE_PREP_DATABASE'), excluded_col = ['geo_timezone', 'geo_latitude', 'geo_longitude', 'ip_address']) }}
