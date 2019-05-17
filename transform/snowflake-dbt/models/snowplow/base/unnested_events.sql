{{
  config(
    materialized='incremental',
    unique_key='event_id'
  )
}}

with base as (
SELECT
    nullif(JSONTEXT['app_id']::string,'') AS app_id,
    nullif(JSONTEXT['base_currency']::string,'') AS base_currency,
    nullif(JSONTEXT['br_colordepth']::string,'') AS br_colordepth,
    nullif(JSONTEXT['br_cookies']::string,'') AS br_cookies,
    nullif(JSONTEXT['br_family']::string,'') AS br_family,
    nullif(JSONTEXT['br_features_director']::string,'') AS br_features_director,
    nullif(JSONTEXT['br_features_flash']::string,'') AS br_features_flash,
    nullif(JSONTEXT['br_features_gears']::string,'') AS br_features_gears,
    nullif(JSONTEXT['br_features_java']::string,'') AS br_features_java,
    nullif(JSONTEXT['br_features_pdf']::string,'') AS br_features_pdf,
    nullif(JSONTEXT['br_features_quicktime']::string,'') AS br_features_quicktime,
    nullif(JSONTEXT['br_features_realplayer']::string,'') AS br_features_realplayer,
    nullif(JSONTEXT['br_features_silverlight']::string,'') AS br_features_silverlight,
    nullif(JSONTEXT['br_features_windowsmedia']::string,'') AS br_features_windowsmedia,
    nullif(JSONTEXT['br_lang']::string,'') AS br_lang,
    nullif(JSONTEXT['br_name']::string,'') AS br_name,
    nullif(JSONTEXT['br_renderengine']::string,'') AS br_renderengine,
    nullif(JSONTEXT['br_type']::string,'') AS br_type,
    nullif(JSONTEXT['br_version']::string,'') AS br_version,
    nullif(JSONTEXT['br_viewheight']::string,'') AS br_viewheight,
    nullif(JSONTEXT['br_viewwidth']::string,'') AS br_viewwidth,
    nullif(JSONTEXT['collector_tstamp']::string,'') AS collector_tstamp,
    nullif(JSONTEXT['contexts']::string,'') AS contexts,
    nullif(JSONTEXT['derived_contexts']::string,'') AS derived_contexts,
    nullif(JSONTEXT['derived_tstamp']::string,'') AS derived_tstamp,
    nullif(JSONTEXT['doc_charset']::string,'') AS doc_charset,
    try_to_numeric(JSONTEXT['doc_height']::string) AS doc_height,
    try_to_numeric(JSONTEXT['doc_width']::string) AS doc_width,
    nullif(JSONTEXT['domain_sessionid']::string,'') AS domain_sessionid,
    nullif(JSONTEXT['domain_sessionidx']::string,'') AS domain_sessionidx,
    nullif(JSONTEXT['domain_userid']::string,'') AS domain_userid,
    nullif(JSONTEXT['dvce_created_tstamp']::string,'') AS dvce_created_tstamp,
    nullif(JSONTEXT['dvce_ismobile']::string,'') AS dvce_ismobile,
    nullif(JSONTEXT['dvce_screenheight']::string,'') AS dvce_screenheight,
    nullif(JSONTEXT['dvce_screenwidth']::string,'') AS dvce_screenwidth,
    nullif(JSONTEXT['dvce_sent_tstamp']::string,'') AS dvce_sent_tstamp,
    nullif(JSONTEXT['dvce_type']::string,'') AS dvce_type,
    nullif(JSONTEXT['etl_tags']::string,'') AS etl_tags,
    nullif(JSONTEXT['etl_tstamp']::string,'') AS etl_tstamp,
    nullif(JSONTEXT['event']::string,'') AS event,
    nullif(JSONTEXT['event_fingerprint']::string,'') AS event_fingerprint,
    nullif(JSONTEXT['event_format']::string,'') AS event_format,
    nullif(JSONTEXT['event_id']::string,'') AS event_id,
    left(right(JSONTEXT['contexts'], 41), 36) as web_page_id,
    nullif(JSONTEXT['event_name']::string,'') AS event_name,
    nullif(JSONTEXT['event_vendor']::string,'') AS event_vendor,
    nullif(JSONTEXT['event_version']::string,'') AS event_version,
    nullif(JSONTEXT['geo_city']::string,'') AS geo_city,
    nullif(JSONTEXT['geo_country']::string,'') AS geo_country,
    nullif(JSONTEXT['geo_latitude']::string,'') AS geo_latitude,
    nullif(JSONTEXT['geo_longitude']::string,'') AS geo_longitude,
    nullif(JSONTEXT['geo_region']::string,'') AS geo_region,
    nullif(JSONTEXT['geo_region_name']::string,'') AS geo_region_name,
    nullif(JSONTEXT['geo_timezone']::string,'') AS geo_timezone,
    nullif(JSONTEXT['geo_zipcode']::string,'') AS geo_zipcode,
    nullif(JSONTEXT['ip_domain']::string,'') AS ip_domain,
    nullif(JSONTEXT['ip_isp']::string,'') AS ip_isp,
    nullif(JSONTEXT['ip_netspeed']::string,'') AS ip_netspeed,
    nullif(JSONTEXT['ip_organization']::string,'') AS ip_organization,
    nullif(JSONTEXT['mkt_campaign']::string,'') AS mkt_campaign,
    nullif(JSONTEXT['mkt_clickid']::string,'') AS mkt_clickid,
    nullif(JSONTEXT['mkt_content']::string,'') AS mkt_content,
    nullif(JSONTEXT['mkt_medium']::string,'') AS mkt_medium,
    nullif(JSONTEXT['mkt_network']::string,'') AS mkt_network,
    nullif(JSONTEXT['mkt_source']::string,'') AS mkt_source,
    nullif(JSONTEXT['mkt_term']::string,'') AS mkt_term,
    nullif(JSONTEXT['name_tracker']::string,'') AS name_tracker,
    nullif(JSONTEXT['network_userid']::string,'') AS network_userid,
    nullif(JSONTEXT['os_family']::string,'') AS os_family,
    nullif(JSONTEXT['os_manufacturer']::string,'') AS os_manufacturer,
    nullif(JSONTEXT['os_name']::string,'') AS os_name,
    nullif(JSONTEXT['os_timezone']::string,'') AS os_timezone,
    nullif(JSONTEXT['page_referrer']::string,'') AS page_referrer,
    nullif(JSONTEXT['page_title']::string,'') AS page_title,
    nullif(JSONTEXT['page_url']::string,'') AS page_url,
    nullif(JSONTEXT['page_urlfragment']::string,'') AS page_urlfragment,
    nullif(JSONTEXT['page_urlhost']::string,'') AS page_urlhost,
    nullif(JSONTEXT['page_urlpath']::string,'') AS page_urlpath,
    nullif(JSONTEXT['page_urlport']::string,'') AS page_urlport,
    nullif(JSONTEXT['page_urlquery']::string,'') AS page_urlquery,
    nullif(JSONTEXT['page_urlscheme']::string,'') AS page_urlscheme,
    nullif(JSONTEXT['platform']::string,'') AS platform,
    try_to_numeric(JSONTEXT['pp_xoffset_max']::string) AS pp_xoffset_max,
    try_to_numeric(JSONTEXT['pp_xoffset_min']::string) AS pp_xoffset_min,
    try_to_numeric(JSONTEXT['pp_yoffset_max']::string) AS pp_yoffset_max,
    try_to_numeric(JSONTEXT['pp_yoffset_min']::string) AS pp_yoffset_min,
    nullif(JSONTEXT['refr_domain_userid']::string,'') AS refr_domain_userid,
    nullif(JSONTEXT['refr_dvce_tstamp']::string,'') AS refr_dvce_tstamp,
    nullif(JSONTEXT['refr_medium']::string,'') AS refr_medium,
    nullif(JSONTEXT['refr_source']::string,'') AS refr_source,
    nullif(JSONTEXT['refr_term']::string,'') AS refr_term,
    nullif(JSONTEXT['refr_urlfragment']::string,'') AS refr_urlfragment,
    nullif(JSONTEXT['refr_urlhost']::string,'') AS refr_urlhost,
    nullif(JSONTEXT['refr_urlpath']::string,'') AS refr_urlpath,
    nullif(JSONTEXT['refr_urlport']::string,'') AS refr_urlport,
    nullif(JSONTEXT['refr_urlquery']::string,'') AS refr_urlquery,
    nullif(JSONTEXT['refr_urlscheme']::string,'') AS refr_urlscheme,
    nullif(JSONTEXT['se_action']::string,'') AS se_action,
    nullif(JSONTEXT['se_category']::string,'') AS se_category,
    nullif(JSONTEXT['se_label']::string,'') AS se_label,
    nullif(JSONTEXT['se_property']::string,'') AS se_property,
    nullif(JSONTEXT['se_value']::string,'') AS se_value,
    nullif(JSONTEXT['ti_category']::string,'') AS ti_category,
    nullif(JSONTEXT['ti_currency']::string,'') AS ti_currency,
    nullif(JSONTEXT['ti_name']::string,'') AS ti_name,
    nullif(JSONTEXT['ti_orderid']::string,'') AS ti_orderid,
    nullif(JSONTEXT['ti_price']::string,'') AS ti_price,
    nullif(JSONTEXT['ti_price_base']::string,'') AS ti_price_base,
    nullif(JSONTEXT['ti_quantity']::string,'') AS ti_quantity,
    nullif(JSONTEXT['ti_sku']::string,'') AS ti_sku,
    nullif(JSONTEXT['tr_affiliation']::string,'') AS tr_affiliation,
    nullif(JSONTEXT['tr_city']::string,'') AS tr_city,
    nullif(JSONTEXT['tr_country']::string,'') AS tr_country,
    nullif(JSONTEXT['tr_currency']::string,'') AS tr_currency,
    nullif(JSONTEXT['tr_orderid']::string,'') AS tr_orderid,
    nullif(JSONTEXT['tr_shipping']::string,'') AS tr_shipping,
    nullif(JSONTEXT['tr_shipping_base']::string,'') AS tr_shipping_base,
    nullif(JSONTEXT['tr_state']::string,'') AS tr_state,
    nullif(JSONTEXT['tr_tax']::string,'') AS tr_tax,
    nullif(JSONTEXT['tr_tax_base']::string,'') AS tr_tax_base,
    nullif(JSONTEXT['tr_total']::string,'') AS tr_total,
    nullif(JSONTEXT['tr_total_base']::string,'') AS tr_total_base,
    nullif(JSONTEXT['true_tstamp']::string,'') AS true_tstamp,
    nullif(JSONTEXT['txn_id']::string,'') AS txn_id,
    nullif(JSONTEXT['unstruct_event']::string,'') AS unstruct_event,
    nullif(JSONTEXT['user_fingerprint']::string,'') AS user_fingerprint,
    nullif(JSONTEXT['user_id']::string, '') AS user_id,
    nullif(JSONTEXT['user_ipaddress']::string,'') AS user_ipaddress,
    nullif(JSONTEXT['useragent']::string,'') AS useragent,
    nullif(JSONTEXT['v_collector']::string,'') AS v_collector,
    nullif(JSONTEXT['v_etl']::string,'') AS v_etl,
    nullif(JSONTEXT['v_tracker']::string,'') AS v_tracker,
    uploaded_at
{% if target.name not in ("prod") -%}

FROM {{ source('snowplow', 'events_sample') }}

{%- else %}

FROM {{ source('snowplow', 'events') }}

{%- endif %}

WHERE JSONTEXT['app_id']::string IS NOT NULL
AND lower(JSONTEXT['page_url']::string) NOT LIKE 'https://staging.gitlab.com/%'
AND lower(JSONTEXT['page_url']::string) NOT LIKE 'http://localhost:%'

{% if is_incremental() %}
    AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
{% endif %}

{{- dbt_utils.group_by(n=133) -}}

), events_to_ignore as (

    SELECT event_id
    FROM base
    GROUP BY 1
    HAVING count (*) > 1
)

SELECT *
FROM base
WHERE event_id NOT IN (SELECT * FROM events_to_ignore)
