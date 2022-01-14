WITH source AS (

    SELECT
      id                                AS id,
      visitor_id                        AS visitor_id,
      cookie_id                         AS cookie_id,
      event_date                        AS event_date,
      modified_date                     AS modified_date,
      is_first_session                  AS is_first_session,
      channel                           AS channel,
      page_title                        AS page_title,
      landing_page                      AS landing_page,
      landing_page_raw                  AS landing_page_raw,
      referrer_page                     AS referrer_page,
      referrer_page_raw                 AS referrer_page_raw,
      referrer_name                     AS referrer_name,
      search_phrase                     AS search_phrase,
      web_source                        AS web_source,
      has_form                          AS has_form,
      has_chat                          AS has_chat,
      has_email                         AS has_email,
      has_crm_activity                  AS has_crm_activity,
      device                            AS device,
      ad_provider                       AS ad_provider,
      account_unique_id                 AS account_unique_id,
      account_name                      AS account_name,
      advertiser_unique_id              AS advertiser_unique_id,
      advertiser_name                   AS advertiser_name,
      site_unique_id                    AS site_unique_id,
      site_name                         AS site_name,
      placement_unique_id               AS placement_unique_id,
      placement_name                    AS placement_name,
      campaign_unique_id                AS campaign_unique_id,
      campaign_name                     AS campaign_name,
      ad_group_unique_id                AS ad_group_unique_id,
      ad_group_name                     AS ad_group_name,
      ad_unique_id                      AS ad_unique_id,
      ad_name                           AS ad_name,
      creative_unique_id                AS creative_unique_id,
      creative_name                     AS creative_name,
      creative_description_1            AS creative_description_1,
      creative_description_2            AS creative_description_2,
      creative_destination_url          AS creative_destination_url,
      creative_display_url              AS creative_display_url,
      keyword_unique_id                 AS keyword_unique_id,
      keyword_name                      AS keyword_name,
      keyword_match_type                AS keyword_match_type,
      campaign                          AS campaign,
      source                            AS source,
      medium                            AS medium,
      term                              AS term,
      content                           AS content,
      city                              AS city,
      region                            AS region,
      country                           AS country,
      isp_name                          AS isp_name,
      ip_address                        AS ip_address,
      is_deleted                        AS is_deleted,
      row_key                           AS row_key,
      landing_page_key                  AS landing_page_key,
      referrer_page_key                 AS referrer_page_key,
      account_row_key                   AS account_row_key,
      advertiser_row_key                AS advertiser_row_key,
      site_row_key                      AS site_row_key,
      placement_row_key                 AS placement_row_key,
      campaign_row_key                  AS campaign_row_key,
      ad_row_key                        AS ad_row_key,
      ad_group_row_key                  AS ad_group_row_key,
      creative_row_key                  AS creative_row_key,
      keyword_row_key                   AS keyword_row_key,
      _created_date                     AS _created_date,
      _modified_date                    AS _modified_date,
      _deleted_date                     AS _deleted_date
    FROM {{ source('bizible', 'biz_sessions') }}
    ORDER BY uploaded_at DESC

)