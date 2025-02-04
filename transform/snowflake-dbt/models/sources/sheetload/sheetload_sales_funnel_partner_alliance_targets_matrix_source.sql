WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_partner_alliance_targets_matrix') }}

), renamed AS (

    SELECT
      kpi_name::VARCHAR                                                                         AS kpi_name,
      month::VARCHAR                                                                            AS month,
      {{ sales_qualified_source_cleaning('sales_qualified_source') }}::VARCHAR                  AS sales_qualified_source,
      IFF(source.sales_qualified_source::VARCHAR = 'Channel Generated', 'Partner Sourced', 'Co-sell')
                                                                                                AS sqs_bucket_engagement,
      alliance_partner::VARCHAR                                                                 AS alliance_partner,
      partner_category::VARCHAR                                                                 AS partner_category,
      order_type::VARCHAR                                                                       AS order_type,
      area::VARCHAR                                                                             AS area,
      user_segment::VARCHAR                                                                     AS user_segment,
      user_geo::VARCHAR 	                                                                      AS user_geo,
      user_region::VARCHAR 	                                                                    AS user_region,
      user_area::VARCHAR                                                                        AS user_area,
      user_business_unit::VARCHAR                                                               AS user_business_unit,
      user_role_type::VARCHAR                                                                   AS user_role_type,
      REPLACE(allocated_target, ',', '')::FLOAT                                                 AS allocated_target,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP                                        AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
