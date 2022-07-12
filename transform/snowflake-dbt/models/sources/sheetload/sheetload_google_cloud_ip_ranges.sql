WITH source AS (

  SELECT * 
  FROM {{ source('sheetload','google_cloud_ip_ranges') }}

),

renamed AS (
    SELECT
    meta.value['creationTime']::TIMESTAMP AS data_pulled_at,
    prefixes.value['ipv4Prefix']::VARCHAR AS ip_v4_prefix,
    PARSE_IP(ip_v4_prefix, 'INET')['ipv4']::NUMBER AS ipv4,
    SPLIT_PART(ip_v4_prefix, '/', 1) AS base_ip,
    SPLIT_PART(ip_v4_prefix, '/', 2) AS mask,
    TO_CHAR(ipv4, repeat('X', length(ipv4))) AS hex_ipv4,
    PARSE_IP(ip_v4_prefix, 'INET')['ipv4_range_end']::NUMBER AS ipv4_range_end,
    PARSE_IP(ip_v4_prefix, 'INET')['ipv4_range_start']::NUMBER AS ipv4_range_start,
    TO_CHAR(ipv4_range_start,repeat('X', length(ipv4_range_start))) AS hex_ipv4_range_start,
    TO_CHAR(ipv4_range_end,repeat('X', length(ipv4_range_end))) AS hex_ipv4_range_end,
    prefixes.value['ipv6Prefix']::VARCHAR AS ip_v6_prefix,
    parse_ip(ip_v6_prefix, 'INET')['hex_ipv6']::VARCHAR AS hex_ipv6,
    parse_ip(ip_v6_prefix, 'INET')['hex_ipv6_range_start']::VARCHAR AS hex_ipv6_range_start,
    parse_ip(ip_v6_prefix, 'INET')['hex_ipv6_range_start']::VARCHAR AS hex_ipv6_range_end,
    COALESCE(hex_ipv4_range_start,hex_ipv6_range_start) AS hex_ip_range_start,
    COALESCE(hex_ipv4_range_end,hex_ipv6_range_end)AS hex_ip_range_end,
    COALESCE(hex_ipv4,hex_ipv6) AS hex_ip,
    CASE
      WHEN ipv4 IS NOT NULL THEN 'ip4'
      WHEN hex_ipv6 IS NOT NULL THEN 'ipv6'
      ELSE 'unknown'
    END AS ip_type,
    prefixes.value['scope']::VARCHAR AS scope,
    prefixes.value['service']::VARCHAR AS service
  FROM source 
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(source.json_data)) AS meta
  INNER JOIN LATERAL FLATTEN(INPUT => meta.value['prefixes']) AS prefixes

    
)

SELECT * 
FROM source